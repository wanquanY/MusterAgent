//! Path safety for job directories.
//!
//! Job paths come from the protocol (`BrowserInput::path`,
//! `BrowserDeclaredOutput::path`) and are re-validated here before any host
//! filesystem operation. Collection never follows symlinks: every component
//! of an output path must be a real directory and the leaf a regular file.

use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use crate::MAX_BROWSER_JOB_PATH_BYTES;
use musteragent_identity::BrowserJobId;
use thiserror::Error;

/// Container-side mount points.
pub const CONTAINER_INPUT_DIR: &str = "/sandbox/input";
pub const CONTAINER_WORK_DIR: &str = "/sandbox/work";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("job path is not a safe relative path")]
    Unsafe,
    #[error("job path traverses a symlink or non-directory")]
    Symlink,
    #[error("job output is not a regular file")]
    NotRegularFile,
    #[error("job output exceeds its byte limit")]
    TooLarge,
    #[error("job output changed while it was read")]
    Changed,
    #[error("job filesystem operation failed: {0}")]
    Io(String),
}

impl From<std::io::Error> for PathError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind().to_string())
    }
}

/// Validates a protocol job path and returns it as relative path components.
pub fn safe_relative_path(value: &str) -> Result<PathBuf, PathError> {
    if value.is_empty()
        || value.len() > MAX_BROWSER_JOB_PATH_BYTES
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.chars().any(char::is_control)
        || value
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err(PathError::Unsafe);
    }
    let path = PathBuf::from(value);
    if !path
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(PathError::Unsafe);
    }
    Ok(path)
}

/// Host directories for one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDirs {
    pub root: PathBuf,
    /// Mounted read-only at [`CONTAINER_INPUT_DIR`].
    pub input: PathBuf,
    /// Read-only seed copied into bounded tmpfs at [`CONTAINER_WORK_DIR`].
    /// The collector writes its validated snapshot back here after execution.
    pub work: PathBuf,
}

impl JobDirs {
    /// Creates fresh, empty directories. An existing directory for the same
    /// job (left by a crash) is removed first so stale files never leak in.
    pub fn create(work_root: &Path, job_id: BrowserJobId) -> Result<Self, PathError> {
        let root = work_root.join(job_id.into_uuid().to_string());
        if fs::symlink_metadata(&root).is_ok() {
            fs::remove_dir_all(&root)?;
        }
        fs::create_dir_all(work_root)?;
        fs::create_dir(&root)?;
        let input = root.join("input");
        let work = root.join("work");
        fs::create_dir(&input)?;
        fs::create_dir(&work)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755))?;
        fs::set_permissions(&input, fs::Permissions::from_mode(0o755))?;
        // The job runs as an unprivileged uid that differs from ours.
        fs::set_permissions(&work, fs::Permissions::from_mode(0o777))?;
        Ok(Self { root, input, work })
    }

    /// Writes one input file below the input directory (read-only for the
    /// job) and links it into the work directory so the job sees inputs and
    /// outputs in one cwd, like the browser runtime.
    pub fn place_input(&self, path: &str, bytes: &[u8]) -> Result<(), PathError> {
        let relative = safe_relative_path(path)?;
        let target = self.input.join(&relative);
        create_parents(&self.input, &relative, 0o755)?;
        if fs::symlink_metadata(&target).is_ok() {
            return Err(PathError::Unsafe);
        }
        fs::write(&target, bytes)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(0o444))?;
        create_parents(&self.work, &relative, 0o777)?;
        let link = self.work.join(&relative);
        let container_target = Path::new(CONTAINER_INPUT_DIR).join(&relative);
        std::os::unix::fs::symlink(container_target, link)?;
        Ok(())
    }

    /// Ensures the parent directories of a declared output exist so the job
    /// can write it.
    pub fn prepare_output(&self, path: &str) -> Result<(), PathError> {
        let relative = safe_relative_path(path)?;
        create_parents(&self.work, &relative, 0o777)
    }

    /// Reads one declared output without following symlinks. Returns `None`
    /// when the file does not exist.
    pub fn read_output(&self, path: &str, maximum: u64) -> Result<Option<Vec<u8>>, PathError> {
        let relative = safe_relative_path(path)?;
        let mut current = self.work.clone();
        let components: Vec<_> = relative.components().collect();
        for (index, component) in components.iter().enumerate() {
            current.push(component.as_os_str());
            let metadata = match fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let leaf = index + 1 == components.len();
            if metadata.file_type().is_symlink() {
                return Err(PathError::Symlink);
            }
            if !leaf && !metadata.is_dir() {
                return Err(PathError::Symlink);
            }
            if leaf && !metadata.is_file() {
                return Err(PathError::NotRegularFile);
            }
            if leaf && metadata.len() > maximum {
                return Err(PathError::TooLarge);
            }
        }
        let before = fs::symlink_metadata(&current)?;
        let file = fs::File::open(&current)?;
        let opened = file.metadata()?;
        if opened.ino() != before.ino() || opened.dev() != before.dev() || !opened.is_file() {
            return Err(PathError::Changed);
        }
        let mut bytes = Vec::new();
        file.take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > maximum {
            return Err(PathError::TooLarge);
        }
        Ok(Some(bytes))
    }

    /// Relative paths of every non-directory entry in the work directory
    /// that is neither a declared output nor an input link. Walks without
    /// following symlinks; bounded by `limit` entries.
    pub fn undeclared_entries(
        &self,
        declared: &[&str],
        inputs: &[&str],
        limit: usize,
    ) -> Result<Vec<String>, PathError> {
        let mut found = Vec::new();
        let mut stack = vec![(self.work.clone(), String::new())];
        let mut visited = 0_usize;
        while let Some((dir, prefix)) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                visited += 1;
                if visited > limit.saturating_mul(16).max(4_096) {
                    return Ok(found);
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                let relative = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{name}")
                };
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    stack.push((entry.path(), relative));
                    continue;
                }
                if declared.contains(&relative.as_str())
                    || (file_type.is_symlink() && inputs.contains(&relative.as_str()))
                {
                    continue;
                }
                found.push(relative);
                if found.len() >= limit {
                    return Ok(found);
                }
            }
        }
        found.sort();
        Ok(found)
    }

    /// Removes the job directory. Permissions are restored first because the
    /// job may have created read-only directories.
    pub fn remove(&self) -> Result<(), PathError> {
        make_writable(&self.root)?;
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn make_writable(path: &Path) -> Result<(), PathError> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_dir() {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
        for entry in fs::read_dir(path)? {
            make_writable(&entry?.path())?;
        }
    }
    Ok(())
}

fn create_parents(base: &Path, relative: &Path, mode: u32) -> Result<(), PathError> {
    let mut current = base.to_path_buf();
    let components: Vec<_> = relative.components().collect();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(PathError::Symlink),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                fs::set_permissions(&current, fs::Permissions::from_mode(mode))?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirs() -> (tempfile::TempDir, JobDirs) {
        let temp = tempfile::tempdir().expect("tempdir");
        let dirs = JobDirs::create(temp.path(), BrowserJobId::generate()).expect("dirs");
        (temp, dirs)
    }

    #[test]
    fn accepts_plain_relative_paths() {
        for path in [
            "main.py",
            "data/in.csv",
            "a/b/c.txt",
            "with space.txt",
            ".hidden",
        ] {
            assert!(safe_relative_path(path).is_ok(), "{path}");
        }
    }

    #[test]
    fn rejects_traversal_and_absolute_paths() {
        let long = "a".repeat(MAX_BROWSER_JOB_PATH_BYTES + 1);
        for path in [
            "",
            "/etc/passwd",
            "../x",
            "a/../../x",
            "a/./b",
            ".",
            "..",
            "a//b",
            "a/",
            "a\\b",
            "a\0b",
            "a\nb",
            long.as_str(),
        ] {
            assert_eq!(safe_relative_path(path), Err(PathError::Unsafe), "{path:?}");
        }
    }

    #[test]
    fn inputs_are_read_only_files_linked_into_work() {
        let (_temp, dirs) = dirs();
        dirs.place_input("data/in.csv", b"a,b\n").expect("input");
        let stored = dirs.input.join("data/in.csv");
        assert_eq!(fs::read(&stored).expect("read"), b"a,b\n");
        assert_eq!(
            fs::metadata(&stored).expect("meta").permissions().mode() & 0o777,
            0o444
        );
        let link = fs::read_link(dirs.work.join("data/in.csv")).expect("link");
        assert_eq!(link, Path::new("/sandbox/input/data/in.csv"));
        assert_eq!(
            dirs.place_input("data/in.csv", b"x"),
            Err(PathError::Unsafe)
        );
        assert_eq!(dirs.place_input("../escape", b"x"), Err(PathError::Unsafe));
    }

    #[test]
    fn reads_declared_regular_file_outputs() {
        let (_temp, dirs) = dirs();
        dirs.prepare_output("out/result.json").expect("prepare");
        fs::write(dirs.work.join("out/result.json"), b"{}").expect("write");
        assert_eq!(
            dirs.read_output("out/result.json", 10).expect("read"),
            Some(b"{}".to_vec())
        );
        assert_eq!(dirs.read_output("missing.txt", 10).expect("missing"), None);
    }

    #[test]
    fn output_byte_limit_is_enforced() {
        let (_temp, dirs) = dirs();
        fs::write(dirs.work.join("big.bin"), vec![0_u8; 11]).expect("write");
        assert_eq!(dirs.read_output("big.bin", 10), Err(PathError::TooLarge));
        assert!(dirs.read_output("big.bin", 11).expect("fits").is_some());
    }

    #[test]
    fn symlinked_outputs_and_parents_are_refused() {
        let (temp, dirs) = dirs();
        let secret = temp.path().join("secret.txt");
        fs::write(&secret, b"host secret").expect("secret");
        std::os::unix::fs::symlink(&secret, dirs.work.join("out.txt")).expect("link");
        assert_eq!(dirs.read_output("out.txt", 100), Err(PathError::Symlink));

        std::os::unix::fs::symlink(temp.path(), dirs.work.join("dir")).expect("dir link");
        assert_eq!(
            dirs.read_output("dir/secret.txt", 100),
            Err(PathError::Symlink)
        );
        assert_eq!(dirs.prepare_output("dir/new.txt"), Err(PathError::Symlink));
    }

    #[test]
    fn directories_and_fifos_are_not_outputs() {
        let (_temp, dirs) = dirs();
        fs::create_dir(dirs.work.join("out.txt")).expect("dir");
        assert_eq!(
            dirs.read_output("out.txt", 100),
            Err(PathError::NotRegularFile)
        );
    }

    #[test]
    fn undeclared_entries_exclude_outputs_and_input_links() {
        let (_temp, dirs) = dirs();
        dirs.place_input("in.csv", b"x").expect("input");
        fs::write(dirs.work.join("out.txt"), b"ok").expect("out");
        fs::create_dir(dirs.work.join("cache")).expect("cache");
        fs::write(dirs.work.join("cache/extra.bin"), b"x").expect("extra");
        fs::write(dirs.work.join("stray.txt"), b"x").expect("stray");
        let found = dirs
            .undeclared_entries(&["out.txt"], &["in.csv"], 100)
            .expect("walk");
        assert_eq!(
            found,
            vec!["cache/extra.bin".to_owned(), "stray.txt".to_owned()]
        );
    }

    #[test]
    fn recreating_a_job_dir_discards_stale_files_and_remove_cleans_up() {
        let temp = tempfile::tempdir().expect("tempdir");
        let job = BrowserJobId::generate();
        let first = JobDirs::create(temp.path(), job).expect("first");
        fs::write(first.work.join("stale.txt"), b"x").expect("stale");
        let second = JobDirs::create(temp.path(), job).expect("second");
        assert!(!second.work.join("stale.txt").exists());
        fs::create_dir(second.work.join("locked")).expect("locked");
        fs::set_permissions(
            second.work.join("locked"),
            fs::Permissions::from_mode(0o500),
        )
        .expect("chmod");
        second.remove().expect("remove");
        assert!(!second.root.exists());
    }
}
