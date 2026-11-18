//! Format parsing has a physical lifetime independent of Tokio's blocking pool.
//! Only a host-configured executable is invoked; file contents never select it.
use crate::{
    ExtractionQuality,
    search_indexing::{MAX_EXTRACTED_TEXT_BYTES, SearchExtractionError as Error},
};
use std::{
    io::{Read, Write},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
};

const FLAG: &str = "--musteragent-document-parser";
const MAX_SOURCE: usize = 32 * 1024 * 1024;
const MAX_QUALITY: usize = 4 * 1024 * 1024;
const MAX_OUTPUT: usize = 4 + MAX_QUALITY + MAX_EXTRACTED_TEXT_BYTES;
// Per host process; child OS limits additionally constrain individual work.
static SLOTS: Semaphore = Semaphore::const_new(2);
pub(crate) struct Material {
    pub text: String,
    pub quality: ExtractionQuality,
}

pub(crate) async fn extract(media: &str, bytes: Vec<u8>) -> Result<Material, Error> {
    crate::search_indexing::parser_identity(media)?;
    if bytes.len() > MAX_SOURCE || media.len() > 512 {
        return Err(Error::OutputTooLarge);
    }
    let _permit = SLOTS
        .acquire()
        .await
        .map_err(|_| Error::ParserUnavailable)?;
    let executable = std::env::var_os("MUSTERAGENT_DOCUMENT_PARSER_EXECUTABLE")
        .map(std::path::PathBuf::from)
        .map(Ok)
        .unwrap_or_else(std::env::current_exe)
        .map_err(|_| Error::ParserUnavailable)?;
    run(&executable, media, bytes, Duration::from_secs(120)).await
}

async fn run(
    executable: &std::path::Path,
    media: &str,
    bytes: Vec<u8>,
    timeout: Duration,
) -> Result<Material, Error> {
    let mut child = tokio::process::Command::new(executable)
        .arg(FLAG)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Error::ParserUnavailable)?;
    #[cfg(target_os = "macos")]
    let pid = child.id().ok_or(Error::ParserFailed)?;
    let mut stdin = child.stdin.take().ok_or(Error::ParserFailed)?;
    let stdout = child.stdout.take().ok_or(Error::ParserFailed)?;
    let work = async {
        tokio::try_join!(
            async move {
                stdin.write_u16(media.len() as u16).await?;
                stdin.write_all(media.as_bytes()).await?;
                stdin.write_all(&bytes).await?;
                stdin.shutdown().await?;
                drop(stdin); // EOF is part of the framing; flushing alone cannot finish the source.
                Ok::<_, std::io::Error>(())
            },
            async {
                let mut output = Vec::new();
                stdout
                    .take((MAX_OUTPUT + 1) as u64)
                    .read_to_end(&mut output)
                    .await?;
                Ok::<_, std::io::Error>(output)
            },
            child.wait()
        )
    };
    #[cfg(target_os = "macos")]
    let work = async {
        tokio::select! {
            value = work => value,
            _ = observe_memory(pid, 512 * 1024 * 1024) => {
                Err(std::io::Error::other("document worker exceeded memory budget"))
            }
        }
    };
    let outcome = tokio::time::timeout(timeout, work).await;
    let (_, output, status) = match outcome {
        Ok(Ok(value)) => value,
        _ => {
            let _ = child.kill().await;
            return Err(Error::ParserFailed);
        }
    };
    if !status.success() {
        return Err(match status.code() {
            Some(20) => Error::ParserUnavailable,
            Some(21) => Error::OcrRequired,
            Some(22) => Error::OutputTooLarge,
            Some(23) => Error::InvalidTextEncoding,
            _ => Error::ParserFailed,
        });
    }
    decode(output)
}

#[cfg(target_os = "macos")]
async fn observe_memory(pid: u32, budget: u64) {
    use libproc::pid_rusage::{RUsageInfoV2, pidrusage};
    let mut failures = 0;
    loop {
        // This is a sampling bound, not a hard peak-memory guarantee. The
        // child is still owned and reaped by run(), including monitor failures.
        match pidrusage::<RUsageInfoV2>(pid as i32) {
            Ok(info) if info.ri_resident_size.max(info.ri_phys_footprint) <= budget => {
                failures = 0;
            }
            Ok(_) => return,
            Err(_) => {
                // An exiting process can disappear before wait() observes it.
                failures += 1;
                if failures >= 3 {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn decode(output: Vec<u8>) -> Result<Material, Error> {
    if output.len() < 4 || output.len() > MAX_OUTPUT {
        return Err(Error::ParserFailed);
    }
    let size =
        u32::from_be_bytes(output[..4].try_into().map_err(|_| Error::ParserFailed)?) as usize;
    if size > MAX_QUALITY
        || size + 4 > output.len()
        || output.len() - size - 4 > MAX_EXTRACTED_TEXT_BYTES
    {
        return Err(Error::OutputTooLarge);
    }
    let quality = serde_json::from_slice(&output[4..size + 4]).map_err(|_| Error::ParserFailed)?;
    let text =
        String::from_utf8(output[size + 4..].to_vec()).map_err(|_| Error::InvalidTextEncoding)?;
    Ok(Material { text, quality })
}

/// Called by each host before creating threads or initializing credentials.
/// Returns None for normal startup. The host exits immediately for this mode.
pub fn document_parser_process_entrypoint() -> Option<i32> {
    if std::env::args().nth(1).as_deref() != Some(FLAG) {
        return None;
    }
    Some(match run_child() {
        Ok(()) => 0,
        Err(Error::ParserUnavailable) => 20,
        Err(Error::OcrRequired) => 21,
        Err(Error::OutputTooLarge) => 22,
        Err(Error::InvalidTextEncoding) => 23,
        Err(Error::ParserFailed) => 24,
    })
}
fn run_child() -> Result<(), Error> {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, setrlimit};
        setrlimit(Resource::RLIMIT_CPU, 90, 90).map_err(|_| Error::ParserUnavailable)?;
        // Darwin does not enforce these address-space limits; its parent
        // observes the child through proc_pid_rusage instead.
        #[cfg(target_os = "linux")]
        setrlimit(Resource::RLIMIT_AS, 512 * 1024 * 1024, 512 * 1024 * 1024)
            .map_err(|_| Error::ParserUnavailable)?;
        setrlimit(Resource::RLIMIT_NOFILE, 64, 64).map_err(|_| Error::ParserUnavailable)?;
        setrlimit(Resource::RLIMIT_CORE, 0, 0).map_err(|_| Error::ParserUnavailable)?;
    }
    let mut input = std::io::stdin().lock();
    let mut length = [0; 2];
    input
        .read_exact(&mut length)
        .map_err(|_| Error::ParserFailed)?;
    let length = u16::from_be_bytes(length) as usize;
    if length > 512 {
        return Err(Error::ParserUnavailable);
    }
    let mut media = vec![0; length];
    input
        .read_exact(&mut media)
        .map_err(|_| Error::ParserFailed)?;
    let media = String::from_utf8(media).map_err(|_| Error::ParserUnavailable)?;
    let mut bytes = Vec::new();
    input
        .take((MAX_SOURCE + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::ParserFailed)?;
    if bytes.len() > MAX_SOURCE {
        return Err(Error::OutputTooLarge);
    }
    let material = crate::search_indexing::extract_document_sync(&media, bytes)?;
    let quality = serde_json::to_vec(&material.quality).map_err(|_| Error::ParserFailed)?;
    if quality.len() > MAX_QUALITY || material.text.len() > MAX_EXTRACTED_TEXT_BYTES {
        return Err(Error::OutputTooLarge);
    }
    let mut output = std::io::stdout().lock();
    output
        .write_all(&(quality.len() as u32).to_be_bytes())
        .map_err(|_| Error::ParserFailed)?;
    output
        .write_all(&quality)
        .map_err(|_| Error::ParserFailed)?;
    output
        .write_all(material.text.as_bytes())
        .map_err(|_| Error::ParserFailed)?;
    output.flush().map_err(|_| Error::ParserFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    async fn process_exists(pid: &str) -> bool {
        tokio::process::Command::new("/bin/kill")
            .args(["-0", pid])
            .stderr(Stdio::null())
            .status()
            .await
            .unwrap()
            .success()
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_and_timeout_reap_the_physical_worker() {
        use std::os::unix::fs::PermissionsExt;
        for cancelled in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let executable = directory.path().join("parser");
            let marker = directory.path().join("pid");
            std::fs::write(
                &executable,
                format!(
                    "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nexec /bin/sleep 30\n",
                    marker.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
            let task = tokio::spawn(async move {
                run(
                    &executable,
                    "application/pdf",
                    vec![],
                    Duration::from_secs(5),
                )
                .await
                .map(|_| ())
            });
            let pid = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(value) = std::fs::read_to_string(&marker)
                        && !value.is_empty()
                    {
                        break value;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(process_exists(&pid).await);
            if cancelled {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(task.await.unwrap().is_err());
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while process_exists(&pid).await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("parser PID must not outlive cancellation or timeout");
        }
    }
    #[test]
    fn rejects_inconsistent_worker_frames() {
        for bytes in [vec![], vec![0; 3], vec![0, 0, 0, 8, 1], vec![255; 5]] {
            assert!(decode(bytes).is_err());
        }
    }
}
