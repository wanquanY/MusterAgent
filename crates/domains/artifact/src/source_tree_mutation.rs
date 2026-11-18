//! Atomic mutation contract for versioned Application source trees.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    ApplicationContractError, MAX_APPLICATION_SOURCE_FILES, SourceTreeFile, SourceTreeManifestV1,
};

pub const MAX_SOURCE_TREE_MUTATIONS: usize = MAX_APPLICATION_SOURCE_FILES;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SourceTreeMutation {
    Put(SourceTreeFile),
    Move { from: String, to: String },
    Delete { path: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceTreePatch {
    pub mutations: Vec<SourceTreeMutation>,
    pub entrypoint: Option<String>,
}

impl SourceTreePatch {
    /// Applies every mutation to an isolated copy and validates the complete
    /// successor. Callers persist only the returned manifest, so a rejected
    /// change set can never expose a partially updated project.
    pub fn apply(
        &self,
        base: &SourceTreeManifestV1,
    ) -> Result<SourceTreeManifestV1, SourceTreeMutationError> {
        base.validate()?;
        if self.mutations.is_empty() || self.mutations.len() > MAX_SOURCE_TREE_MUTATIONS {
            return Err(SourceTreeMutationError::InvalidMutationCount);
        }

        let mut files = base
            .files
            .iter()
            .cloned()
            .map(|file| (file.path.clone(), file))
            .collect::<BTreeMap<_, _>>();
        for mutation in &self.mutations {
            match mutation {
                SourceTreeMutation::Put(file) => {
                    reject_case_collision(&files, &file.path, Some(&file.path))?;
                    files.insert(file.path.clone(), file.clone());
                }
                SourceTreeMutation::Move { from, to } => {
                    if from == to {
                        return Err(SourceTreeMutationError::NoChanges);
                    }
                    let mut file = files
                        .remove(from)
                        .ok_or(SourceTreeMutationError::SourceMissing)?;
                    if files.contains_key(to) {
                        return Err(SourceTreeMutationError::TargetExists);
                    }
                    reject_case_collision(&files, to, None)?;
                    file.path.clone_from(to);
                    files.insert(to.clone(), file);
                }
                SourceTreeMutation::Delete { path } => {
                    if files.remove(path).is_none() {
                        return Err(SourceTreeMutationError::SourceMissing);
                    }
                }
            }
        }

        let successor = SourceTreeManifestV1 {
            files: files.into_values().collect(),
            entrypoint: self
                .entrypoint
                .clone()
                .unwrap_or_else(|| base.entrypoint.clone()),
            build_profile: base.build_profile,
            dependency_lock_ref: base.dependency_lock_ref.clone(),
        };
        successor.validate()?;
        if successor == *base {
            return Err(SourceTreeMutationError::NoChanges);
        }
        Ok(successor)
    }
}

fn reject_case_collision(
    files: &BTreeMap<String, SourceTreeFile>,
    candidate: &str,
    replacing: Option<&str>,
) -> Result<(), SourceTreeMutationError> {
    if files.keys().any(|existing| {
        replacing != Some(existing.as_str()) && existing.eq_ignore_ascii_case(candidate)
    }) {
        Err(SourceTreeMutationError::CaseCollision)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SourceTreeMutationError {
    #[error(transparent)]
    Contract(#[from] ApplicationContractError),
    #[error("source tree mutation count is outside the governed bound")]
    InvalidMutationCount,
    #[error("source tree mutation source does not exist")]
    SourceMissing,
    #[error("source tree mutation target already exists")]
    TargetExists,
    #[error("source tree mutation creates a case-insensitive path collision")]
    CaseCollision,
    #[error("source tree mutation does not change the version")]
    NoChanges,
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;

    use crate::{ApplicationBuildProfile, SourceFileKind};

    use super::*;

    fn file(path: &str, media_type: &str, source: &str) -> SourceTreeFile {
        SourceTreeFile {
            path: path.to_owned(),
            kind: SourceFileKind::Text,
            content_ref: ContentRef {
                content_id: ContentId::generate(),
                media_type: MediaType::new(media_type).expect("media type"),
                byte_length: source.len() as u64,
                sha256: Sha256::digest(source),
            },
            resource: None,
        }
    }

    fn base() -> SourceTreeManifestV1 {
        SourceTreeManifestV1 {
            files: vec![
                file("assets/app.css", "text/css", "body{}"),
                file("index.html", "text/html", "<h1>old</h1>"),
            ],
            entrypoint: "index.html".to_owned(),
            build_profile: ApplicationBuildProfile::Static,
            dependency_lock_ref: None,
        }
    }

    #[test]
    fn applies_put_move_and_delete_as_one_validated_successor() {
        let successor = SourceTreePatch {
            mutations: vec![
                SourceTreeMutation::Put(file("index.html", "text/html", "<h1>new</h1>")),
                SourceTreeMutation::Move {
                    from: "assets/app.css".to_owned(),
                    to: "styles/app.css".to_owned(),
                },
                SourceTreeMutation::Put(file(
                    "scripts/app.js",
                    "application/javascript",
                    "export {}",
                )),
            ],
            entrypoint: None,
        }
        .apply(&base())
        .expect("atomic patch");

        assert_eq!(
            successor
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["index.html", "scripts/app.js", "styles/app.css"]
        );
        assert_eq!(successor.entrypoint, "index.html");
    }

    #[test]
    fn rejected_patch_never_mutates_the_base_manifest() {
        let original = base();
        let unchanged = original.clone();
        let result = SourceTreePatch {
            mutations: vec![SourceTreeMutation::Delete {
                path: "index.html".to_owned(),
            }],
            entrypoint: None,
        }
        .apply(&original);

        assert_eq!(
            result,
            Err(SourceTreeMutationError::Contract(
                ApplicationContractError::EntrypointMissing
            ))
        );
        assert_eq!(original, unchanged);
    }

    #[test]
    fn rejects_media_kind_and_extension_confusion() {
        let mut wrong_kind = base();
        wrong_kind.files[0].kind = SourceFileKind::Binary;
        assert_eq!(
            wrong_kind.validate(),
            Err(ApplicationContractError::FileRepresentationMismatch)
        );

        let mut wrong_extension = base();
        wrong_extension.files[0].path = "assets/app.js".to_owned();
        assert_eq!(
            wrong_extension.validate(),
            Err(ApplicationContractError::FileExtensionMismatch)
        );
    }
}
