//! File delivery is independent of the product's preview/editor registry.
//!
//! A producer can deliver opaque bytes even when no editor understands them.
//! Frozen Python contracts retain their original tabular admission policy.

use musteragent_content_ref::MediaType;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum GeneratedFileContract {
    PythonTabular,
    PythonFile,
}

impl GeneratedFileContract {
    /// Generated files are collected and stored as one in-memory object
    /// (browser Runner outputs become `PutContent`), so they share the buffered-object
    /// bound
    /// (`MAX_BUFFERED_CONTENT_BYTES` in `runtime/content`). Longer media made
    /// on a Desktop reaches the conversation through streamed publication,
    /// which accepts up to `MAX_CONTENT_BYTES` (78.125 GiB).
    pub const MAXIMUM_BYTES: u64 = 264 * 1024 * 1024 + 12;

    #[must_use]
    pub fn for_tool(key: &str) -> Option<Self> {
        match key {
            "data.python.run@3" | "data.python.run@4" | "data.python.run@5"
            | "data.python.run@6" | "data.python.run@7" => Some(Self::PythonTabular),
            "data.python.run@8" | "data.python.run@9" | "data.python.run@10"
            | "data.python.run@12" => Some(Self::PythonFile),
            _ => None,
        }
    }

    #[must_use]
    pub fn accepts(self, path: &str, media_type: &MediaType) -> bool {
        match self {
            Self::PythonTabular => {
                let profile = super::file_capability(media_type);
                profile.supports_generator("data.python.run") && profile.accepts_path(path)
            }
            Self::PythonFile => valid_output_media_type(media_type.as_str()),
        }
    }

    pub fn accepts_length(self, bytes: u64) -> bool {
        bytes <= Self::MAXIMUM_BYTES && (bytes > 0 || self == Self::PythonFile)
    }
}

// Same bounded, lowercase media-type grammar as the cross-origin Runner.
// MIME describes bytes; it does not confer permission to execute or preview.
fn valid_output_media_type(value: &str) -> bool {
    fn token(value: &str) -> bool {
        !value.is_empty()
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-".contains(&b))
    }
    let mut parts = value.split(';');
    let Some((kind, subtype)) = parts.next().and_then(|part| part.split_once('/')) else {
        return false;
    };
    token(kind)
        && token(subtype)
        && parts.all(|part| {
            part.split_once('=').is_some_and(|(key, value)| {
                token(key) && !value.is_empty() && value.bytes().all(|b| b.is_ascii_graphic())
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_files_do_not_require_a_preview_or_editor() {
        for (path, mime) in [
            ("chart.png", "image/png"),
            ("chart.svg", "image/svg+xml"),
            ("report.pdf", "application/pdf"),
            ("metrics.json", "application/json"),
            ("archive.zip", "application/zip"),
            ("data.parquet", "application/vnd.apache.parquet"),
            ("custom.bin", "application/octet-stream"),
            ("notes.txt", "text/plain;charset=utf-8"),
        ] {
            let media = MediaType::new(mime).unwrap();
            assert!(
                GeneratedFileContract::PythonFile.accepts(path, &media),
                "{mime}"
            );
            assert!(!GeneratedFileContract::PythonTabular.accepts(path, &media));
        }
        assert!(
            GeneratedFileContract::PythonTabular
                .accepts("data.csv", &MediaType::new("text/csv").unwrap())
        );
        assert!(
            !GeneratedFileContract::PythonTabular
                .accepts("data.png", &MediaType::new("text/csv").unwrap())
        );
    }

    #[test]
    fn malformed_mime_and_unknown_producers_are_rejected() {
        for mime in [
            "image/",
            "/png",
            "image/png/extra",
            "IMAGE/PNG",
            "text/plain;bad",
        ] {
            assert!(
                !GeneratedFileContract::PythonFile
                    .accepts("output", &MediaType::new(mime).unwrap())
            );
        }
        assert_eq!(
            GeneratedFileContract::for_tool("data.python.run@7"),
            Some(GeneratedFileContract::PythonTabular)
        );
        assert_eq!(
            GeneratedFileContract::for_tool("data.python.run@8"),
            Some(GeneratedFileContract::PythonFile)
        );
        assert_eq!(
            GeneratedFileContract::for_tool("untrusted.generator@1"),
            None
        );
        for key in [
            "audio.narration.create@1",
            "audio.music.create@1",
            "audio.effect.create@1",
            "video.template.use@1",
            "video.scene.preview@1",
            "video.scene.render@1",
            "video.narrate@1",
        ] {
            assert_eq!(GeneratedFileContract::for_tool(key), None, "{key}");
        }
        assert!(GeneratedFileContract::PythonFile.accepts_length(0));
        assert!(!GeneratedFileContract::PythonTabular.accepts_length(0));
        assert!(
            GeneratedFileContract::PythonFile.accepts_length(GeneratedFileContract::MAXIMUM_BYTES)
        );
        assert!(
            !GeneratedFileContract::PythonFile
                .accepts_length(GeneratedFileContract::MAXIMUM_BYTES + 1)
        );
    }
}
