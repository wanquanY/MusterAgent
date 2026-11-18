//! Closed media capability registry for logical Runtime files.

use musteragent_content_ref::MediaType;

pub const FILE_CAPABILITY_REGISTRY_VERSION: &str = "musteragent.file-capabilities/2";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileRepresentation {
    Text,
    Binary,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileAuthoring {
    DirectText,
    ReplaceOnly,
    GeneratedBy(&'static str),
    DirectTextOrGeneratedBy(&'static str),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FilePreview {
    None,
    Text,
    Image,
    Spreadsheet,
    Application,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FileCapabilityProfile {
    pub representation: FileRepresentation,
    pub authoring: FileAuthoring,
    pub preview: FilePreview,
    pub maximum_bytes: u64,
    pub extensions: &'static [&'static str],
}

impl FileCapabilityProfile {
    #[must_use]
    pub fn accepts_path(self, path: &str) -> bool {
        if self.extensions.is_empty() {
            return true;
        }
        let extension = path
            .rsplit('/')
            .next()
            .and_then(|name| name.rsplit_once('.'))
            .map(|(_, extension)| extension.to_ascii_lowercase());
        extension.is_some_and(|extension| {
            self.extensions
                .iter()
                .any(|candidate| *candidate == extension)
        })
    }

    #[must_use]
    pub const fn supports_direct_text(self) -> bool {
        matches!(
            self.authoring,
            FileAuthoring::DirectText | FileAuthoring::DirectTextOrGeneratedBy(_)
        )
    }

    #[must_use]
    pub fn supports_generator(self, capability: &str) -> bool {
        matches!(
            self.authoring,
            FileAuthoring::GeneratedBy(expected)
                | FileAuthoring::DirectTextOrGeneratedBy(expected)
                if expected == capability
        )
    }
}

/// Returns the immutable V1 product decision for one media type.
///
/// Capability lookup is based on the case-insensitive MIME essence. Textual
/// types may additionally declare exactly one `charset=utf-8` parameter,
/// because Runtime content references historically persist that canonical
/// spelling. Other parameters and non-UTF-8 character sets stay closed: they
/// remain transferable binary content and are never promoted to editable text
/// merely because their MIME essence or path looks familiar.
#[must_use]
pub fn file_capability(media_type: &MediaType) -> FileCapabilityProfile {
    let Some(essence) = supported_media_essence(media_type.as_str()) else {
        return unknown();
    };
    match essence.as_str() {
        "text/plain" => text(&["txt"], FilePreview::Text, 1),
        "text/markdown" => text(&["md", "markdown"], FilePreview::Text, 2),
        "text/x-python" => text(&["py"], FilePreview::Text, 2),
        "text/html" => text(&["html", "htm"], FilePreview::Application, 2),
        "text/css" => text(&["css"], FilePreview::Text, 2),
        "text/javascript" | "application/javascript" => {
            text(&["js", "jsx", "mjs", "cjs"], FilePreview::Text, 2)
        }
        "text/typescript" | "application/typescript" => text(&["ts", "tsx"], FilePreview::Text, 2),
        "application/json" => text(&["json"], FilePreview::Text, 5),
        "application/yaml" | "text/yaml" => text(&["yaml", "yml"], FilePreview::Text, 2),
        "application/toml" | "text/toml" => text(&["toml"], FilePreview::Text, 2),
        "application/xml" | "text/xml" => text(&["xml"], FilePreview::Text, 5),
        "text/vtt" => text(&["vtt"], FilePreview::Text, 2),
        "text/x-java-properties" => text(&["properties"], FilePreview::Text, 2),
        "text/mdx" => text(&["mdx"], FilePreview::Text, 2),
        "text/csv" => FileCapabilityProfile {
            representation: FileRepresentation::Text,
            authoring: FileAuthoring::DirectTextOrGeneratedBy("data.python.run"),
            preview: FilePreview::Spreadsheet,
            maximum_bytes: mib(50),
            extensions: &["csv"],
        },
        "text/tab-separated-values" => FileCapabilityProfile {
            representation: FileRepresentation::Text,
            authoring: FileAuthoring::DirectTextOrGeneratedBy("data.python.run"),
            preview: FilePreview::Spreadsheet,
            maximum_bytes: mib(50),
            extensions: &["tsv"],
        },
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
            FileCapabilityProfile {
                representation: FileRepresentation::Binary,
                authoring: FileAuthoring::GeneratedBy("data.python.run"),
                preview: FilePreview::Spreadsheet,
                maximum_bytes: mib(50),
                extensions: &["xlsx"],
            }
        }
        "application/pdf" => binary(&["pdf"], FilePreview::None, 50),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            binary(&["docx"], FilePreview::None, 50)
        }
        "application/vnd.ms-powerpoint" => binary(&["ppt"], FilePreview::None, 50),
        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => {
            binary(&["pptx"], FilePreview::None, 50)
        }
        "image/png" => binary(&["png"], FilePreview::Image, 20),
        "image/jpeg" => binary(&["jpg", "jpeg"], FilePreview::Image, 20),
        "image/gif" => binary(&["gif"], FilePreview::Image, 20),
        "image/webp" => binary(&["webp"], FilePreview::Image, 20),
        "image/svg+xml" => text(&["svg"], FilePreview::Image, 5),
        "application/zip" => binary(&["zip"], FilePreview::None, 100),
        _ => unknown(),
    }
}

fn supported_media_essence(value: &str) -> Option<String> {
    let mut segments = value.split(';');
    let essence = segments.next()?.to_ascii_lowercase();
    let parameter = segments.next();
    if segments.next().is_some() {
        return None;
    }
    match parameter {
        None => Some(essence),
        Some(parameter)
            if essence.starts_with("text/")
                || matches!(
                    essence.as_str(),
                    "application/javascript" | "application/typescript" | "application/json"
                ) =>
        {
            parameter
                .eq_ignore_ascii_case("charset=utf-8")
                .then_some(essence)
        }
        Some(_) => None,
    }
}

const fn unknown() -> FileCapabilityProfile {
    FileCapabilityProfile {
        representation: FileRepresentation::Binary,
        authoring: FileAuthoring::ReplaceOnly,
        preview: FilePreview::None,
        maximum_bytes: mib(10),
        extensions: &[],
    }
}

const fn text(
    extensions: &'static [&'static str],
    preview: FilePreview,
    maximum_mib: u64,
) -> FileCapabilityProfile {
    FileCapabilityProfile {
        representation: FileRepresentation::Text,
        authoring: FileAuthoring::DirectText,
        preview,
        maximum_bytes: mib(maximum_mib),
        extensions,
    }
}

const fn binary(
    extensions: &'static [&'static str],
    preview: FilePreview,
    maximum_mib: u64,
) -> FileCapabilityProfile {
    FileCapabilityProfile {
        representation: FileRepresentation::Binary,
        authoring: FileAuthoring::ReplaceOnly,
        preview,
        maximum_bytes: mib(maximum_mib),
        extensions,
    }
}

const fn mib(value: u64) -> u64 {
    value * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::MediaType;

    use super::*;

    #[test]
    fn registry_never_promotes_unknown_content_to_editable_text() {
        let profile = file_capability(&MediaType::new("application/x-private").expect("media"));
        assert_eq!(profile.representation, FileRepresentation::Binary);
        assert_eq!(profile.authoring, FileAuthoring::ReplaceOnly);
        assert_eq!(profile.preview, FilePreview::None);
    }

    #[test]
    fn configuration_text_has_explicit_owner_capabilities() {
        for (mime, path) in [
            ("application/yaml", "app.yaml"),
            ("text/yaml", "app.yml"),
            ("application/toml", "app.toml"),
            ("text/toml", "app.toml"),
        ] {
            let profile = file_capability(&MediaType::new(mime).unwrap());
            assert!(profile.supports_direct_text());
            assert!(profile.accepts_path(path));
            assert_eq!(profile.maximum_bytes, 2 * 1024 * 1024);
            let legacy =
                file_capability(&MediaType::new(format!("{mime};charset=iso-8859-1")).unwrap());
            assert!(!legacy.supports_direct_text());
        }
        assert!(
            !file_capability(&MediaType::new("application/x-yaml-unregistered").unwrap())
                .supports_direct_text()
        );
    }

    #[test]
    fn registry_selects_semantic_generators_and_extensions() {
        let xlsx = file_capability(
            &MediaType::new("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                .expect("media"),
        );
        assert!(xlsx.supports_generator("data.python.run"));
        assert!(!xlsx.supports_direct_text());
        assert!(xlsx.accepts_path("reports/final.XLSX"));
        assert!(!xlsx.accepts_path("reports/final.zip"));

        let csv = file_capability(&MediaType::new("text/csv").expect("CSV media"));
        assert!(csv.supports_direct_text());
        assert!(csv.supports_generator("data.python.run"));

        let javascript =
            file_capability(&MediaType::new("text/javascript").expect("JavaScript media"));
        assert!(javascript.accepts_path("src/component.jsx"));

        let pdf = file_capability(&MediaType::new("application/pdf").expect("PDF media"));
        assert_eq!(pdf.representation, FileRepresentation::Binary);
        assert_eq!(pdf.authoring, FileAuthoring::ReplaceOnly);
        assert_eq!(pdf.maximum_bytes, 50 * 1024 * 1024);
        assert!(pdf.accepts_path("research/source.PDF"));

        let docx = file_capability(
            &MediaType::new(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
            .expect("DOCX media"),
        );
        assert_eq!(docx.maximum_bytes, 50 * 1024 * 1024);
        assert!(docx.accepts_path("documents/spec.docx"));
    }

    #[test]
    fn registry_accepts_only_the_supported_utf8_text_parameter() {
        let plain = file_capability(&MediaType::new("text/markdown").expect("media"));
        let utf8 = file_capability(
            &MediaType::new("TEXT/MARKDOWN;charset=UTF-8").expect("parameterized media"),
        );
        assert_eq!(utf8, plain);

        let legacy_encoding = file_capability(
            &MediaType::new("text/markdown;charset=iso-8859-1").expect("legacy media"),
        );
        assert_eq!(legacy_encoding.representation, FileRepresentation::Binary);
        assert_eq!(legacy_encoding.authoring, FileAuthoring::ReplaceOnly);

        let extra_parameter = file_capability(
            &MediaType::new("text/markdown;charset=utf-8;profile=commonmark")
                .expect("parameterized media"),
        );
        assert_eq!(extra_parameter.authoring, FileAuthoring::ReplaceOnly);
    }
}
