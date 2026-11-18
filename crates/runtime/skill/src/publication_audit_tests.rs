use super::*;
use std::io::{Cursor, Write};
use zip::{ZipWriter, write::SimpleFileOptions};

fn archive(header: &str, extra: Option<(&str, &[u8])>) -> Vec<u8> {
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    zip.start_file("SKILL.md", options)
        .expect("valid package fixture");
    write!(zip, "---\n{header}\n---\n# Complete work\nAdapt the original full workflow to the user's story and assets.\n").expect("valid package fixture");
    if let Some((name, bytes)) = extra {
        zip.start_file(name, options)
            .expect("valid package fixture");
        zip.write_all(bytes).expect("valid package fixture");
    }
    zip.finish().expect("valid package fixture").into_inner()
}

fn inspect(bytes: &[u8]) -> Result<SkillArchiveInspection, SkillArchiveAuditError> {
    let request = ImportSkillArchive {
        tenant_id: TenantId::generate(),
        principal_id: PrincipalId::generate(),
        archive_path: "this-path-is-never-opened.zip".into(),
        source: SkillPackageSource::Local,
        source_ref: "publication-audit".into(),
        publisher_ref: "".into(),
        suggested_name: None,
        display_name: None,
        expected_archive_hash: Some(musteragent_content_ref::Sha256::digest(bytes).to_string()),
        occurred_at: UtcTimestamp::from_unix_millis(1),
    };
    inspect_skill_archive_bytes(&request, bytes)
}

#[test]
fn publication_and_installation_reject_the_original_unquoted_colon_description() {
    let bytes = archive(
        "name: sample-data-story\ndescription: Complete narrated data story: source values",
        None,
    );
    assert!(matches!(
        inspect(&bytes),
        Err(SkillArchiveAuditError::InvalidFrontmatter)
    ));
}

#[test]
fn quoted_yaml_preserves_author_text_and_reports_the_original_archive() {
    let bytes = archive(
        "name: sample-data-story\ndescription: \"Complete narrated data story: source values\"\nsupported-surfaces: desktop",
        Some(("assets/font.woff2", b"original font resource")),
    );
    let info = inspect(&bytes).expect("valid package fixture");
    assert_eq!(info.skill_key, "sample-data-story");
    assert_eq!(
        info.description,
        "Complete narrated data story: source values"
    );
    assert_eq!(info.file_count, 2);
    assert_eq!(
        info.archive_sha256,
        musteragent_content_ref::Sha256::digest(&bytes)
    );
    assert_eq!(
        info.declared_surfaces,
        Some(BTreeSet::from([Surface::Desktop]))
    );
    assert!(info.expanded_byte_length > 100);
}

#[test]
fn publication_uses_the_same_file_size_and_native_payload_checks_as_installation() {
    let header = "name: publication-film\ndescription: A complete film workflow";
    let oversized = vec![0_u8; 17 * 1024 * 1024];
    assert!(matches!(
        inspect(&archive(header, Some(("assets/font.ttf", &oversized)))),
        Err(SkillArchiveAuditError::FileSize)
    ));
    assert!(matches!(
        inspect(&archive(
            header,
            Some(("assets/disguised.png", b"MZnative"))
        )),
        Err(SkillArchiveAuditError::NativePayload)
    ));
}
