use super::*;
use lopdf::{Stream, dictionary};

fn pdf(mapping: &str, code: Vec<u8>, empty_page: bool) -> Vec<u8> {
    let mut doc = Document::with_version("1.7");
    let pages = doc.new_object_id();
    let cmap = doc.add_object(Stream::new(dictionary! {}, format!(
        "/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n/CIDSystemInfo << /Registry (Test) /Ordering (Test) /Supplement 0 >> def\n/CMapName /Test def\n/CMapType 2 def\n1 begincodespacerange\n<00> <FF>\nendcodespacerange\n{mapping}\nendcmap\nCMapName currentdict /CMap defineresource pop\nend\nend"
    ).into_bytes()));
    let font = doc.add_object(dictionary! {
        "Type" => "Font", "Subtype" => "Type3",
        "Encoding" => dictionary! {"Type"=>"Encoding", "Differences" => vec![Object::Integer(1), Object::Name(b"A".to_vec()), Object::Name(b"B".to_vec())]},
        "ToUnicode" => cmap,
    });
    let content = lopdf::content::Content {
        operations: vec![
            lopdf::content::Operation::new("BT", vec![]),
            lopdf::content::Operation::new(
                "Tf",
                vec![Object::Name(b"F1".to_vec()), Object::Integer(12)],
            ),
            lopdf::content::Operation::new("Tj", vec![Object::string_literal(code)]),
            lopdf::content::Operation::new("ET", vec![]),
        ],
    }
    .encode()
    .unwrap();
    let content = doc.add_object(Stream::new(dictionary! {}, content));
    let page = doc.add_object(dictionary! {"Type"=>"Page", "Parent"=>pages, "Contents"=>content,
        "Resources"=>dictionary! {"Font"=>dictionary! {"F1"=>font}}, "MediaBox"=>vec![0.into(),0.into(),612.into(),792.into()]});
    let mut kids = vec![Object::Reference(page)];
    if empty_page {
        let empty = doc.add_object(Stream::new(dictionary! {}, vec![]));
        kids.push(Object::Reference(doc.add_object(
            dictionary! {"Type"=>"Page", "Parent"=>pages,"Contents"=>empty},
        )));
    }
    doc.objects.insert(
        pages,
        dictionary! {"Type"=>"Pages", "Count"=>kids.len() as i64, "Kids"=>kids}.into(),
    );
    let catalog = doc.add_object(dictionary! {"Type"=>"Catalog", "Pages"=>pages});
    doc.trailer.set("Root", catalog);
    let mut bytes = vec![];
    doc.save_to(&mut bytes).unwrap();
    bytes
}

#[test]
fn unicode_mapping_precedes_rendering_differences() {
    let bytes = pdf(
        "2 beginbfchar\n<01> <4E2D>\n<02> <6587>\nendbfchar",
        vec![1, 2],
        false,
    );
    // This proves the production dependency's old path fails on the same bytes.
    assert!(
        !Document::load_mem(&bytes)
            .unwrap()
            .extract_text(&[1])
            .unwrap()
            .contains("中文")
    );
    let parsed = extract(bytes).unwrap();
    assert_eq!(parsed.text, "# Page 1\n中文\n");
    assert_eq!(parsed.quality.pages_with_text, [1]);
    assert!(parsed.quality.issues.is_empty());
}

#[test]
fn unmapped_codes_and_empty_pages_preserve_explicit_coverage_gaps() {
    let parsed = extract(pdf(
        "1 beginbfchar\n<01> <4E2D>\nendbfchar",
        vec![1, 2],
        true,
    ))
    .unwrap();
    assert!(parsed.text.contains("中�"));
    assert_eq!(parsed.quality.pages_total, Some(2));
    assert_eq!(parsed.quality.pages_with_text, [1]);
    assert!(
        parsed
            .quality
            .issues
            .iter()
            .any(|i| i.page == Some(1) && i.code == "pdf_unmapped_character_code")
    );
    assert!(
        parsed
            .quality
            .issues
            .iter()
            .any(|i| i.page == Some(2) && i.code == "pdf_page_has_no_extracted_text")
    );
}

#[test]
fn malformed_mapping_cannot_fall_back_to_false_latin_text() {
    let parsed = extract(pdf("this is not a cmap section", vec![1, 2], false)).unwrap();
    assert!(!parsed.text.contains("AB"));
    assert!(!parsed.quality.issues.is_empty());
}

#[test]
fn unicode_sequences_and_surrogate_pairs_survive_extraction() {
    let parsed = extract(pdf(
        "2 beginbfchar\n<01> <00660069>\n<02> <D83DDE42>\nendbfchar",
        vec![1, 2],
        false,
    ))
    .unwrap();
    assert!(parsed.text.contains("fi🙂"));
    assert!(parsed.quality.issues.is_empty());
}

#[tokio::test]
async fn host_worker_parses_pdf_through_os_limits_and_bounded_protocol() {
    let bytes = pdf(
        "2 beginbfchar\n<01> <4E2D>\n<02> <6587>\nendbfchar",
        vec![1, 2],
        false,
    );
    let material = crate::parser_process::extract("application/pdf", bytes)
        .await
        .expect("physical PDF worker");
    assert_eq!(material.text, "# Page 1\n中文\n");
    assert_eq!(material.quality.pages_with_text, [1]);
}
