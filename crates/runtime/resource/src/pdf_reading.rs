//! PDF text interpretation with Unicode precedence and explicit coverage.
//! Encoding selects glyphs for rendering; ToUnicode maps source codes to text.
//! Keep original bytes untouched and reject lossy font fallbacks as evidence.
use crate::representation::ExtractionQuality;
use crate::search_indexing::{MAX_EXTRACTED_TEXT_BYTES, ParsedSearchText, SearchExtractionError};
use lopdf::{Dictionary, Document, Encoding, Object};
use std::collections::BTreeMap;

const MAX_PAGES: usize = 10_000;

pub(crate) fn extract(bytes: Vec<u8>) -> Result<ParsedSearchText, SearchExtractionError> {
    let document = Document::load_mem_with_options(
        &bytes,
        lopdf::LoadOptions::with_max_decompressed_size(MAX_EXTRACTED_TEXT_BYTES),
    )
    .map_err(map_error)?;
    let pages = document.get_pages();
    if pages.len() > MAX_PAGES {
        return Err(SearchExtractionError::OutputTooLarge);
    }
    let mut quality = ExtractionQuality {
        pages_total: Some(pages.len() as u32),
        ..Default::default()
    };
    let mut text = String::new();
    for (number, id) in pages {
        let fonts = document.get_page_fonts(id).map_err(map_error)?;
        let encodings = fonts
            .into_iter()
            .map(|(name, font)| text_encoding(&document, font).map(|encoding| (name, encoding)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let bytes = document
            .get_page_content_with_limit(id, MAX_EXTRACTED_TEXT_BYTES)
            .map_err(map_error)?;
        let content = lopdf::content::Content::decode(&bytes).map_err(map_error)?;
        let mut page = String::new();
        let mut font = None;
        let mut stack = Vec::new();
        for operation in content.operations {
            match operation.operator.as_str() {
                "q" => stack.push(font),
                "Q" => {
                    font = stack.pop().unwrap_or(None);
                }
                "Tf" => {
                    font = operation
                        .operands
                        .first()
                        .and_then(|o| o.as_name().ok())
                        .and_then(|name| encodings.get(name));
                }
                "Tj" | "TJ" | "'" | "\"" => {
                    if matches!(operation.operator.as_str(), "'" | "\"") {
                        newline(&mut page);
                    }
                    let values = if operation.operator == "\"" {
                        operation.operands.get(2..).unwrap_or_default()
                    } else {
                        operation.operands.as_slice()
                    };
                    for value in values {
                        append_operand(&mut page, font, value, number, &mut quality)?;
                    }
                }
                "ET" | "T*" | "TD" => newline(&mut page),
                "Td" => {
                    if operation
                        .operands
                        .get(1)
                        .and_then(|o| o.as_float().ok())
                        .is_some_and(|y| y != 0.0)
                    {
                        newline(&mut page);
                    }
                }
                // Form XObjects may contain text, and images may need vision.
                // The decoder does not claim to have extracted either here.
                "Do" => quality.issue("pdf_xobject_requires_visual_review", Some(number)),
                _ => {}
            }
            if page.len().saturating_add(text.len()) > MAX_EXTRACTED_TEXT_BYTES {
                return Err(SearchExtractionError::OutputTooLarge);
            }
        }
        if page.trim().is_empty() {
            quality.issue("pdf_page_has_no_extracted_text", Some(number));
        } else {
            quality.pages_with_text.push(number);
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            text.push_str(&format!("# Page {number}\n{}\n", page.trim()));
        }
        if text.len() > MAX_EXTRACTED_TEXT_BYTES {
            return Err(SearchExtractionError::OutputTooLarge);
        }
    }
    if text.is_empty() {
        return Err(SearchExtractionError::OcrRequired);
    }
    Ok(ParsedSearchText {
        text,
        parser_key: "resource.pdf-text",
        parser_revision: "1",
        media_type: "text/markdown",
        quality,
    })
}

enum TextEncoding<'a> {
    Decoded(Encoding<'a>),
    Unavailable,
}

fn text_encoding<'a>(
    document: &'a Document,
    font: &'a Dictionary,
) -> Result<TextEncoding<'a>, SearchExtractionError> {
    if font.has(b"ToUnicode") {
        // Ask the library to parse only the Unicode mapping, without its
        // rendering-encoding precedence. No source font or document mutation.
        let mut unicode = Dictionary::new();
        unicode.set("Type", Object::Name(b"Font".to_vec()));
        unicode.set(
            "ToUnicode",
            font.get(b"ToUnicode").map_err(map_error)?.clone(),
        );
        return match unicode
            .get_font_encoding_with_limit(document, MAX_EXTRACTED_TEXT_BYTES)
            .map_err(map_error)?
        {
            Encoding::UnicodeMapEncoding(map) => {
                Ok(TextEncoding::Decoded(Encoding::UnicodeMapEncoding(map)))
            }
            // lopdf substitutes StandardEncoding after malformed CMaps. That
            // substitution is unsuitable for Agent evidence; never accept it.
            _ => Ok(TextEncoding::Unavailable),
        };
    }
    if font.get(b"Subtype").and_then(Object::as_name).ok() == Some(b"Type3") {
        // Type3 glyph programs have no inferable Unicode meaning.
        return Ok(TextEncoding::Unavailable);
    }
    Ok(TextEncoding::Decoded(
        font.get_font_encoding_with_limit(document, MAX_EXTRACTED_TEXT_BYTES)
            .map_err(map_error)?,
    ))
}

fn append_operand(
    text: &mut String,
    encoding: Option<&TextEncoding<'_>>,
    value: &Object,
    page: u32,
    quality: &mut ExtractionQuality,
) -> Result<(), SearchExtractionError> {
    match value {
        Object::String(bytes, _) => {
            let Some(TextEncoding::Decoded(encoding)) = encoding else {
                quality.issue("pdf_font_mapping_unavailable", Some(page));
                text.push('\u{fffd}');
                return Ok(());
            };
            match encoding {
                Encoding::UnicodeMapEncoding(map) => {
                    let mut offset = 0;
                    while offset < bytes.len() {
                        let mut code = 0u32;
                        let mut found = None;
                        for length in 1..=4.min(bytes.len() - offset) {
                            code = (code << 8) | u32::from(bytes[offset + length - 1]);
                            if let Some(value) = map.get(code, length as u8) {
                                found = Some((length, value));
                                break;
                            }
                        }
                        if let Some((length, utf16)) = found {
                            for character in char::decode_utf16(utf16) {
                                match character {
                                    Ok(c) => text.push(c),
                                    Err(_) => {
                                        text.push('\u{fffd}');
                                        quality.issue("pdf_invalid_unicode_mapping", Some(page));
                                    }
                                }
                            }
                            offset += length;
                        } else {
                            text.push('\u{fffd}');
                            quality.issue("pdf_unmapped_character_code", Some(page));
                            offset += 1;
                        }
                        if text.len() > MAX_EXTRACTED_TEXT_BYTES {
                            return Err(SearchExtractionError::OutputTooLarge);
                        }
                    }
                }
                _ => {
                    let decoded = encoding.bytes_to_string(bytes).map_err(map_error)?;
                    if !bytes.is_empty() && decoded.is_empty() {
                        quality.issue("pdf_font_mapping_unavailable", Some(page));
                    }
                    text.push_str(&decoded);
                }
            }
        }
        Object::Array(values) => {
            for value in values {
                append_operand(text, encoding, value, page, quality)?;
            }
        }
        // PDF text arrays use large negative adjustments as inter-word spacing.
        Object::Integer(v) if *v < -100 => {
            if !text.ends_with(' ') {
                text.push(' ');
            }
        }
        Object::Real(v) if *v < -100.0 => {
            if !text.ends_with(' ') {
                text.push(' ');
            }
        }
        _ => {}
    }
    Ok(())
}

fn newline(text: &mut String) {
    if !text.ends_with('\n') {
        text.push('\n');
    }
}

pub(crate) fn map_error(error: lopdf::Error) -> SearchExtractionError {
    match error {
        lopdf::Error::Decompress(lopdf::DecompressError::MemoryLimitExceeded { .. }) => {
            SearchExtractionError::OutputTooLarge
        }
        _ => SearchExtractionError::ParserFailed,
    }
}

#[cfg(test)]
mod tests;
