//! Bounded UTF-8 paging over the original verified content reader.
use crate::{ContentStoreError, VerifiedContentReader};

#[derive(Debug, thiserror::Error)]
pub enum ContentTextError {
    #[error("invalid text page range or encoding")]
    Invalid,
    #[error("content read failed")]
    Content(#[from] ContentStoreError),
}

/// Uses the content owner's blocking IO lane. Authorization remains at the caller;
/// possession of a Content ID never authorizes this read.
pub async fn read_verified_text_page(
    reader: VerifiedContentReader,
    offset: u64,
    max_bytes: usize,
) -> Result<String, ContentTextError> {
    tokio::task::spawn_blocking(move || read_page(&reader, offset, max_bytes))
        .await
        .map_err(|_| ContentTextError::Content(ContentStoreError::Unavailable))?
}

fn read_page(
    reader: &VerifiedContentReader,
    offset: u64,
    max_bytes: usize,
) -> Result<String, ContentTextError> {
    let length = reader.reference().byte_length;
    if !(4..=65536).contains(&max_bytes) || offset > length {
        return Err(ContentTextError::Invalid);
    }
    let count = (length - offset).min(max_bytes as u64) as usize;
    let mut bytes = zeroize::Zeroizing::new(vec![0; count]);
    reader.read_exact_at(&mut bytes, offset)?;
    page_text(&bytes, offset + count as u64 == length)
}

fn page_text(bytes: &[u8], final_page: bool) -> Result<String, ContentTextError> {
    match std::str::from_utf8(bytes) {
        Ok(value) => Ok(value.to_owned()),
        Err(error) if !final_page && error.error_len().is_none() && error.valid_up_to() > 0 => {
            Ok(std::str::from_utf8(&bytes[..error.valid_up_to()])
                .map_err(|_| ContentTextError::Invalid)?
                .to_owned())
        }
        Err(_) => Err(ContentTextError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paging_preserves_utf8_without_accepting_invalid_offsets_or_final_bytes() {
        assert_eq!(page_text(&[b'a', 0xe4, 0xb8], false).unwrap(), "a");
        assert!(page_text(&[0xb8, 0xad], false).is_err());
        assert!(page_text(&[b'a', 0xe4, 0xb8], true).is_err());
        assert_eq!(page_text("中".as_bytes(), true).unwrap(), "中");
        assert_eq!(page_text(&[], true).unwrap(), "");
    }
}
