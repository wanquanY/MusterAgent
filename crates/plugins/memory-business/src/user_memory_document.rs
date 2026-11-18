//! The original `user_memory.remember@2` request and stored document contract.
//! Keep field order and validation stable: published Content hashes include
//! these exact JSON bytes. Management decoding does not authorize publication.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserMemoryDocument {
    pub fact_key: String,
    pub content: String,
}

#[derive(Debug)]
pub struct InvalidUserMemoryDocument;

impl UserMemoryDocument {
    pub fn decode(bytes: &[u8]) -> Result<Self, InvalidUserMemoryDocument> {
        let document: Self =
            serde_json::from_slice(bytes).map_err(|_| InvalidUserMemoryDocument)?;
        document.validate()?;
        Ok(document)
    }

    pub fn encode(&self) -> Result<Vec<u8>, InvalidUserMemoryDocument> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| InvalidUserMemoryDocument)
    }

    fn validate(&self) -> Result<(), InvalidUserMemoryDocument> {
        let key = self.fact_key.as_bytes();
        if !(2..=64).contains(&key.len())
            || !key[0].is_ascii_lowercase()
            || !key.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'_' | b'.' | b'-')
            })
            || self.content.trim().is_empty()
            || self.content.len() > 2_000
        {
            Err(InvalidUserMemoryDocument)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_document_bytes_and_utf8_limits_are_unchanged() {
        let bytes = br#"{"fact_key":"preference.language","content":"Use Rust"}"#;
        assert_eq!(
            UserMemoryDocument::decode(bytes).unwrap().encode().unwrap(),
            bytes
        );
        for content in [
            format!("{}ab", "中".repeat(666)),
            format!("a{}", "\0".repeat(1999)),
        ] {
            assert_eq!(content.len(), 2_000);
            let document = UserMemoryDocument {
                fact_key: "preference.reply".into(),
                content,
            };
            let bytes = document.encode().unwrap();
            assert_eq!(
                UserMemoryDocument::decode(&bytes).unwrap().content,
                document.content
            );
        }
    }

    #[test]
    fn rejects_non_contract_json_and_invalid_original_input() {
        for bytes in [
            br#"{}"#.as_slice(),
            br#"{"content":"secret"}"#,
            br#"{"fact_key":"aa","content":"ok","unknown":true}"#,
            br#"{"fact_key":"aa","fact_key":"bb","content":"ok"}"#,
            br#"{"fact_key":"aa","content":"ok","content":"different"}"#,
            br#"{"fact_key":"aa","content":3}"#,
            br#"{"fact_key":"bad key","content":"ok"}"#,
            br#"{"fact_key":"A","content":"ok"}"#,
            br#"{"fact_key":"aa","content":"   "}"#,
            b"{invalid json}",
        ] {
            assert!(UserMemoryDocument::decode(bytes).is_err());
        }
        for (key, content) in [
            ("a".into(), "ok".into()),
            ("a".repeat(65), "ok".into()),
            ("preference.reply".into(), "中".repeat(667)),
        ] {
            assert!(
                UserMemoryDocument {
                    fact_key: key,
                    content
                }
                .encode()
                .is_err()
            );
        }
    }
}
