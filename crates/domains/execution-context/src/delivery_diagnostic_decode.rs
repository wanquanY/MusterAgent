//! Diagnostics observe a rejected payload; they never replace the original
//! decoder's acceptance or the Publication owner's reference authority.
use crate::*;
use musteragent_identity::{ArtifactId, CommandId, InteractionId, ToolCallId};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicDeliveryDiagnosticFailure {
    pub issue: PublicDeliveryIssue,
    pub diagnostics: Vec<PublicDeliveryDiagnostic>,
}

pub fn parse_public_delivery_v6(
    bytes: &[u8],
) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryDiagnosticFailure> {
    parse_with_diagnostics(PublicDeliveryContract::V6, bytes)
}

pub fn parse_public_delivery_v7(
    bytes: &[u8],
) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryDiagnosticFailure> {
    parse_with_diagnostics(PublicDeliveryContract::V7, bytes)
}

fn parse_with_diagnostics(
    contract: PublicDeliveryContract,
    bytes: &[u8],
) -> Result<Vec<ParsedPublicDelivery>, PublicDeliveryDiagnosticFailure> {
    // The existing strict decoder is the only acceptance authority. In
    // particular, duplicate JSON properties cannot be normalized into success.
    contract.parse(bytes).map_err(|failure| {
        let mut diagnostics = diagnose(bytes, failure);
        diagnostics.sort();
        diagnostics.dedup();
        diagnostics.truncate(MAX_DELIVERY_DIAGNOSTICS);
        PublicDeliveryDiagnosticFailure {
            issue: failure.into(),
            diagnostics,
        }
    })
}

fn issue(kind: PublicDeliveryDiagnosticKind, path: &str) -> PublicDeliveryDiagnostic {
    PublicDeliveryDiagnostic::new(kind, path).expect("owner-generated closed diagnostic path")
}

fn diagnose(bytes: &[u8], failure: PublicDeliveryParseError) -> Vec<PublicDeliveryDiagnostic> {
    use PublicDeliveryDiagnosticKind as K;
    if bytes.len() > MAX_PUBLIC_DELIVERY_BYTES {
        return vec![issue(K::TooLarge, "/")];
    }
    let node = match serde_json::from_slice::<Node>(bytes) {
        Ok(node) => node,
        Err(error) => {
            return vec![issue(
                if error.is_eof() {
                    K::UnexpectedEnd
                } else {
                    K::MalformedJson
                },
                "/",
            )];
        }
    };
    let mut issues = Vec::new();
    let Some(root) = object(&node, "/", &mut issues) else {
        return issues;
    };
    properties(root, &["deliveries"], "/", &mut issues);
    match get(root, "deliveries") {
        None => issues.push(issue(K::MissingRequired, "/deliveries")),
        Some(Node::Array(entries))
            if !entries.is_empty() && entries.len() <= MAX_PUBLIC_DELIVERIES =>
        {
            for (ordinal, entry) in entries.iter().enumerate() {
                entry_issues(entry, &format!("/deliveries/{ordinal}"), &mut issues);
            }
        }
        Some(Node::Array(_)) => issues.push(issue(K::InvalidValue, "/deliveries")),
        _ => issues.push(issue(K::InvalidType, "/deliveries")),
    }
    if issues.is_empty() {
        issues.push(issue(
            if failure == PublicDeliveryParseError::TooLarge {
                K::TooLarge
            } else {
                K::InvalidValue
            },
            "/deliveries",
        ));
    }
    // Reference loss must never be hidden by the bounded structural diagnostics.
    issues.sort_by_key(|entry| {
        (
            !matches!(entry.kind(), K::ReferenceSyntax | K::ReferenceUnavailable),
            entry.clone(),
        )
    });
    issues.truncate(MAX_DELIVERY_DIAGNOSTICS);
    issues
}

fn entry_issues(node: &Node, path: &str, issues: &mut Vec<PublicDeliveryDiagnostic>) {
    use PublicDeliveryDiagnosticKind as K;
    let Some(fields) = object(node, path, issues) else {
        return;
    };
    let kind = required_string(fields, "kind", path, false, issues);
    let allowed: &[&str] = match kind {
        Some("message") => &["kind", "purpose", "text"],
        Some("interaction") => &["kind", "purpose", "interaction_id"],
        Some("artifact") => &["kind", "purpose", "artifact_ref", "artifact_tool_call_id"],
        Some("partner_created") => &[
            "kind",
            "purpose",
            "creation_command_id",
            "partner_tool_call_id",
        ],
        Some(_) => {
            issues.push(issue(K::InvalidValue, &format!("{path}/kind")));
            return;
        }
        None => return,
    };
    properties(fields, allowed, path, issues);
    if let Some(purpose) = required_string(fields, "purpose", path, false, issues) {
        let valid = match kind {
            Some("interaction") => matches!(purpose, "clarification" | "required_decision"),
            Some("artifact") => matches!(purpose, "reply" | "result" | "important_update"),
            Some("partner_created") => purpose == "result",
            _ => matches!(
                purpose,
                "reply"
                    | "result"
                    | "important_update"
                    | "clarification"
                    | "required_decision"
                    | "blocked"
            ),
        };
        if !valid {
            issues.push(issue(K::InvalidValue, &format!("{path}/purpose")));
        }
    }
    match kind {
        Some("message") => {
            if let Some(text) = required_string(fields, "text", path, false, issues) {
                if text.trim().is_empty() {
                    issues.push(issue(K::InvalidValue, &format!("{path}/text")));
                }
                if text.len() > MAX_DIALOGUE_MESSAGE_BYTES {
                    issues.push(issue(K::TooLarge, &format!("{path}/text")));
                }
            }
        }
        Some("interaction") => reference(
            fields,
            "interaction_id",
            path,
            |s| s.parse::<InteractionId>().is_ok(),
            issues,
        ),
        Some("partner_created") => {
            reference(
                fields,
                "creation_command_id",
                path,
                |s| s.parse::<CommandId>().is_ok(),
                issues,
            );
            reference(
                fields,
                "partner_tool_call_id",
                path,
                |s| ToolCallId::new(s).is_ok(),
                issues,
            );
        }
        Some("artifact") => {
            reference(
                fields,
                "artifact_tool_call_id",
                path,
                |s| ToolCallId::new(s).is_ok(),
                issues,
            );
            let subpath = format!("{path}/artifact_ref");
            match get(fields, "artifact_ref") {
                Some(Node::Object(locator)) => {
                    properties(locator, &["kind", "id"], &subpath, issues);
                    match required_string(locator, "kind", &subpath, true, issues) {
                        Some("artifact" | "file_node") => {}
                        Some(_) => {
                            issues.push(issue(K::ReferenceSyntax, &format!("{subpath}/kind")))
                        }
                        None => {}
                    }
                    reference(
                        locator,
                        "id",
                        &subpath,
                        |s| s.parse::<ArtifactId>().is_ok(),
                        issues,
                    );
                }
                _ => issues.push(issue(K::ReferenceSyntax, &subpath)),
            }
        }
        _ => {}
    }
}
fn object<'a>(
    node: &'a Node,
    path: &str,
    issues: &mut Vec<PublicDeliveryDiagnostic>,
) -> Option<&'a [(String, Node)]> {
    match node {
        Node::Object(fields) => Some(fields),
        _ => {
            issues.push(issue(PublicDeliveryDiagnosticKind::ObjectRequired, path));
            None
        }
    }
}
fn properties(
    fields: &[(String, Node)],
    allowed: &[&str],
    path: &str,
    issues: &mut Vec<PublicDeliveryDiagnostic>,
) {
    let mut seen = BTreeSet::new();
    for (name, _) in fields {
        let known = allowed.contains(&name.as_str());
        // Do not retain an attacker-controlled unknown key, even in a path.
        let field_path = if known {
            format!(
                "{}{name}",
                if path == "/" {
                    "/".into()
                } else {
                    format!("{path}/")
                }
            )
        } else {
            path.into()
        };
        if !seen.insert(name) {
            issues.push(issue(
                PublicDeliveryDiagnosticKind::DuplicateProperty,
                &field_path,
            ));
        }
        if !known {
            issues.push(issue(
                PublicDeliveryDiagnosticKind::UnexpectedProperty,
                path,
            ));
        }
    }
}
fn get<'a>(fields: &'a [(String, Node)], name: &str) -> Option<&'a Node> {
    fields
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}
fn required_string<'a>(
    fields: &'a [(String, Node)],
    name: &str,
    path: &str,
    is_reference: bool,
    issues: &mut Vec<PublicDeliveryDiagnostic>,
) -> Option<&'a str> {
    match get(fields, name) {
        Some(Node::String(value)) => Some(value),
        value => {
            use PublicDeliveryDiagnosticKind as K;
            issues.push(issue(
                if is_reference {
                    K::ReferenceSyntax
                } else if value.is_none() {
                    K::MissingRequired
                } else {
                    K::InvalidType
                },
                &format!("{path}/{name}"),
            ));
            None
        }
    }
}
fn reference(
    fields: &[(String, Node)],
    name: &str,
    path: &str,
    valid: impl Fn(&str) -> bool,
    issues: &mut Vec<PublicDeliveryDiagnostic>,
) {
    if let Some(value) = required_string(fields, name, path, true, issues)
        && !valid(value)
    {
        issues.push(issue(
            PublicDeliveryDiagnosticKind::ReferenceSyntax,
            &format!("{path}/{name}"),
        ));
    }
}

// Duplicate-preserving diagnostic tree. It is bounded by the original byte
// limit and serde's recursion limit, and never used to accept a delivery.
enum Node {
    Object(Vec<(String, Node)>),
    Array(Vec<Node>),
    String(String),
    Other,
}
impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Tree;
        impl<'de> Visitor<'de> for Tree {
            type Value = Node;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON value")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut m: M) -> Result<Node, M::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = m.next_entry()? {
                    fields.push(entry);
                }
                Ok(Node::Object(fields))
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut s: S) -> Result<Node, S::Error> {
                let mut items = Vec::new();
                while let Some(item) = s.next_element()? {
                    items.push(item);
                }
                Ok(Node::Array(items))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Node, E> {
                Ok(Node::String(value.into()))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Node, E> {
                Ok(Node::String(value))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Node, E> {
                Ok(Node::Other)
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Node, E> {
                Ok(Node::Other)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Node, E> {
                Ok(Node::Other)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Node, E> {
                Ok(Node::Other)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Node, E> {
                Ok(Node::Other)
            }
        }
        d.deserialize_any(Tree)
    }
}
