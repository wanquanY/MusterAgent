//! Value-free repair diagnostics. Group only identical constraints; keep every
//! affected JSON pointer. Display limits never decide which errors are retained.
use super::*;
use jsonschema::error::TypeKind;
use serde_json::{Value, json};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DetailedIssue {
    pub issue: ToolArgumentValidationIssue,
    pub path: String,
    pub constraint: Value,
}

impl DetailedIssue {
    pub fn at(
        kind: ToolArgumentValidationIssueKind,
        path: String,
        error: &jsonschema::ValidationError<'_>,
    ) -> Self {
        // Legacy UI issue metadata has a restricted path alphabet/length. Its
        // root projection must not restrict the machine-readable JSON pointer.
        let issue = ToolArgumentValidationIssue::new(kind, path.clone())
            .unwrap_or_else(|_| ToolArgumentValidationIssue::new(kind, "/").expect("root pointer"));
        let mut detail = Self::new(issue, error);
        detail.path = path;
        detail
    }

    pub fn new(
        issue: ToolArgumentValidationIssue,
        error: &jsonschema::ValidationError<'_>,
    ) -> Self {
        let kind = error.kind();
        // Only values from the immutable schema, never error.to_string(): the
        // latter includes potentially private argument values.
        let expected = match kind {
            ValidationErrorKind::Type {
                kind: TypeKind::Single(value),
            } => json!(value.to_string()),
            ValidationErrorKind::Type {
                kind: TypeKind::Multiple(values),
            } => json!(values.iter().map(|v| v.to_string()).collect::<Vec<_>>()),
            ValidationErrorKind::Enum { options } => options.clone(),
            ValidationErrorKind::Constant { expected_value } => expected_value.clone(),
            ValidationErrorKind::Pattern { pattern } => json!(pattern),
            ValidationErrorKind::Format { format } => json!(format),
            ValidationErrorKind::Maximum { limit }
            | ValidationErrorKind::Minimum { limit }
            | ValidationErrorKind::ExclusiveMaximum { limit }
            | ValidationErrorKind::ExclusiveMinimum { limit } => limit.clone(),
            ValidationErrorKind::MaxItems { limit }
            | ValidationErrorKind::MinItems { limit }
            | ValidationErrorKind::MaxLength { limit }
            | ValidationErrorKind::MinLength { limit }
            | ValidationErrorKind::MaxProperties { limit }
            | ValidationErrorKind::MinProperties { limit } => json!(limit),
            ValidationErrorKind::Required { .. } => json!("present"),
            ValidationErrorKind::AdditionalProperties { .. }
            | ValidationErrorKind::UnevaluatedProperties { .. } => {
                json!("remove unknown field or use the schema-defined field name")
            }
            ValidationErrorKind::Not { schema } => schema.clone(),
            _ => Value::Null,
        };
        Self {
            path: issue.path().to_owned(),
            issue,
            constraint: json!({"rule":kind.keyword(), "expected":expected}),
        }
    }
    pub fn fallback(issue: ToolArgumentValidationIssue) -> Self {
        Self {
            path: issue.path().to_owned(),
            issue,
            constraint: json!({"rule":"schema", "expected":"conform to the active schema"}),
        }
    }
}

pub(super) fn normalize(issues: &mut Vec<DetailedIssue>) {
    // Type-inapplicable pattern negation rejects a number only because JSON
    // Schema patterns apply to strings. Report the positive string type once.
    // Keep independent negations (including not:{}) and all string-value rules.
    let typed: std::collections::BTreeSet<_> = issues
        .iter()
        .filter(|v| v.constraint["rule"] == "type" && v.constraint["expected"] == "string")
        .map(|v| v.path.clone())
        .collect();
    issues.retain(|v| {
        !(v.constraint["rule"] == "not"
            && (v.constraint["expected"] == json!({"type":"number"})
                || v.constraint["expected"].as_object().is_some_and(|o| {
                    o.len() == 1 && o.get("pattern").is_some_and(Value::is_string)
                }))
            && typed.contains(&v.path))
    });
    issues.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then_with(|| a.issue.kind().cmp(&b.issue.kind()))
            .then_with(|| a.constraint.to_string().cmp(&b.constraint.to_string()))
    });
    issues.dedup();
}

pub(super) fn report(details: &[DetailedIssue]) -> Value {
    let mut groups: BTreeMap<(String, String), (Value, Vec<String>)> = BTreeMap::new();
    for detail in details {
        let key = (
            detail.issue.kind().as_str().to_owned(),
            detail.constraint.to_string(),
        );
        groups
            .entry(key)
            .or_insert_with(|| (detail.constraint.clone(), Vec::new()))
            .1
            .push(detail.path.clone());
    }
    let groups = groups
        .into_iter()
        .map(|((kind, _), (constraint, mut paths))| {
            paths.sort();
            paths.dedup();
            json!({"kind":kind,"constraint":constraint,"count":paths.len(),"paths":paths})
        })
        .collect::<Vec<_>>();
    json!({
        "ok":false,
        "error":{"code":"invalid_command","retryable":false,
            "message":"Arguments do not match the active versioned schema. Correct the indicated fields and make a new call; do not replay unchanged arguments."},
        "execution_started":false,
        "repair":{"action":"correct_arguments","issue_count":details.len(),"groups":groups}
    })
}
