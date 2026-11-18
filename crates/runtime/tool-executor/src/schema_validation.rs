mod compiler;
mod diagnostics;
use diagnostics::DetailedIssue;
use std::collections::BTreeMap;

use jsonschema::error::ValidationErrorKind;
use musteragent_content_ref::Sha256;
use musteragent_loop::LoopPortError;
use musteragent_runtime_capability::ToolTerminalOutcome;
use musteragent_runtime_contract::{
    ErrorCode, ErrorInfo, RetryDisposition, SafeDisplayText, ToolArgumentValidationIssue,
    ToolArgumentValidationIssueKind,
};

/// Shared process-local cache of immutable Snapshot-pinned Draft 2020-12 validators.
/// Cache misses validate the Schema document itself before compiling it;
/// remote and file resolution stay disabled by Cargo features.
pub(crate) struct ToolSchemaValidatorCache;

impl ToolSchemaValidatorCache {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn validate(
        &self,
        schema_hash: Sha256,
        schema_bytes: &[u8],
        instance: &serde_json::Value,
    ) -> Result<(), SchemaValidationError> {
        let validator =
            compiler::get(schema_hash, schema_bytes).map_err(SchemaValidationError::Contract)?;
        validate_instance(&validator, instance)
    }

    pub(crate) async fn validate_async(
        &self,
        schema_hash: Sha256,
        schema_bytes: &[u8],
        instance: &serde_json::Value,
    ) -> Result<(), SchemaValidationError> {
        let validator = compiler::get_async(schema_hash, schema_bytes)
            .await
            .map_err(SchemaValidationError::Contract)?;
        validate_instance(&validator, instance)
    }
}

fn validate_instance(
    validator: &jsonschema::Validator,
    instance: &serde_json::Value,
) -> Result<(), SchemaValidationError> {
    let mut invalid = false;
    let violations = validator
        .iter_errors(instance)
        .inspect(|_| invalid = true)
        .flat_map(|error| schema_violations(&error, 0))
        .collect::<Vec<_>>();
    if invalid {
        Err(SchemaValidationError::from_details(violations))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SchemaValidationError {
    /// Pinned schema material or the validator is broken. The model cannot
    /// repair this by changing arguments, so never return argument feedback.
    Contract(ErrorCode),
    Arguments {
        issues: Vec<ToolArgumentValidationIssue>,
        details: Vec<DetailedIssue>,
    },
}

impl SchemaValidationError {
    pub(crate) fn argument_issues(&self) -> Result<&[ToolArgumentValidationIssue], LoopPortError> {
        match self {
            Self::Contract(code) => Err(LoopPortError {
                code: *code,
                retryable: false,
            }),
            Self::Arguments { issues, .. } => Ok(issues),
        }
    }

    fn from_details(mut details: Vec<DetailedIssue>) -> Self {
        diagnostics::normalize(&mut details);
        let mut issues = details.iter().map(|d| d.issue.clone()).collect::<Vec<_>>();
        issues.sort();
        issues.dedup();
        Self::Arguments { issues, details }
    }
    #[cfg(test)]
    fn from_violations(issues: Vec<ToolArgumentValidationIssue>) -> Self {
        Self::from_details(issues.into_iter().map(DetailedIssue::fallback).collect())
    }
    pub(crate) fn diagnostic_json(&self) -> Result<serde_json::Value, LoopPortError> {
        self.argument_issues()?;
        let Self::Arguments { details, .. } = self else {
            unreachable!()
        };
        Ok(diagnostics::report(details))
    }
}

fn schema_violations(error: &jsonschema::ValidationError<'_>, depth: usize) -> Vec<DetailedIssue> {
    let path = error.instance_path().as_str();
    let pointer = if path.is_empty() { "/" } else { path };
    let fallback = || {
        vec![DetailedIssue::at(
            ToolArgumentValidationIssueKind::InvalidValue,
            pointer.to_owned(),
            error,
        )]
    };
    if depth >= 64 {
        return fallback();
    }
    match error.kind() {
        ValidationErrorKind::AnyOf { context } | ValidationErrorKind::OneOfNotValid { context } => {
            branch_violations(context, path, depth + 1)
        }
        ValidationErrorKind::PropertyNames { error } => schema_violations(error, depth + 1),
        ValidationErrorKind::Required { property } => property
            .as_str()
            .map(|property| {
                vec![DetailedIssue::at(
                    ToolArgumentValidationIssueKind::MissingRequired,
                    child_path(path, property),
                    error,
                )]
            })
            .unwrap_or_else(fallback),
        ValidationErrorKind::AdditionalProperties { unexpected }
        | ValidationErrorKind::UnevaluatedProperties { unexpected } => unexpected
            .iter()
            .map(|property| {
                DetailedIssue::at(
                    ToolArgumentValidationIssueKind::UnexpectedProperty,
                    child_path(path, property),
                    error,
                )
            })
            .collect(),
        _ => fallback(),
    }
}
fn child_path(parent: &str, property: &str) -> String {
    format!(
        "{parent}/{}",
        property.replace('~', "~0").replace('/', "~1")
    )
}

fn branch_violations(
    context: &[Vec<jsonschema::ValidationError<'static>>],
    fallback_path: &str,
    depth: usize,
) -> Vec<DetailedIssue> {
    let mut best: Option<((usize, usize, usize), Vec<DetailedIssue>)> = None;
    for branch in context {
        let mut violations = branch
            .iter()
            .filter(|error| error.instance_path().as_str() == fallback_path)
            .chain(
                branch
                    .iter()
                    .filter(|error| error.instance_path().as_str() != fallback_path),
            )
            .flat_map(|error| schema_violations(error, depth))
            .collect::<Vec<_>>();
        diagnostics::normalize(&mut violations);
        if violations.is_empty() {
            continue;
        }
        // An untagged object's own keys select its structural shape. Shared
        // nested mistakes (e.g. numeric EMUs in a shape/picture frame) must not
        // outweigh a missing branch-defining field or extra content fields.
        // Rank the complete branch: counting a display-limited prefix makes
        // unrelated branches tie and incorrectly selects schema declaration order.
        let structural_mismatches = branch
            .iter()
            .filter(|error| error.instance_path().as_str() == fallback_path)
            .map(|error| match error.kind() {
                ValidationErrorKind::Required { .. } | ValidationErrorKind::Type { .. } => 1,
                ValidationErrorKind::AdditionalProperties { unexpected }
                | ValidationErrorKind::UnevaluatedProperties { unexpected } => unexpected.len(),
                _ => 0,
            })
            .sum::<usize>();
        // A branch whose const discriminator does not match is not the chosen
        // operation, even if it happens to require fewer fields. Inspect the
        // entire union, including operations beyond the old 16-branch prefix.
        let discriminator_mismatches = branch
            .iter()
            .filter(|error| matches!(error.kind(), ValidationErrorKind::Constant { .. }))
            .count();
        let score = (
            discriminator_mismatches,
            structural_mismatches,
            violations.len(),
        );
        if best
            .as_ref()
            .is_none_or(|(best_score, _)| score < *best_score)
        {
            best = Some((score, violations));
        }
    }
    best.map(|(_, violations)| violations)
        .unwrap_or_else(|| invalid_value_violation(fallback_path))
}

fn invalid_value_violation(path: &str) -> Vec<DetailedIssue> {
    let path = if path.is_empty() { "/" } else { path };
    let issue =
        ToolArgumentValidationIssue::new(ToolArgumentValidationIssueKind::InvalidValue, path)
            .unwrap_or_else(|_| {
                ToolArgumentValidationIssue::new(ToolArgumentValidationIssueKind::InvalidValue, "/")
                    .expect("root pointer")
            });
    let mut detail = DetailedIssue::fallback(issue);
    detail.path = path.to_owned();
    vec![detail]
}

pub(crate) fn invalid_arguments_outcome(
    validation: &SchemaValidationError,
) -> Result<ToolTerminalOutcome, LoopPortError> {
    validation.argument_issues()?;
    let SchemaValidationError::Arguments { details, .. } = validation else {
        unreachable!()
    };
    let code = ErrorCode::InvalidCommand;
    let message = format!(
        "Tool arguments do not match the active versioned schema ({} affected fields). No tool execution or side effects occurred. Full structured diagnostics accompany this rejection. Correct the indicated fields, then make a new call. retryable=false prohibits replaying unchanged arguments; it does not prohibit correcting them.",
        details.len()
    );
    let safe_message = SafeDisplayText::new(message).map_err(|_| LoopPortError {
        code: ErrorCode::InternalInvariantViolated,
        retryable: false,
    })?;
    Ok(ToolTerminalOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message,
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    })
}

pub(crate) fn planning_failure_outcome(
    failure: musteragent_loop::ToolCallPlanningFailure,
) -> Result<ToolTerminalOutcome, LoopPortError> {
    let code = ErrorCode::InvalidCommand;
    Ok(ToolTerminalOutcome::Failed {
        error: ErrorInfo {
            code,
            category: code.category(),
            safe_message: SafeDisplayText::new(failure.diagnostic()).map_err(|_| {
                LoopPortError {
                    code: ErrorCode::InternalInvariantViolated,
                    retryable: false,
                }
            })?,
            retryable: false,
            dependency_ref: None,
            diagnostic_ref: None,
        },
        retry_disposition: RetryDisposition::DoNotRetry,
        diagnostic_result: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_diagnostics_match_the_closed_result_schema() {
        use musteragent_loop::{PlannedToolCall, ToolCallPlanningFailure};
        use musteragent_runtime_model::{ArgumentDiagnostic, ArgumentSource, StopReason};
        let schema: serde_json::Value = serde_json::from_slice(include_bytes!(
            "schema_validation/protocol-diagnostic.schema.json"
        ))
        .expect("schema");
        jsonschema::draft202012::meta::validate(&schema).expect("valid schema");
        let validator = jsonschema::draft202012::options()
            .build(&schema)
            .expect("validator");
        for failure in [
            ToolCallPlanningFailure::Truncated,
            ToolCallPlanningFailure::MalformedJson,
            ToolCallPlanningFailure::ObjectRequired,
            ToolCallPlanningFailure::InvalidValue,
        ] {
            for legacy in [true, false] {
                let mut diagnostic =
                    ArgumentDiagnostic::observed("{", ArgumentSource::Stream, None);
                diagnostic.finish(
                    if failure == ToolCallPlanningFailure::Truncated {
                        StopReason::MaxOutputTokens
                    } else {
                        StopReason::ToolCalls
                    },
                    16384,
                    1048576,
                    None,
                );
                let call = PlannedToolCall {
                    call_id: musteragent_identity::ToolCallId::new("invalid").expect("call"),
                    name: musteragent_tool::WireName::new("test").expect("name"),
                    arguments: musteragent_runtime_model::JsonValue::Object(BTreeMap::new()),
                    provider_replay: None,
                    planning_failure: Some(failure),
                    argument_diagnostic: (!legacy).then_some(diagnostic),
                };
                let mut value = failure.diagnostic_json(&call).expect("diagnostic");
                validator.validate(&value).expect("conforms");
                value["raw_arguments"] = serde_json::json!("private");
                assert!(!validator.is_valid(&value));
            }
        }
    }

    #[test]
    fn full_diagnostics_retain_every_field_and_expected_type_without_duplicate_rules() {
        let schema = serde_json::json!({"type":"array","items":{"type":"object","properties":{
            "x":{"type":"string","not":{"type":"number"}}},"required":["x"]}});
        let bytes = serde_json::to_vec(&schema).expect("valid schema diagnostic fixture");
        let value = serde_json::json!(
            (0..657)
                .map(|_| serde_json::json!({"x":123}))
                .collect::<Vec<_>>()
        );
        let validation = ToolSchemaValidatorCache::new()
            .validate(Sha256::digest(&bytes), &bytes, &value)
            .expect_err("invalid tool input must fail before dispatch");
        let report = validation
            .diagnostic_json()
            .expect("valid schema diagnostic fixture");
        assert_eq!(report["repair"]["issue_count"], 657);
        let groups = report["repair"]["groups"]
            .as_array()
            .expect("valid schema diagnostic fixture");
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0]["constraint"],
            serde_json::json!({"rule":"type","expected":"string"})
        );
        assert_eq!(
            groups[0]["paths"]
                .as_array()
                .expect("valid schema diagnostic fixture")
                .len(),
            657
        );
        assert!(
            groups[0]["paths"]
                .as_array()
                .expect("valid schema diagnostic fixture")
                .contains(&serde_json::json!("/656/x"))
        );
        let contract: serde_json::Value =
            serde_json::from_slice(include_bytes!("schema_validation/diagnostic.schema.json"))
                .expect("valid schema diagnostic fixture");
        assert!(
            jsonschema::draft202012::options()
                .build(&contract)
                .expect("valid schema diagnostic fixture")
                .is_valid(&report)
        );
    }

    #[test]
    fn diagnostics_keep_unicode_and_escaped_paths_and_independent_not_constraints() {
        let key = format!("{}~/字段", "long".repeat(80));
        let schema = serde_json::json!({"type":"object", "properties": {key.clone(): {"type":"string","not":{}}}});
        let bytes = serde_json::to_vec(&schema).expect("valid schema diagnostic fixture");
        let input = serde_json::json!({key.clone():7});
        let failure = ToolSchemaValidatorCache::new()
            .validate(Sha256::digest(&bytes), &bytes, &input)
            .expect_err("invalid tool input must fail before dispatch");
        let report = failure
            .diagnostic_json()
            .expect("valid schema diagnostic fixture");
        assert_eq!(
            report["repair"]["issue_count"], 2,
            "not:{} is independent of the type repair",
            "{}"
        );
        for group in report["repair"]["groups"]
            .as_array()
            .expect("valid schema diagnostic fixture")
        {
            assert_eq!(
                group["paths"],
                serde_json::json!([format!("/{}", key.replace('~', "~0").replace('/', "~1"))])
            );
        }
    }

    #[test]
    fn long_valid_paths_remain_repairable_and_do_not_break_safe_display() {
        let path = format!("/{}", "x".repeat(255));
        let issues = ["a", "b", "c"]
            .into_iter()
            .map(|suffix| {
                ToolArgumentValidationIssue::new(
                    ToolArgumentValidationIssueKind::MissingRequired,
                    format!("{}{suffix}", &path[..255]),
                )
                .expect("valid schema diagnostic fixture")
            })
            .collect();
        let ToolTerminalOutcome::Failed { error, .. } =
            invalid_arguments_outcome(&SchemaValidationError::from_violations(issues))
                .expect("valid schema diagnostic fixture")
        else {
            panic!()
        };
        assert_eq!(error.code, ErrorCode::InvalidCommand);
        assert!(error.safe_message.as_str().len() <= 1000);
    }

    #[test]
    fn every_active_builtin_input_and_output_schema_compiles_with_pinned_hashes() {
        let cache = ToolSchemaValidatorCache::new();
        for catalog in [
            musteragent_tool::BuiltinToolCatalog::load_cloud().expect("cloud catalog"),
            musteragent_tool::BuiltinToolCatalog::load_device().expect("device catalog"),
        ] {
            for tool in catalog.definitions() {
                for bytes in [&tool.input_schema_json, &tool.output_schema_json] {
                    validate_advertised_defaults(
                        &serde_json::from_slice(bytes).expect("catalog schema JSON"),
                        tool.key.as_str(),
                    );
                    if let Err(failure) =
                        cache.validate(Sha256::digest(bytes), bytes, &serde_json::json!({}))
                    {
                        assert!(
                            failure.argument_issues().is_ok(),
                            "{}: {failure:?}",
                            tool.key.as_str()
                        );
                    }
                }
            }
        }
    }

    fn validate_advertised_defaults(root: &serde_json::Value, name: &str) {
        let registry = jsonschema::Registry::new()
            .add("urn:tool-defaults", root.clone())
            .expect("local schema resource")
            .prepare()
            .expect("resolved local schema");
        walk_defaults(root, "", name, &registry);
    }

    fn walk_defaults(
        schema: &serde_json::Value,
        path: &str,
        name: &str,
        registry: &jsonschema::Registry<'_>,
    ) {
        let Some(object) = schema.as_object() else {
            return;
        };
        if let Some(default) = object.get("default") {
            // Retain root resolution for defaults whose type contains $refs.
            let reference = serde_json::json!({"$ref": format!("urn:tool-defaults#{path}")});
            let validator = jsonschema::draft202012::options()
                .with_registry(registry)
                .build(&reference)
                .expect("valid referenced subschema");
            assert!(
                validator.is_valid(default),
                "{name}{path}: advertised default contradicts its schema"
            );
        }
        for key in [
            "properties",
            "patternProperties",
            "$defs",
            "definitions",
            "dependentSchemas",
        ] {
            if let Some(children) = object.get(key).and_then(serde_json::Value::as_object) {
                for (child_key, child) in children {
                    let escaped = child_key.replace('~', "~0").replace('/', "~1");
                    walk_defaults(child, &format!("{path}/{key}/{escaped}"), name, registry);
                }
            }
        }
        for key in [
            "items",
            "additionalProperties",
            "unevaluatedProperties",
            "unevaluatedItems",
            "contains",
            "propertyNames",
            "not",
            "if",
            "then",
            "else",
            "contentSchema",
        ] {
            if let Some(child) = object.get(key) {
                walk_defaults(child, &format!("{path}/{key}"), name, registry);
            }
        }
        for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(children) = object.get(key).and_then(serde_json::Value::as_array) {
                for (index, child) in children.iter().enumerate() {
                    walk_defaults(child, &format!("{path}/{key}/{index}"), name, registry);
                }
            }
        }
    }

    #[test]
    fn broken_pinned_schemas_are_system_errors_not_model_feedback() {
        let cache = ToolSchemaValidatorCache::new();
        for (bytes, hash, code) in [
            (
                br#"{"type":"object"}"#.as_slice(),
                Sha256::digest(b"wrong"),
                ErrorCode::ContentIntegrityFailed,
            ),
            (
                b"{broken".as_slice(),
                Sha256::digest(b"{broken"),
                ErrorCode::RuntimeSnapshotInvalid,
            ),
            (
                br#"{"type":7}"#.as_slice(),
                Sha256::digest(br#"{"type":7}"#),
                ErrorCode::RuntimeSnapshotInvalid,
            ),
        ] {
            let failure = cache
                .validate(hash, bytes, &serde_json::json!({}))
                .expect_err("invalid pinned schema");
            assert_eq!(
                failure
                    .argument_issues()
                    .expect_err("schema failure is not an argument issue")
                    .code,
                code
            );
            assert_eq!(
                invalid_arguments_outcome(&failure)
                    .expect_err("schema failure is not a repairable outcome")
                    .code,
                code
            );
        }
    }

    #[test]
    fn invalid_versioned_arguments_are_a_repairable_tool_outcome() {
        let outcome =
            invalid_arguments_outcome(&SchemaValidationError::from_violations(Vec::new()))
                .expect("stable invalid-arguments outcome");
        let ToolTerminalOutcome::Failed {
            error,
            retry_disposition,
            ..
        } = outcome
        else {
            panic!("schema rejection must be a failed Tool outcome");
        };
        assert_eq!(error.code, ErrorCode::InvalidCommand);
        assert!(!error.retryable);
        assert_eq!(retry_disposition, RetryDisposition::DoNotRetry);
        assert!(
            error
                .safe_message
                .as_str()
                .contains("active versioned schema")
        );
    }

    #[test]
    fn top_level_missing_required_field_has_an_exact_safe_path() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "goal": {"type": "string"},
                "name": {"type": "string"}
            },
            "required": ["name", "goal"],
            "type": "object"
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&schema_bytes),
                &schema_bytes,
                &serde_json::json!({"goal": "inspect workspace"}),
            )
            .expect_err("name must be required");
        let outcome = invalid_arguments_outcome(&validation).expect("repair outcome");
        let ToolTerminalOutcome::Failed { .. } = outcome else {
            panic!("schema rejection must fail");
        };
        assert!(
            validation
                .diagnostic_json()
                .expect("valid schema diagnostic fixture")
                .to_string()
                .contains("/name")
        );
    }

    #[test]
    fn nested_missing_fields_preserve_array_item_paths_without_values() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "edits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "old_text": {"type": "string"},
                            "new_text": {"type": "string"}
                        },
                        "required": ["old_text", "new_text"]
                    }
                }
            },
            "required": ["edits"],
            "type": "object"
        });
        let instance = serde_json::json!({
            "edits": [
                {"old_text": "sensitive old value"},
                {"new_text": "sensitive new value"}
            ]
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(Sha256::digest(&schema_bytes), &schema_bytes, &instance)
            .expect_err("both replacements are incomplete");
        let outcome = invalid_arguments_outcome(&validation).expect("repair outcome");
        let ToolTerminalOutcome::Failed { .. } = outcome else {
            panic!("schema rejection must fail");
        };
        let message = validation
            .diagnostic_json()
            .expect("valid schema diagnostic fixture")
            .to_string();
        assert!(message.contains("/edits/0/new_text"));
        assert!(message.contains("/edits/1/old_text"));
        assert!(!message.contains("sensitive old value"));
        assert!(!message.contains("sensitive new value"));
    }

    #[test]
    fn type_and_additional_property_failures_keep_bounded_paths() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "edits": {"type": "array"}
            },
            "required": ["edits"],
            "type": "object"
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&schema_bytes),
                &schema_bytes,
                &serde_json::json!({"edits": "wrong", "new_text": "private"}),
            )
            .expect_err("type and additional property must fail");
        let outcome = invalid_arguments_outcome(&validation).expect("repair outcome");
        let ToolTerminalOutcome::Failed { .. } = outcome else {
            panic!("schema rejection must fail");
        };
        let message = validation
            .diagnostic_json()
            .expect("valid schema diagnostic fixture")
            .to_string();
        assert!(message.contains("/edits"));
        assert!(message.contains("/new_text"));
        assert!(!message.contains("private"));
    }

    #[test]
    fn one_of_reports_the_closest_nested_branch_instead_of_an_empty_diagnostic() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "oneOf": [
                {
                    "additionalProperties": false,
                    "properties": {"operation": {"const": "list", "type": "string"}},
                    "required": ["operation"],
                    "type": "object"
                },
                {
                    "additionalProperties": false,
                    "properties": {
                        "name": {"type": "string"},
                        "operation": {"const": "read", "type": "string"}
                    },
                    "required": ["operation", "name"],
                    "type": "object"
                }
            ],
            "type": "object"
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&schema_bytes),
                &schema_bytes,
                &serde_json::json!({"operation": "read"}),
            )
            .expect_err("read requires a name");

        let issues = validation.argument_issues().expect("missing field issues");
        assert_eq!(issues.len(), 1);
        assert_eq!(
            issues[0].kind(),
            ToolArgumentValidationIssueKind::MissingRequired
        );
        assert_eq!(issues[0].path(), "/name");
    }

    #[test]
    fn any_of_always_emits_a_safe_actionable_diagnostic() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "anyOf": [
                {"type": "string", "minLength": 4},
                {"type": "integer", "minimum": 10}
            ]
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&schema_bytes),
                &schema_bytes,
                &serde_json::json!(false),
            )
            .expect_err("boolean matches no branch");

        let issues = validation.argument_issues().expect("invalid branch issues");
        assert_eq!(issues.len(), 1);
        assert_eq!(
            issues[0].kind(),
            ToolArgumentValidationIssueKind::InvalidValue
        );
        assert_eq!(issues[0].path(), "/");
    }

    #[test]
    fn one_additional_properties_error_reports_multiple_retired_fields() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "prompt": {"type": "string"}
            },
            "required": ["prompt"],
            "type": "object"
        });
        let schema_bytes = serde_json::to_vec(&schema).expect("schema bytes");
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&schema_bytes),
                &schema_bytes,
                &serde_json::json!({
                    "model": "private-model-id",
                    "model_config_version_id": "private-version-id",
                    "prompt": "safe prompt",
                    "reference_image_urls": ["private-url"],
                    "tariff_version_id": "private-tariff-id"
                }),
            )
            .expect_err("retired private fields must fail");

        let issues = validation.argument_issues().expect("retired field issues");
        assert_eq!(issues.len(), 4);
        assert_eq!(issues[0].path(), "/model");
        assert_eq!(issues[1].path(), "/model_config_version_id");
        assert_eq!(issues[2].path(), "/reference_image_urls");
        assert!(
            issues
                .iter()
                .all(|issue| issue.kind() == ToolArgumentValidationIssueKind::UnexpectedProperty)
        );
    }
    #[test]
    fn native_operation_feedback_uses_its_selected_branch_even_after_the_sixteenth() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../contracts/fixtures/executor-environment-tools.json"
        ))
        .unwrap();
        let tool = musteragent_tool::BuiltinToolDefinition::from_released_document(
            fixtures["device.computer.act@2"].clone(),
        )
        .unwrap();
        let validation = ToolSchemaValidatorCache::new()
            .validate(
                Sha256::digest(&tool.input_schema_json),
                &tool.input_schema_json,
                &serde_json::json!({"tool":"set_value","arguments":{}}),
            )
            .expect_err("set_value requires its branch arguments");
        let ToolTerminalOutcome::Failed { error, .. } =
            invalid_arguments_outcome(&validation).expect("structured invalid argument outcome")
        else {
            panic!("failure")
        };
        assert!(
            validation
                .diagnostic_json()
                .expect("valid schema diagnostic fixture")
                .to_string()
                .contains("/arguments/pid")
        );
        assert!(
            validation
                .diagnostic_json()
                .expect("valid schema diagnostic fixture")
                .to_string()
                .contains("/arguments/value")
        );
        assert!(!error.safe_message.as_str().contains("at /tool"));
    }
}
