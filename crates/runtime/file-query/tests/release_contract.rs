mod support;
use musteragent_tool::BuiltinToolCatalog;
use serde_json::{Value, json};
use support::*;
fn validate(catalog: &BuiltinToolCatalog, operation: &str, input: Value, output: Value) {
    let key = format!("files.{operation}");
    let definition = catalog
        .definitions()
        .iter()
        .find(|tool| tool.key.logical_key() == key)
        .expect("published unified file tool");
    for (schema, value) in [
        (&definition.input_schema_json, input),
        (&definition.output_schema_json, output),
    ] {
        let schema: Value = serde_json::from_slice(schema).expect("published JSON schema");
        let validator = jsonschema::validator_for(&schema).expect("published schema validator");
        let errors = validator
            .iter_errors(&value)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        assert!(errors.is_empty(), "{operation}: {errors:?}");
    }
}
#[tokio::test]
async fn released_cloud_and_device_contracts_validate_real_application_results() {
    let f = Fixture::new();
    f.attachment("report.txt", "text/plain", b"report content");
    let list_input = json!({"scope":"conversation"});
    let page = f
        .application
        .list(
            &f.context,
            serde_json::from_value(list_input.clone()).expect("list input"),
        )
        .await
        .expect("list result");
    let file = page.entries[0].file_ref.clone();
    let inspected = f
        .application
        .inspect(&f.context, &file)
        .await
        .expect("inspect result");
    let read_input = json!({"file_ref": file});
    let read = f
        .application
        .read(
            &f.context,
            serde_json::from_value(read_input.clone()).expect("read input"),
        )
        .await
        .expect("read result");
    let search_input = json!({"scope":"conversation", "query":"content"});
    let search = f
        .application
        .search(
            &f.context,
            serde_json::from_value(search_input.clone()).expect("search input"),
        )
        .await
        .expect("search result");
    assert_eq!(search.hits.len(), 1);
    for catalog in [
        BuiltinToolCatalog::load_cloud().expect("cloud catalog"),
        BuiltinToolCatalog::load_device().expect("device catalog"),
    ] {
        // Native placement metadata is emitted by the original native provider.
        // Both direct inspect and nested list/read outputs must accept it.
        let mut native = inspected.clone();
        native.native_workspace_id =
            Some(musteragent_identity::WorkspaceId::generate().to_string());
        native.file_ref.scope = musteragent_runtime_file_query::FileScope::Project;
        native.file_ref.source = musteragent_runtime_file_query::FileSource::Native {
            path: "report.txt".into(),
        };
        validate(
            &catalog,
            "inspect",
            json!({"file_ref":native.file_ref}),
            serde_json::to_value(&native).expect("native entry JSON"),
        );
        let mut native_page = page.clone();
        native_page.entries = vec![native.clone()];
        validate(
            &catalog,
            "list",
            json!({"scope":"project"}),
            serde_json::to_value(native_page).expect("native page JSON"),
        );
        let mut native_read = read.view.clone();
        native_read.entry = native.clone();
        validate(
            &catalog,
            "read",
            json!({"file_ref":native.file_ref}),
            json!({"view":native_read,"visual_content":[]}),
        );
        validate(
            &catalog,
            "list",
            list_input.clone(),
            serde_json::to_value(&page).expect("page JSON"),
        );
        validate(
            &catalog,
            "inspect",
            json!({"file_ref":file}),
            serde_json::to_value(&inspected).expect("inspection JSON"),
        );
        validate(
            &catalog,
            "read",
            read_input.clone(),
            json!({"view":read.view, "visual_content":[]}),
        );
        validate(
            &catalog,
            "search",
            search_input.clone(),
            serde_json::to_value(&search).expect("search JSON"),
        );
    }
}
#[test]
fn list_schema_rejects_authority_arguments() {
    let catalog = BuiltinToolCatalog::load_cloud().expect("cloud catalog");
    let schema: Value = serde_json::from_slice(
        &catalog
            .definitions()
            .iter()
            .find(|tool| tool.key.logical_key() == "files.list")
            .expect("published list tool")
            .input_schema_json,
    )
    .expect("input schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("input schema validator");
    for input in [
        json!({"scope":"conversation","conversation_id":"another"}),
        json!({"scope":"project","root":"/"}),
        json!({"scope":"project","limit":101}),
        json!({"scope":"all"}),
    ] {
        assert!(!validator.is_valid(&input));
    }
}
