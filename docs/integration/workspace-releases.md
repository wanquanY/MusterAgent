# Host-published workspace tools

Set the following top-level TOML field in each host that publishes workspace releases. A relative path is resolved against the TOML file. The reference API example points to a working bundle with three synthetic releases. [Example bundle](../../applications/reference-api/examples/workspace-releases.json)

```toml
workspace_releases_file = "workspace-releases.json"
```

Both the reference API and the standalone cloud worker read and validate the whole file at startup. An unreadable path, invalid document or conflicting immutable identity fails startup. A deployment using only kernel tools can omit this field. Configuring a workspace tool grant without its matching publication fails with `WorkspaceContractError`.

## Release document

The JSON envelope has two required fields. `schema_version` is `musteragent.workspace-tool-releases.v1`. `releases` is a nonempty array. The bundle limit is 32 MiB and each release object is limited to 4 MiB.

| Object | Required fields | Optional fields |
| --- | --- | --- |
| Release | `release_id`, `plugin_id`, `version`, `release_sha256`, `provider_key`, `tools` | `retires`, an array of dotted capability-family prefixes |
| Tool | `key`, `operation_key`, `operation_sha256`, `title`, `description`, `effect`, `input_schema`, `scope_alternatives` | `input_material`, `provides`, `authority_role`, `runtime_definition`, `implementation` |

Each plugin/version pair occurs once. `provider_key` starts with `workspace_`. Digest strings use 64 lowercase hexadecimal digits. A tool key is `<operation_key>.h_<operation_sha256>@1`. Generic tools take an object input schema and an effect of `read`, `write_external` or `destructive`. Reversible writes require the original runtime transaction contract. Each entry in `scope_alternatives` is a complete acceptable set of scopes. Use `[[]]` for a public operation that requires no connector scopes. An input schema can require user approval with `"x-musteragent-require-user-approval": true`.

`provides` names a versioned capability in a retired family. An explicit authority role can bind that capability's original effect, scopes, approval and egress rules. A release can also carry an original runtime definition with a supported implementation pin. These fields follow the existing workspace release validator. Family retirement matches a capability's logical key or a dotted descendant. The example clock release illustrates the authority-role form.

The host retains each release's JSON object bytes through the existing immutable release content store. Its storage identity is derived from plugin, version and declared release digest. The SHA-256 of the object bytes is the reviewed source digest. Preserve those bytes when distributing an existing release to multiple hosts. Reusing an identity with different bytes produces a publication conflict. Release documents are deployment-public material. Put connection credentials in the host executor's credential store.

## Reference API admission

Add an explicit operation grant under the appropriate `[[agents]]` entry. This example selects the search operation from the example bundle. The principal comes from host authentication, and the provider UUID identifies the host's physical execution port.

```toml
[[agents.workspace_tools]]
principal_id = "01982f80-0000-7000-8000-000000000802"
provider_id = "01982f80-0000-7000-8000-000000000809"
plugin_id = "mcp.example-search"
version = "1.0.0"
release_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
operation_ref = "mcp.example_search.find.h_cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc@1"
scopes = []
```

The API resolves the exact operation and checks the granted scope bundle before serving traffic. The grant is part of the immutable Agent version material. The tool is available only to the configured user in that tenant and Agent version. Admission seals its schema, provider, selection and authority witness into the Run. Publishing a bundle alone grants zero operations. The example API configuration deliberately publishes the bundle without granting a connector.

## Execution and replay

A worker reads the selected tool from the Run snapshot. Indexed application catalogs continue to resolve exact index, page and schema references through `ApplicationCatalogMaterialSource`. Restarting a worker with a different publication file does not update an admitted Run.

For physical connector calls, an embedding host can construct the cloud worker's `product_dispatchers`, register its existing `PublishedCapabilityFactory` on the exact product release and supply the dispatcher to the worker. The factory receives the reconstructed, frozen definition and binding. Its physical executor must recheck current installation, principal, Agent authority and connection consent before effects. The standalone worker has no SaaS executor. An attempt without a registered factory returns `CapabilityUnavailable`. Python and MCP sandbox program selection continues to use the sandbox request and its authorized inputs. The workspace release file supplies no executable process or network endpoint.
