//! Shared current-source and placement gates for lexical and vector retrieval.
pub(crate) const QUERY: &str = "WITH RECURSIVE node_paths AS (
    SELECT node_id,parent_node_id,ARRAY[name]::text[] AS path_parts
    FROM resource.nodes WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3
      AND parent_node_id IS NULL AND lifecycle=1
    UNION ALL
    SELECT child.node_id,child.parent_node_id,parent.path_parts || child.name
    FROM resource.nodes child JOIN node_paths parent ON parent.node_id=child.parent_node_id
    WHERE child.tenant_id=$1 AND child.scope_kind=$2 AND child.scope_id=$3 AND child.lifecycle=1
), eligible AS (
    SELECT node.node_id,node.file_artifact_id AS artifact_id,
        version.artifact_version_id,version.version::text AS file_version,
        content.media_type,content.content_hash,manifest.manifest_id,
        chunk.chunk_id,chunk.chunk_ordinal,paths.path_parts,chunk.start_byte,chunk.end_byte,
        chunk.page_number,chunk.slide_number,chunk.line_start,chunk.line_end,
        chunk.section_path,chunk.cell_range,chunk.text_content,chunk.text_features
    FROM resource.search_chunks chunk
    JOIN resource.search_manifests manifest ON manifest.manifest_id=chunk.manifest_id
    JOIN resource.nodes node ON node.node_id=manifest.node_id AND node.tenant_id=manifest.tenant_id
        AND node.scope_kind=manifest.scope_kind AND node.scope_id=manifest.scope_id
    JOIN node_paths paths ON paths.node_id=node.node_id
    JOIN artifact.artifacts artifact ON artifact.artifact_id=manifest.artifact_id
        AND artifact.tenant_id=manifest.tenant_id AND artifact.current_version=manifest.file_version
    JOIN resource.file_versions version ON version.artifact_version_id=manifest.artifact_version_id
        AND version.artifact_id=artifact.artifact_id AND version.version=artifact.current_version
        AND version.content_hash=manifest.source_content_hash
    JOIN artifact.content_refs content ON content.tenant_id=manifest.tenant_id
        AND content.content_id=version.content_id AND content.content_hash=manifest.source_content_hash
    WHERE manifest.tenant_id=$1 AND manifest.scope_kind=$2 AND manifest.scope_id=$3
        AND node.kind=2 AND node.lifecycle=1
)";
