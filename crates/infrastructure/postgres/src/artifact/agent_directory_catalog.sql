WITH files AS (
  SELECT DISTINCT ON (e.artifact_id) e.entry_id,e.seq,
    (CASE
      WHEN a.kind=1 THEN 'markdown'
      WHEN a.kind=4 OR media.mime LIKE 'image/%' THEN 'image'
      WHEN a.kind=5 OR media.mime LIKE 'video/%' THEN 'video'
      WHEN media.mime LIKE 'audio/%' THEN 'audio'
      WHEN media.mime='text/markdown' THEN 'markdown'
      WHEN media.mime IN ('text/csv','text/comma-separated-values','text/tab-separated-values') THEN 'table'
      WHEN media.mime='text/html' THEN 'html'
      WHEN media.mime='application/pdf' THEN 'pdf'
      WHEN media.mime LIKE '%officedocument%' OR a.kind=3
        OR media.mime IN ('application/msword','application/vnd.ms-excel','application/vnd.ms-powerpoint') THEN 'office'
      WHEN media.mime IN ('application/json','application/yaml','text/yaml','application/toml','text/toml',
        'application/javascript','application/typescript','text/typescript','text/x-python',
        'text/x-java-properties','text/javascript','application/xml','text/xml','text/css') THEN 'code'
      WHEN media.mime LIKE 'text/%' THEN 'text'
      WHEN media.mime LIKE '%zip%' OR media.mime LIKE '%tar%'
        OR media.mime IN ('application/vnd.rar','application/x-rar-compressed','application/x-7z-compressed') THEN 'archive'
      ELSE 'binary' END) AS file_type
  FROM agent.agent_dialogue_entries e
  JOIN agent.dialogue_publications p ON p.entry_id=e.entry_id
    AND p.tenant_id=e.tenant_id AND p.owner_id=e.owner_id
    AND p.dialogue_id=e.dialogue_id AND p.agent_id=e.author_agent_id
  JOIN artifact.artifacts a ON a.artifact_id=e.artifact_id AND a.tenant_id=e.tenant_id
  LEFT JOIN artifact.work_item_result_asset_returns returned ON returned.entry_id=e.entry_id AND returned.producer_id<>returned.requester_id
  JOIN artifact.artifact_versions head_version ON head_version.artifact_id=a.artifact_id
    AND head_version.version=COALESCE(returned.version,a.current_version)
  JOIN artifact.content_refs content ON content.tenant_id=a.tenant_id AND content.content_id=head_version.content_id
  CROSS JOIN LATERAL (SELECT lower(btrim(split_part(content.media_type,';',1))) AS mime) media
  WHERE e.tenant_id=$1 AND e.owner_id=$2 AND e.dialogue_id=$3
    AND e.epoch=$4 AND e.seq<=$5 AND a.lifecycle IN (1,2)
    AND ($6::smallint IS NULL OR a.origin=$6)
    AND (e.kind='artifact' OR (e.kind='erased' AND resource.account_file_owned(
      e.tenant_id,e.owner_id,p.agent_id,e.dialogue_id,e.entry_id,e.artifact_id)))
  ORDER BY e.artifact_id,e.seq DESC
)
