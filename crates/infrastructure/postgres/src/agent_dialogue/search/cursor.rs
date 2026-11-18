use super::*;
/// Opaque continuation, not a credential. Account authorization and current
/// epoch are rechecked by the original owner, including for forged cursors.
pub(super) struct Window {
    pub epoch: i64,
    pub at: i64,
    pub before: Option<(i64, uuid::Uuid)>,
}
fn fingerprint(query: &DialogueHistorySearchQuery) -> String {
    Sha256::digest(
        serde_json::to_vec(&serde_json::json!([
            "dialogue-history-search-v1",
            query.actor.tenant_id.to_string(),
            query.actor.principal_id.to_string(),
            query.agent_id.map(|v| v.to_string()),
            query
                .text
                .split_whitespace()
                .map(str::to_lowercase)
                .collect::<Vec<_>>(),
            query.category.key(),
            query.from.map(UtcTimestamp::unix_millis),
            query.to.map(UtcTimestamp::unix_millis)
        ]))
        .expect("scalar query JSON"),
    )
    .to_string()
}
impl Window {
    pub fn read(query: &DialogueHistorySearchQuery, epoch: i64, head: i64) -> Result<Self, Error> {
        let Some(cursor) = &query.cursor else {
            return Ok(Self {
                epoch,
                at: head,
                before: None,
            });
        };
        let parts: Vec<_> = cursor.split('_').collect();
        if parts.len() != 6 || parts[0] != "phs1" || parts[1] != fingerprint(query) {
            return Err(Error::Unavailable);
        }
        let number = |value: &str| -> Result<i64, Error> {
            let n = value.parse::<i64>().map_err(|_| Error::Unavailable)?;
            if n < 0 || n.to_string() != value {
                return Err(Error::Unavailable);
            }
            Ok(n)
        };
        let saved_epoch = number(parts[2])?;
        let at = number(parts[3])?;
        let time = parts[4].parse::<i64>().map_err(|_| Error::Unavailable)?;
        let id = uuid::Uuid::parse_str(parts[5]).map_err(|_| Error::Unavailable)?;
        if saved_epoch != epoch
            || at > head
            || id.is_nil()
            || time.to_string() != parts[4]
            || id.to_string() != parts[5]
        {
            return Err(Error::Unavailable);
        }
        Ok(Self {
            epoch,
            at,
            before: Some((time, id)),
        })
    }
    pub fn continuation(
        &self,
        query: &DialogueHistorySearchQuery,
        time: i64,
        id: uuid::Uuid,
    ) -> String {
        format!(
            "phs1_{}_{}_{}_{}_{}",
            fingerprint(query),
            self.epoch,
            self.at,
            time,
            id
        )
    }
}
