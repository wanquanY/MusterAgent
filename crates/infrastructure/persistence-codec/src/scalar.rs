use musteragent_identity::UtcTimestamp;
use prost_types::Timestamp;

/// Checked protobuf timestamp conversion; sub-millisecond values are rejected.
pub(crate) fn timestamp(value: Option<Timestamp>) -> Result<UtcTimestamp, ()> {
    let v = value.ok_or(())?;
    if !(0..1_000_000_000).contains(&v.nanos) || v.nanos % 1_000_000 != 0 {
        return Err(());
    }
    let millis = v
        .seconds
        .checked_mul(1000)
        .and_then(|s| s.checked_add(i64::from(v.nanos / 1_000_000)))
        .ok_or(())?;
    Ok(UtcTimestamp::from_unix_millis(millis))
}
