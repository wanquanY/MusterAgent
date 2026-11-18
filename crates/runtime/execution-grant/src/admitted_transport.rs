//! Validate a transport successor without changing original Run admission.
//!
//! This is a business/expiry constraint, NOT signature or current permission
//! verification. The original authority owner must authenticate every envelope,
//! recheck current policy, account/device/source/control and active Run, then
//! commit its exact expected transport head before returning a successor.
use crate::{EXECUTION_GRANT_SCHEMA_V10, SignedExecutionGrant};
use musteragent_identity::UtcTimestamp;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum AdmittedTransportGrantError {
    #[error("transport renewal requires its original admitted Device grant")]
    InvalidAdmission,
    #[error("transport renewal changed the admitted business contract")]
    BusinessChanged,
    #[error("transport renewal does not own the current finite time window")]
    InvalidWindow,
}

fn require_same_business(
    admitted: &SignedExecutionGrant,
    value: &SignedExecutionGrant,
) -> Result<(), AdmittedTransportGrantError> {
    let mut business = value.claims.clone();
    business.issued_at = admitted.claims.issued_at;
    business.expires_at = admitted.claims.expires_at;
    if business != admitted.claims
        || value.context_transport_annex != admitted.context_transport_annex
    {
        return Err(AdmittedTransportGrantError::BusinessChanged);
    }
    Ok(())
}

/// Validate a source-owner committed transport head for one new RPC. This does
/// not choose that head, authenticate a signature, or extend any deadline.
pub fn validate_admitted_transport_head(
    admitted: &SignedExecutionGrant,
    head: &SignedExecutionGrant,
    now: UtcTimestamp,
) -> Result<(), AdmittedTransportGrantError> {
    use AdmittedTransportGrantError as Error;
    let device = admitted
        .claims
        .identity_binding
        .and_then(|identity| identity.device_id)
        .ok_or(Error::InvalidAdmission)?;
    if admitted.claims.schema_version.as_str() != EXECUTION_GRANT_SCHEMA_V10
        || admitted.claims.audience.as_str() != format!("musteragent.runtime.device.id-{device}")
        || admitted.claims.validate().is_err()
        || head.claims.validate().is_err()
    {
        return Err(Error::InvalidAdmission);
    }
    require_same_business(admitted, head)?;
    let ceiling = admitted
        .claims
        .expires_at
        .unix_millis()
        .checked_sub(admitted.claims.issued_at.unix_millis())
        .filter(|v| *v > 0)
        .ok_or(Error::InvalidWindow)?;
    let length = head
        .claims
        .expires_at
        .unix_millis()
        .checked_sub(head.claims.issued_at.unix_millis())
        .filter(|v| *v > 0)
        .ok_or(Error::InvalidWindow)?;
    if head.claims.issued_at < admitted.claims.issued_at
        || head.claims.expires_at < admitted.claims.expires_at
        || length > ceiling
        || now < head.claims.issued_at
        || now >= head.claims.expires_at
    {
        return Err(Error::InvalidWindow);
    }
    Ok(())
}

/// `admitted` is the immutable proof actually accepted by the local ledger;
/// `previous` is the original source owner's current transport head. They can
/// differ after renewal and after a late original admission acknowledgement.
/// Their identity is not inferred from ordinal proximity or the latest grant.
pub fn validate_admitted_transport_successor(
    admitted: &SignedExecutionGrant,
    previous: &SignedExecutionGrant,
    successor: &SignedExecutionGrant,
    now: UtcTimestamp,
) -> Result<(), AdmittedTransportGrantError> {
    use AdmittedTransportGrantError as Error;
    validate_admitted_transport_head(admitted, successor, now)?;
    if previous.claims.validate().is_err() {
        return Err(Error::InvalidAdmission);
    }
    require_same_business(admitted, previous)?;
    let original = &admitted.claims;
    let window = |value: &SignedExecutionGrant| {
        value
            .claims
            .expires_at
            .unix_millis()
            .checked_sub(value.claims.issued_at.unix_millis())
            .filter(|duration| *duration > 0)
            .ok_or(Error::InvalidWindow)
    };
    let original_window = window(admitted)?;
    if previous.claims.issued_at < original.issued_at
        || previous.claims.expires_at < original.expires_at
        || window(previous)? > original_window
        || window(successor)? > original_window
        || now < previous.claims.expires_at
        || successor.claims.issued_at < previous.claims.expires_at
        || now < successor.claims.issued_at
        || now >= successor.claims.expires_at
    {
        return Err(Error::InvalidWindow);
    }
    Ok(())
}
