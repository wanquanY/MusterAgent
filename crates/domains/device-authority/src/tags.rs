//! Closed wire literals; each singleton defaults to its only valid value.
use serde::{Deserialize, Serialize};

macro_rules! literal {
    ($name:ident, $wire:literal) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            #[default]
            #[serde(rename = $wire)]
            V1,
        }
    };
}

literal!(AuthoritySchema, "musteragent.device-authority-authority.v1");
literal!(
    BackendEnvelopeSchema,
    "musteragent.device-authority.signed.v1"
);
literal!(Issuer, "musteragent.authority-issuer");
literal!(AuthorityAudience, "musteragent.device.membership");
literal!(ActiveState, "active");
