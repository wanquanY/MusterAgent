#![cfg(test)]

//! Full DevicePrivate acceptance/publication/Resource conformance uses the
//! qualified `owner-memory-proposal` successor composition (@2). Legacy
//! acceptance/grant cases can also run with `device` (@1); unqualified
//! profiles exercise the fail-closed acceptance/claim branches. No fixture
//! treats a Device version alone as permission to run this Agent dialogue.
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_postgres::{PostgresAgentDialogueStore, PostgresOwnedAgentStore, migrate};
use musteragent_runtime_session::*;
use musteragent_testkit::IsolatedPostgres;
use sqlx::PgPool;
#[path = "dialogue_fixture/mod.rs"]
mod fixture;
use fixture::*;
#[path = "dialogue_device/mod.rs"]
mod device;
