//! Opt-in local test diagnostics: only structural failure identifiers are emitted.
//! Provider data, SQL parameters, messages and credentials are never recorded.
use tracing::{
    Event, Metadata, Subscriber,
    span::{Attributes, Id, Record},
};

struct Failures;
struct Fields(String);
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if matches!(
            field.name(),
            "failure.stage"
                | "snapshot.error"
                | "sqlstate"
                | "constraint"
                | "table"
                | "logical_key"
        ) {
            use std::fmt::Write;
            let _ = write!(&mut self.0, " {}={value:?}", field.name());
        }
    }
}
impl Subscriber for Failures {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }
    fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
        Some(tracing::metadata::LevelFilter::WARN)
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        if !fields.0.is_empty() {
            eprintln!("owner-failure{}", fields.0);
        }
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
pub(super) fn install() -> tracing::subscriber::DefaultGuard {
    tracing::subscriber::set_default(Failures)
}
