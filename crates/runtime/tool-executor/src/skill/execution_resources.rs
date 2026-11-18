//! Optional native adapter for the same authorized Skill disclosure. The raw
//! Skill instructions/ref/hash stay intact; the view adds derived execution context.
use musteragent_discovery::AuthorizedSkillDescriptor;
use musteragent_loop::LoopPortError;
use musteragent_run::RunContext;
use musteragent_runtime_contract::ApplicationFuture;

pub trait SkillExecutionResources: Send + Sync {
    fn prepare<'a>(
        &'a self,
        run: &'a RunContext,
        skill: &'a AuthorizedSkillDescriptor,
    ) -> ApplicationFuture<'a, Result<Option<String>, LoopPortError>>;

    /// Fresh local observations are separate from immutable Skill instructions.
    fn context<'a>(
        &'a self,
        _run: &'a RunContext,
        _skill: &'a AuthorizedSkillDescriptor,
    ) -> ApplicationFuture<'a, Result<Option<String>, LoopPortError>> {
        Box::pin(async { Ok(None) })
    }
}
pub(super) fn instructions(directory: &str) -> String {
    format!(
        "\n\n<runtime_skill_resources>\nThe system has prepared this Skill's original files at {}. Resolve the Skill's scripts, references, fonts and assets relative to that directory. Use the provided original files directly; do not download, reinstall, reconstruct or unpack this Skill. Write generated project files and outputs in the authorized workspace. These resources grant no additional execution or network permissions.\n</runtime_skill_resources>",
        serde_json::json!(directory)
    )
}
