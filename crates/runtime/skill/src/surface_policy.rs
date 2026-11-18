//! Package restrictions narrow host compatibility; they never grant execution.
use crate::SkillArchiveAuditError;
use musteragent_skill::Surface;
use std::collections::BTreeSet;

pub(crate) fn parse(
    value: Option<&serde_yaml::Value>,
) -> Result<Option<BTreeSet<Surface>>, SkillArchiveAuditError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .as_str()
        .ok_or(SkillArchiveAuditError::InvalidSurfaces)?;
    let mut result = BTreeSet::new();
    for part in value.split(',') {
        let surface = match part.trim() {
            "desktop" => Surface::Desktop,
            "web" => Surface::Web,
            "mobile" => Surface::Mobile,
            "api" => Surface::Api,
            "channel" => Surface::Channel,
            "studio" => Surface::Studio,
            _ => return Err(SkillArchiveAuditError::InvalidSurfaces),
        };
        if !result.insert(surface) {
            return Err(SkillArchiveAuditError::InvalidSurfaces);
        }
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_surface_ceiling_is_not_a_default_or_parent_union() {
        let value = serde_yaml::Value::String("desktop".into());
        assert_eq!(
            parse(Some(&value)).expect("valid test fixture"),
            Some(BTreeSet::from([Surface::Desktop]))
        );
        assert_eq!(parse(None).expect("valid test fixture"), None);
        let complete = serde_yaml::Value::String("web,api,desktop,channel,mobile,studio".into());
        assert_eq!(
            parse(Some(&complete))
                .expect("valid test fixture")
                .expect("valid test fixture"),
            BTreeSet::from([
                Surface::Web,
                Surface::Api,
                Surface::Desktop,
                Surface::Channel,
                Surface::Mobile,
                Surface::Studio,
            ])
        );
        for value in ["", "desktop,desktop", "desktop,web,other", "all"] {
            assert!(parse(Some(&serde_yaml::Value::String(value.into()))).is_err());
        }
        assert!(parse(Some(&serde_yaml::Value::Sequence(vec![]))).is_err());
    }
}
