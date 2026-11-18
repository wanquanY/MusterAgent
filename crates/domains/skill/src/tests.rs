use musteragent_identity::ProductId;

use musteragent_identity::VersionedKey;
use musteragent_sandbox::SandboxCompatibility;

use super::{
    ActivationPolicy, ProductSkillRelease, SkillLifecycle, Surface, builtin_skill_catalog,
    builtin_skill_specs,
};

#[test]
fn surfaces_keep_their_persisted_discriminants() {
    for (surface, code, name) in [
        (Surface::Web, 1, "web"),
        (Surface::Studio, 2, "studio"),
        (Surface::Api, 3, "api"),
        (Surface::Desktop, 4, "desktop"),
        (Surface::Channel, 5, "channel"),
        (Surface::Mobile, 6, "mobile"),
    ] {
        assert_eq!(surface.canonical_code(), code);
        assert_eq!(surface.as_str(), name);
    }
}

const GENERIC_BASE_SKILLS: [&str; 7] = [
    "assistant-base@1",
    "project-files@1",
    "data-analysis@1",
    "web-research@1",
    "vision-analysis@1",
    "workspace-reading@1",
    "skill-creator@1",
];

#[test]
fn the_kernel_compiles_exactly_the_generic_base_skills() {
    let specs = builtin_skill_specs();
    assert_eq!(
        specs.iter().map(|spec| spec.key).collect::<Vec<_>>(),
        GENERIC_BASE_SKILLS
    );
    assert!(
        specs
            .iter()
            .all(|spec| spec.lifecycle == SkillLifecycle::Active)
    );
    let catalog = builtin_skill_catalog().expect("kernel Skill catalog");
    assert_eq!(catalog.definitions.len(), GENERIC_BASE_SKILLS.len());
    for definition in catalog.definitions.values() {
        let expected = if definition.skill_ref.as_str() == "assistant-base@1" {
            ActivationPolicy::Base
        } else {
            ActivationPolicy::ExplicitOrResolved
        };
        assert_eq!(definition.activation_policy, expected);
    }
}

#[test]
fn kernel_skills_carry_no_product_branding() {
    for spec in builtin_skill_specs() {
        let text = [
            spec.title,
            spec.description,
            spec.when_to_use,
            spec.instructions,
        ]
        .concat()
        .to_ascii_lowercase();
        // Synthetic host brands exercise the boundary between kernel and host content.
        for brand in [
            "example-host-suite",
            "sample-host-assistant",
            "fixture-host-studio",
        ] {
            assert!(!text.contains(brand), "{} mentions {brand}", spec.key);
        }
    }
}

#[test]
fn kernel_skills_declare_sandbox_compatibility() {
    let release = ProductSkillRelease::for_product(ProductId::KERNEL);
    for key in GENERIC_BASE_SKILLS {
        let expected =
            (key == "data-analysis@1").then_some(SandboxCompatibility::BrowserCompatible);
        assert_eq!(
            release.sandbox_compatibility(&VersionedKey::new(key).expect("Skill ref")),
            expected,
            "{key}"
        );
    }
}

#[test]
fn the_kernel_release_is_exact_and_forgery_is_refused() {
    let release = ProductSkillRelease::for_product(ProductId::KERNEL);
    assert!(release.catalog().is_ok());
    let reference = release.reference();
    assert!(ProductSkillRelease::resolve(&reference).is_ok());
    let mut forged = reference;
    forged.digest[0] ^= 1;
    assert!(ProductSkillRelease::resolve(&forged).is_err());
}
