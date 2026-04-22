//! Policy index building
//!
//! This module provides functions to build O(1) indexed policy sets from
//! a list of policy restrictions.
//!
//! # Key Design Decisions
//!
//! - Class policies are indexed INTO `by_property`, not a separate index
//! - Implicit properties (@id, rdf:type) are always included for class policies
//! - Insertion order is preserved within each bucket
//! - Each policy is indexed once per property (no duplicates)

use crate::types::{PolicyAction, PolicyRestriction, PolicySet, PropertyPolicyEntry, TargetMode};
use fluree_db_core::IndexStats;
use fluree_db_core::Sid;
use fluree_vocab::namespaces::{JSON_LD, RDF};
use std::collections::HashSet;

/// Get the @id property SID
fn id_property() -> Sid {
    Sid::new(JSON_LD, "id")
}

/// Get the rdf:type property SID
fn rdf_type_property() -> Sid {
    Sid::new(RDF, "type")
}

/// Check if a property is implicit (@id or rdf:type)
fn is_implicit_property(sid: &Sid) -> bool {
    (sid.namespace_code == JSON_LD && sid.name.as_ref() == "id")
        || (sid.namespace_code == RDF && sid.name.as_ref() == "type")
}

/// Build indexed policy set from restrictions.
///
/// # Arguments
///
/// * `restrictions` - List of policy restrictions in parse order
/// * `stats` - Database stats for class→property mapping (optional)
/// * `action_filter` - Filter to only include policies matching this action (View or Modify)
///
/// # Returns
///
/// A `PolicySet` with:
/// - `restrictions` - All matching restrictions in parse order
/// - `by_subject` - Index from subject SID to restriction indices
/// - `by_property` - Index from property SID to restriction indices (includes class policies!)
/// - `defaults` - Default-bucket policy indices
pub fn build_policy_set(
    restrictions: Vec<PolicyRestriction>,
    stats: Option<&IndexStats>,
    action_filter: PolicyAction,
) -> PolicySet {
    let mut set = PolicySet::new();

    for restriction in restrictions {
        // Filter by action
        match (&restriction.action, &action_filter) {
            (PolicyAction::Both, _) => {}                      // Matches any filter
            (PolicyAction::View, PolicyAction::View) => {}     // View matches view
            (PolicyAction::Modify, PolicyAction::Modify) => {} // Modify matches modify
            _ => continue,                                     // Skip non-matching
        }

        let idx = set.restrictions.len();
        set.restrictions.push(restriction);
        let restriction = &set.restrictions[idx];

        match restriction.target_mode {
            TargetMode::OnSubject => {
                // Index by each target subject
                for subject_sid in &restriction.targets {
                    set.by_subject
                        .entry(subject_sid.clone())
                        .or_default()
                        .push(idx);
                }
            }
            TargetMode::OnProperty => {
                // Index by each target property
                // If the policy also has class restrictions (OnProperty + OnClass combined),
                // we need to check the subject's class during evaluation.
                let class_check_needed = restriction.class_policy;
                for property_sid in &restriction.targets {
                    set.by_property
                        .entry(property_sid.clone())
                        .or_default()
                        .push(PropertyPolicyEntry {
                            idx,
                            class_check_needed,
                        });
                }
            }
            TargetMode::OnClass => {
                // Class policies are indexed INTO by_property
                // Collect all properties for this restriction (union across classes + implicit)
                let mut props_for_restriction: HashSet<Sid> = HashSet::new();

                // Add properties from all target classes via stats
                if let Some(db_stats) = stats {
                    for class_sid in &restriction.for_classes {
                        let class_props = get_properties_for_class(class_sid, db_stats);
                        props_for_restriction.extend(class_props);
                    }
                }

                // ALWAYS include implicit properties @id and rdf:type
                props_for_restriction.insert(id_property());
                props_for_restriction.insert(rdf_type_property());

                // Index this restriction ONCE per property with per-property class_check_needed
                for prop in props_for_restriction {
                    // Compute class_check_needed for THIS property:
                    // - Implicit properties (@id, rdf:type) always need check
                    // - If all classes using this property are in for_classes, no check needed
                    // - Otherwise, check needed
                    let class_check_needed =
                        compute_class_check_needed(&restriction.for_classes, &prop, stats);

                    set.by_property
                        .entry(prop)
                        .or_default()
                        .push(PropertyPolicyEntry {
                            idx,
                            class_check_needed,
                        });
                }
            }
            TargetMode::Default => {
                // Default-bucket policies
                set.defaults.push(idx);
            }
        }
    }

    set
}

/// Get all properties used by instances of a class from stats.
///
/// Returns an empty vec if the class is not found or stats are unavailable.
fn get_properties_for_class(class_sid: &Sid, stats: &IndexStats) -> Vec<Sid> {
    let Some(ref classes) = stats.classes else {
        return vec![];
    };

    for class_entry in classes {
        if &class_entry.class_sid == class_sid {
            return class_entry
                .properties
                .iter()
                .map(|p| p.property_sid.clone())
                .collect();
        }
    }

    vec![]
}

/// Get all classes that use a given property from stats.
///
/// Returns an empty HashSet if the property is not found or stats are unavailable.
pub fn get_all_classes_for_property(property_sid: &Sid, stats: &IndexStats) -> HashSet<Sid> {
    let Some(ref classes) = stats.classes else {
        return HashSet::new();
    };

    let mut result = HashSet::new();
    for class_entry in classes {
        for prop_usage in &class_entry.properties {
            if &prop_usage.property_sid == property_sid {
                result.insert(class_entry.class_sid.clone());
                break;
            }
        }
    }

    result
}

/// Determine if runtime class check is needed for a class policy property.
///
/// Class check needed iff:
/// 1. Property is implicit (@id or rdf:type), OR
/// 2. Some class OUTSIDE the policy's target classes also uses this property
///
/// This means: check needed = implicit || !all_classes.is_subset(for_classes)
pub fn compute_class_check_needed(
    for_classes: &HashSet<Sid>,
    property_sid: &Sid,
    stats: Option<&IndexStats>,
) -> bool {
    // Implicit properties always need class check (shared across all classes)
    if is_implicit_property(property_sid) {
        return true;
    }

    let Some(db_stats) = stats else {
        // Without stats, we must assume check is needed
        return true;
    };

    // Get all classes that use this property from stats
    let all_classes = get_all_classes_for_property(property_sid, db_stats);

    // Check if all_classes is a subset of for_classes
    // If NOT a subset, some class outside our targets uses this property → need check
    !all_classes.is_subset(for_classes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PolicyValue;
    use fluree_db_core::{ClassPropertyUsage, ClassStatEntry};

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn make_prop_restriction(id: &str, property: Sid) -> PolicyRestriction {
        PolicyRestriction {
            id: id.to_string(),
            target_mode: TargetMode::OnProperty,
            targets: [property].into_iter().collect(),
            action: PolicyAction::View,
            value: PolicyValue::Allow,
            required: false,
            message: None,
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        }
    }

    fn make_class_restriction(id: &str, class: Sid) -> PolicyRestriction {
        PolicyRestriction {
            id: id.to_string(),
            target_mode: TargetMode::OnClass,
            targets: HashSet::new(),
            action: PolicyAction::View,
            value: PolicyValue::Allow,
            required: false,
            message: None,
            class_policy: true,
            for_classes: [class].into_iter().collect(),
            class_check_needed: true, // Will be computed
        }
    }

    fn make_default_restriction(id: &str) -> PolicyRestriction {
        PolicyRestriction {
            id: id.to_string(),
            target_mode: TargetMode::Default,
            targets: HashSet::new(),
            action: PolicyAction::Both,
            value: PolicyValue::Deny,
            required: false,
            message: None,
            class_policy: false,
            for_classes: HashSet::new(),
            class_check_needed: false,
        }
    }

    fn make_stats_with_class(class_sid: Sid, property_sids: Vec<Sid>) -> IndexStats {
        IndexStats {
            flakes: 100,
            size: 5000,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid,
                count: 10,
                properties: property_sids
                    .into_iter()
                    .map(|p| ClassPropertyUsage {
                        property_sid: p,
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: Vec::new(),
                    })
                    .collect(),
            }]),
            graphs: None,
        }
    }

    #[test]
    fn test_build_policy_set_property_index() {
        let restrictions = vec![
            make_prop_restriction("p1", make_sid(100, "name")),
            make_prop_restriction("p2", make_sid(100, "age")),
        ];

        let set = build_policy_set(restrictions, None, PolicyAction::View);

        assert_eq!(set.restrictions.len(), 2);
        assert_eq!(
            set.by_property.get(&make_sid(100, "name")).unwrap().len(),
            1
        );
        assert_eq!(set.by_property.get(&make_sid(100, "age")).unwrap().len(), 1);
    }

    #[test]
    fn test_build_policy_set_class_index() {
        let person_class = make_sid(100, "Person");
        let name_prop = make_sid(100, "name");
        let age_prop = make_sid(100, "age");

        let stats = make_stats_with_class(
            person_class.clone(),
            vec![name_prop.clone(), age_prop.clone()],
        );

        let restrictions = vec![make_class_restriction("c1", person_class)];

        let set = build_policy_set(restrictions, Some(&stats), PolicyAction::View);

        assert_eq!(set.restrictions.len(), 1);

        // Should be indexed for name, age, @id, and rdf:type
        assert!(set.by_property.contains_key(&name_prop));
        assert!(set.by_property.contains_key(&age_prop));
        assert!(set.by_property.contains_key(&id_property()));
        assert!(set.by_property.contains_key(&rdf_type_property()));
    }

    #[test]
    fn test_build_policy_set_defaults() {
        let restrictions = vec![
            make_prop_restriction("p1", make_sid(100, "name")),
            make_default_restriction("d1"),
        ];

        let set = build_policy_set(restrictions, None, PolicyAction::View);

        assert_eq!(set.restrictions.len(), 2);
        assert_eq!(set.defaults.len(), 1);
        assert_eq!(set.defaults[0], 1); // Index of the default restriction
    }

    #[test]
    fn test_action_filter() {
        let mut view_restriction = make_prop_restriction("v1", make_sid(100, "name"));
        view_restriction.action = PolicyAction::View;

        let mut modify_restriction = make_prop_restriction("m1", make_sid(100, "age"));
        modify_restriction.action = PolicyAction::Modify;

        let restrictions = vec![view_restriction, modify_restriction];

        // Filter for View only
        let view_set = build_policy_set(restrictions.clone(), None, PolicyAction::View);
        assert_eq!(view_set.restrictions.len(), 1);
        assert_eq!(view_set.restrictions[0].id, "v1");

        // Filter for Modify only
        let modify_set = build_policy_set(restrictions, None, PolicyAction::Modify);
        assert_eq!(modify_set.restrictions.len(), 1);
        assert_eq!(modify_set.restrictions[0].id, "m1");
    }

    #[test]
    fn test_class_check_needed_implicit() {
        let for_classes: HashSet<Sid> = [make_sid(100, "Person")].into_iter().collect();

        // @id is implicit, always needs check
        assert!(compute_class_check_needed(
            &for_classes,
            &id_property(),
            None
        ));

        // rdf:type is implicit, always needs check
        assert!(compute_class_check_needed(
            &for_classes,
            &rdf_type_property(),
            None
        ));
    }

    #[test]
    fn test_class_check_needed_exclusive_property() {
        let person = make_sid(100, "Person");
        let company = make_sid(100, "Company");
        let name_prop = make_sid(100, "name");
        let ssn_prop = make_sid(100, "ssn");

        // Stats: Person uses name, ssn; Company uses name only
        let stats = IndexStats {
            flakes: 100,
            size: 5000,
            properties: None,
            classes: Some(vec![
                ClassStatEntry {
                    class_sid: person.clone(),
                    count: 10,
                    properties: vec![
                        ClassPropertyUsage {
                            property_sid: name_prop.clone(),
                            datatypes: Vec::new(),
                            langs: Vec::new(),
                            ref_classes: Vec::new(),
                        },
                        ClassPropertyUsage {
                            property_sid: ssn_prop.clone(),
                            datatypes: Vec::new(),
                            langs: Vec::new(),
                            ref_classes: Vec::new(),
                        },
                    ],
                },
                ClassStatEntry {
                    class_sid: company.clone(),
                    count: 5,
                    properties: vec![ClassPropertyUsage {
                        property_sid: name_prop.clone(),
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: Vec::new(),
                    }],
                },
            ]),
            graphs: None,
        };

        let person_only: HashSet<Sid> = [person.clone()].into_iter().collect();

        // name is used by both Person and Company, needs check
        assert!(compute_class_check_needed(
            &person_only,
            &name_prop,
            Some(&stats)
        ));

        // ssn is only used by Person, no check needed
        assert!(!compute_class_check_needed(
            &person_only,
            &ssn_prop,
            Some(&stats)
        ));
    }
}
