//! Datalog rule support for user-defined reasoning rules
//!
//! This module provides structures for representing and executing user-defined
//! datalog rules that use `where`/`insert` patterns. Rules are stored in the
//! database using the `f:rule` predicate (https://ns.flur.ee/db#rule).
//!
//! ## Rule Format
//!
//! Rules are JSON objects with the following structure:
//! ```json
//! {
//!   "@context": {"ex": "http://example.org/"},
//!   "where": {"@id": "?person", "ex:parents": {"ex:parents": "?grandparent"}},
//!   "insert": {"@id": "?person", "ex:grandparent": "?grandparent"}
//! }
//! ```
//!
//! The `where` clause matches patterns in the database, binding variables.
//! The `insert` clause uses those bindings to generate new facts.
//!
//! ## Execution
//!
//! Rules are executed in a fixpoint loop alongside OWL2-RL rules:
//! 1. For each rule, find all bindings that match the `where` patterns
//! 2. For each binding, instantiate the `insert` templates to generate flakes
//! 3. Repeat until no new facts are generated

use fluree_db_core::Sid;
use fluree_vocab::jsonld_names::ID as JSONLD_ID;
use fluree_vocab::namespaces::{JSON_LD, XSD};
use fluree_vocab::xsd_names;
use std::collections::HashMap;
use std::sync::Arc;

/// A single triple pattern in a rule's where or insert clause
#[derive(Debug, Clone)]
pub struct RuleTriplePattern {
    /// Subject: variable name (e.g., "?person") or constant SID
    pub subject: RuleTerm,
    /// Predicate: variable name or constant SID
    pub predicate: RuleTerm,
    /// Object: variable name, constant SID, or literal value
    pub object: RuleTerm,
}

/// A term in a rule pattern (subject, predicate, or object position)
#[derive(Debug, Clone)]
pub enum RuleTerm {
    /// Variable binding (e.g., "?person")
    Var(Arc<str>),
    /// Constant IRI (resolved to SID)
    Sid(Sid),
    /// Literal value
    Value(RuleValue),
}

impl RuleTerm {
    /// Create a variable term
    pub fn var(name: &str) -> Self {
        RuleTerm::Var(Arc::from(name))
    }

    /// Create a SID term
    pub fn sid(sid: Sid) -> Self {
        RuleTerm::Sid(sid)
    }

    /// Check if this term is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, RuleTerm::Var(_))
    }

    /// Get the variable name if this is a variable
    pub fn var_name(&self) -> Option<&str> {
        match self {
            RuleTerm::Var(name) => Some(name.as_ref()),
            _ => None,
        }
    }
}

/// A literal value in a rule
#[derive(Debug, Clone)]
pub enum RuleValue {
    String(String),
    Long(i64),
    Double(f64),
    Boolean(bool),
    Ref(Sid),
}

/// A filter expression in a rule's where clause
#[derive(Debug, Clone)]
pub enum RuleFilter {
    /// Comparison: (op, var, value) e.g., (>=, ?age, 18)
    Compare {
        op: CompareOp,
        left: RuleTerm,
        right: RuleTerm,
    },
    /// Boolean AND of multiple conditions
    And(Vec<RuleFilter>),
    /// Boolean OR of multiple conditions
    Or(Vec<RuleFilter>),
    /// Boolean NOT
    Not(Box<RuleFilter>),
}

/// Comparison operators for filter expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A parsed datalog rule ready for execution
#[derive(Debug, Clone)]
pub struct DatalogRule {
    /// Rule identifier (the @id of the rule entity)
    pub id: Sid,
    /// Human-readable name (if available)
    pub name: Option<String>,
    /// Triple patterns in the where clause (body)
    pub where_patterns: Vec<RuleTriplePattern>,
    /// Filter expressions in the where clause
    pub filters: Vec<RuleFilter>,
    /// Triple patterns in the insert clause (head)
    pub insert_patterns: Vec<RuleTriplePattern>,
    /// Predicates this rule depends on (from where patterns)
    pub depends_on: Vec<Sid>,
    /// Predicates this rule generates (from insert patterns)
    pub generates: Vec<Sid>,
}

impl DatalogRule {
    /// Create a new datalog rule
    pub fn new(
        id: Sid,
        where_patterns: Vec<RuleTriplePattern>,
        insert_patterns: Vec<RuleTriplePattern>,
    ) -> Self {
        // Extract dependencies and generations
        let depends_on: Vec<Sid> = where_patterns
            .iter()
            .filter_map(|p| match &p.predicate {
                RuleTerm::Sid(sid) => Some(sid.clone()),
                _ => None,
            })
            .collect();

        let generates: Vec<Sid> = insert_patterns
            .iter()
            .filter_map(|p| match &p.predicate {
                RuleTerm::Sid(sid) => Some(sid.clone()),
                _ => None,
            })
            .collect();

        Self {
            id,
            name: None,
            where_patterns,
            filters: Vec::new(),
            insert_patterns,
            depends_on,
            generates,
        }
    }

    /// Add filters to the rule
    pub fn with_filters(mut self, filters: Vec<RuleFilter>) -> Self {
        self.filters = filters;
        self
    }

    /// Set the rule name
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Get all variable names used in this rule
    pub fn variables(&self) -> Vec<Arc<str>> {
        let mut vars: Vec<Arc<str>> = Vec::new();

        for pattern in &self.where_patterns {
            if let RuleTerm::Var(v) = &pattern.subject {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
            if let RuleTerm::Var(v) = &pattern.predicate {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
            if let RuleTerm::Var(v) = &pattern.object {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
        }

        for pattern in &self.insert_patterns {
            if let RuleTerm::Var(v) = &pattern.subject {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
            if let RuleTerm::Var(v) = &pattern.predicate {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
            if let RuleTerm::Var(v) = &pattern.object {
                if !vars.contains(v) {
                    vars.push(v.clone());
                }
            }
        }

        vars
    }
}

/// A set of datalog rules with dependency information
#[derive(Debug, Default)]
pub struct DatalogRuleSet {
    /// Rules indexed by ID
    rules: HashMap<Sid, DatalogRule>,
    /// Execution order (topologically sorted by dependencies)
    execution_order: Vec<Sid>,
}

impl DatalogRuleSet {
    /// Create an empty rule set
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule to the set
    pub fn add_rule(&mut self, rule: DatalogRule) {
        self.rules.insert(rule.id.clone(), rule);
        // Recompute execution order
        self.compute_execution_order();
    }

    /// Get a rule by ID
    pub fn get(&self, id: &Sid) -> Option<&DatalogRule> {
        self.rules.get(id)
    }

    /// Iterate over rules in execution order
    pub fn iter_in_order(&self) -> impl Iterator<Item = &DatalogRule> {
        self.execution_order
            .iter()
            .filter_map(|id| self.rules.get(id))
    }

    /// Get all rules
    pub fn rules(&self) -> impl Iterator<Item = &DatalogRule> {
        self.rules.values()
    }

    /// Check if the rule set is empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Get the number of rules
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Compute execution order based on dependencies
    fn compute_execution_order(&mut self) {
        // Simple topological sort: rules that don't depend on other rules' outputs go first
        // For now, just use insertion order (can be improved later)
        self.execution_order = self.rules.keys().cloned().collect();

        // Sort by number of dependencies (fewer deps first)
        self.execution_order
            .sort_by_key(|id| self.rules.get(id).map(|r| r.depends_on.len()).unwrap_or(0));
    }

    /// Get all predicates that rules depend on (for initial delta seeding)
    pub fn dependent_predicates(&self) -> Vec<Sid> {
        let mut predicates: Vec<Sid> = Vec::new();
        for rule in self.rules.values() {
            for pred in &rule.depends_on {
                if !predicates.contains(pred) {
                    predicates.push(pred.clone());
                }
            }
        }
        predicates
    }
}

/// A binding from variable names to values
pub type Bindings = HashMap<Arc<str>, BindingValue>;

/// A value bound to a variable during rule execution
#[derive(Debug, Clone)]
pub enum BindingValue {
    /// Reference to another entity
    Sid(Sid),
    /// Literal string
    String(String),
    /// Literal integer
    Long(i64),
    /// Literal floating point
    Double(f64),
    /// Literal boolean
    Boolean(bool),
}

impl BindingValue {
    /// Convert to a FlakeValue for use in flake construction
    pub fn to_flake_value(&self) -> fluree_db_core::value::FlakeValue {
        use fluree_db_core::value::FlakeValue;
        match self {
            BindingValue::Sid(sid) => FlakeValue::Ref(sid.clone()),
            BindingValue::String(s) => FlakeValue::String(s.clone()),
            BindingValue::Long(n) => FlakeValue::Long(*n),
            BindingValue::Double(d) => FlakeValue::Double(*d),
            BindingValue::Boolean(b) => FlakeValue::Boolean(*b),
        }
    }
}

/// Instantiate an insert pattern with bindings to create a flake
///
/// Returns None if any required variable is missing from bindings
pub fn instantiate_pattern(
    pattern: &RuleTriplePattern,
    bindings: &Bindings,
    t: i64,
) -> Option<fluree_db_core::flake::Flake> {
    use fluree_db_core::flake::Flake;
    use fluree_db_core::value::FlakeValue;

    // Resolve subject
    let subject = match &pattern.subject {
        RuleTerm::Var(name) => {
            let binding = bindings.get(name.as_ref())?;
            match binding {
                BindingValue::Sid(sid) => sid.clone(),
                _ => return None, // Subject must be a SID
            }
        }
        RuleTerm::Sid(sid) => sid.clone(),
        RuleTerm::Value(_) => return None, // Subject can't be a literal
    };

    // Resolve predicate
    let predicate = match &pattern.predicate {
        RuleTerm::Var(name) => {
            let binding = bindings.get(name.as_ref())?;
            match binding {
                BindingValue::Sid(sid) => sid.clone(),
                _ => return None, // Predicate must be a SID
            }
        }
        RuleTerm::Sid(sid) => sid.clone(),
        RuleTerm::Value(_) => return None, // Predicate can't be a literal
    };

    // Resolve object
    let object = match &pattern.object {
        RuleTerm::Var(name) => {
            let binding = bindings.get(name.as_ref())?;
            binding.to_flake_value()
        }
        RuleTerm::Sid(sid) => FlakeValue::Ref(sid.clone()),
        RuleTerm::Value(val) => match val {
            RuleValue::String(s) => FlakeValue::String(s.clone()),
            RuleValue::Long(n) => FlakeValue::Long(*n),
            RuleValue::Double(d) => FlakeValue::Double(*d),
            RuleValue::Boolean(b) => FlakeValue::Boolean(*b),
            RuleValue::Ref(sid) => FlakeValue::Ref(sid.clone()),
        },
    };

    // Determine datatype from object
    // For Ref objects, use $id marker (JSON_LD namespace, "id" local name)
    let datatype = match &object {
        FlakeValue::Ref(_) => Sid::new(JSON_LD, JSONLD_ID), // $id marker for references
        FlakeValue::String(_) => Sid::new(XSD, xsd_names::STRING),
        FlakeValue::Long(_) => Sid::new(XSD, xsd_names::LONG),
        FlakeValue::Double(_) => Sid::new(XSD, xsd_names::DOUBLE),
        FlakeValue::Boolean(_) => Sid::new(XSD, xsd_names::BOOLEAN),
        _ => Sid::new(XSD, xsd_names::STRING), // Default
    };

    Some(Flake::new(
        subject, predicate, object, datatype, t, true, None,
    ))
}

/// Execute a single rule and generate new flakes
///
/// This function:
/// 1. Takes a rule and a set of bindings (from pattern matching)
/// 2. For each binding row, instantiates the insert patterns to create flakes
/// 3. Returns the generated flakes (filtered for uniqueness)
pub fn execute_rule_with_bindings(
    rule: &DatalogRule,
    binding_rows: Vec<Bindings>,
    t: i64,
) -> Vec<fluree_db_core::flake::Flake> {
    let mut flakes = Vec::new();

    for bindings in binding_rows {
        for pattern in &rule.insert_patterns {
            if let Some(flake) = instantiate_pattern(pattern, &bindings, t) {
                // Check for duplicates
                if !flakes.iter().any(|f: &fluree_db_core::flake::Flake| {
                    f.s == flake.s
                        && f.p == flake.p
                        && f.o == flake.o
                        && f.dt == flake.dt
                        && f.m == flake.m
                }) {
                    flakes.push(flake);
                }
            }
        }
    }

    flakes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_term_var() {
        let term = RuleTerm::var("?person");
        assert!(term.is_var());
        assert_eq!(term.var_name(), Some("?person"));
    }

    #[test]
    fn test_rule_term_sid() {
        let sid = Sid::new(100, "Person");
        let term = RuleTerm::sid(sid.clone());
        assert!(!term.is_var());
        assert_eq!(term.var_name(), None);
    }

    #[test]
    fn test_datalog_rule_variables() {
        let rule = DatalogRule::new(
            Sid::new(1, "rule1"),
            vec![RuleTriplePattern {
                subject: RuleTerm::var("?person"),
                predicate: RuleTerm::sid(Sid::new(100, "type")),
                object: RuleTerm::var("?type"),
            }],
            vec![RuleTriplePattern {
                subject: RuleTerm::var("?person"),
                predicate: RuleTerm::sid(Sid::new(100, "derived")),
                object: RuleTerm::var("?type"),
            }],
        );

        let vars = rule.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.iter().any(|v| v.as_ref() == "?person"));
        assert!(vars.iter().any(|v| v.as_ref() == "?type"));
    }

    #[test]
    fn test_datalog_rule_set() {
        let mut rule_set = DatalogRuleSet::new();
        assert!(rule_set.is_empty());

        let rule = DatalogRule::new(Sid::new(1, "rule1"), vec![], vec![]);
        rule_set.add_rule(rule);

        assert_eq!(rule_set.len(), 1);
        assert!(!rule_set.is_empty());
    }
}
