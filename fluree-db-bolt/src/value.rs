//! The PackStream value model.
//!
//! Maps are represented as `Vec<(String, Value)>` rather than a hash map:
//! Bolt maps are small (message metadata, node properties), insertion order
//! makes encoding deterministic for byte-fixture tests, and linear lookup is
//! cheaper than hashing at these sizes.

/// A PackStream value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(std::sync::Arc<str>),
    List(Vec<Value>),
    Map(MapValue),
    /// A tagged structure (nodes, relationships, dates, ...). Bolt limits
    /// structures to at most 15 fields.
    Structure(Structure),
}

/// An insertion-ordered string-keyed map.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MapValue(pub Vec<(std::sync::Arc<str>, Value)>);

/// A PackStream structure: signature byte plus fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Structure {
    pub signature: u8,
    pub fields: Vec<Value>,
}

impl MapValue {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Insert or replace a key.
    pub fn insert(&mut self, key: impl Into<std::sync::Arc<str>>, value: impl Into<Value>) {
        let key = key.into();
        if let Some(slot) = self.0.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value.into();
        } else {
            self.0.push((key, value.into()));
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.get(key) {
            Some(Value::String(s)) => Some(s),
            _ => None,
        }
    }

    pub fn get_int(&self, key: &str) -> Option<i64> {
        match self.get(key) {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl FromIterator<(std::sync::Arc<str>, Value)> for MapValue {
    fn from_iter<T: IntoIterator<Item = (std::sync::Arc<str>, Value)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl FromIterator<(String, Value)> for MapValue {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }
}

impl Value {
    /// Convenience constructor for an empty map.
    pub fn empty_map() -> Self {
        Value::Map(MapValue::new())
    }

    pub fn as_map(&self) -> Option<&MapValue> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.into())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v.into())
    }
}

impl From<std::sync::Arc<str>> for Value {
    fn from(v: std::sync::Arc<str>) -> Self {
        Value::String(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::List(v)
    }
}

impl From<MapValue> for Value {
    fn from(v: MapValue) -> Self {
        Value::Map(v)
    }
}

/// Structure signatures for the graph and temporal types Bolt results carry.
///
/// Node/Relationship/Path gained an `element_id` string field in Bolt 5.0;
/// the pre-5.0 shapes have one fewer field. DateTime structures changed in
/// 5.0 (and via the 4.4 `utc` patch, which we do not negotiate): the legacy
/// forms carry a local-seconds offset baked into the epoch value, the modern
/// forms are UTC epoch + offset.
pub mod sig {
    pub const NODE: u8 = 0x4E; // 'N'
    pub const RELATIONSHIP: u8 = 0x52; // 'R'
    pub const UNBOUND_RELATIONSHIP: u8 = 0x72; // 'r'
    pub const PATH: u8 = 0x50; // 'P'
    pub const DATE: u8 = 0x44; // 'D'  (days since epoch)
    pub const TIME: u8 = 0x54; // 'T'  (nanos, tz offset seconds)
    pub const LOCAL_TIME: u8 = 0x74; // 't'
    pub const DATE_TIME: u8 = 0x49; // 'I'  (5.0+: utc seconds, nanos, offset)
    pub const DATE_TIME_ZONE_ID: u8 = 0x69; // 'i'  (5.0+)
    pub const LOCAL_DATE_TIME: u8 = 0x64; // 'd'
    pub const DATE_TIME_LEGACY: u8 = 0x46; // 'F'  (<5.0: local seconds, nanos, offset)
    pub const DATE_TIME_ZONE_ID_LEGACY: u8 = 0x66; // 'f'
    pub const DURATION: u8 = 0x45; // 'E'
    pub const POINT_2D: u8 = 0x58; // 'X'
    pub const POINT_3D: u8 = 0x59; // 'Y'
}
