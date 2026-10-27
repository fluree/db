//! Why a value is not a `T`, said in `T`'s terms alone.
//!
//! serde's own messages quote what they could not parse (`invalid type:
//! string "…"`, ``unknown variant `…` ``), and a graph source's input is where
//! a credential given in the wrong place turns up. So [`strict`] parses with
//! serde_json as always, and when that refuses, the same JSON is read again
//! through [`Input`], a deserializer whose error type, [`Refusal`], keeps only
//! what the type itself says:
//!
//! - where the read stopped: the keys and indexes down to the value;
//! - what kind of value was there (a string, an object), never the value;
//! - what the type expected, the fields it is missing, the variants it knows.
//!
//! A hand-written deserializer's free-text message (`custom`) can hold
//! anything, so only messages known to carry nothing of the input are kept;
//! any other becomes "not a valid value here".
//!
//! [`strict`]: super::strict

use std::fmt;

use fluree_db_iceberg::auth::google::EXTERNAL_ACCOUNT_CONFIG_FORMS;
use serde::de::{self, DeserializeOwned, DeserializeSeed, IntoDeserializer, Unexpected, Visitor};
use serde_json::Value;

/// The free-text messages a [`Refusal`] repeats: each names the forms a type
/// accepts and nothing of the value.
const KNOWN_MESSAGES: &[&str] = &[EXTERNAL_ACCOUNT_CONFIG_FORMS];

/// The message serde gives when no variant of an untagged enum matches,
/// followed by the enum's name.
const UNTAGGED_PREFIX: &str = "data did not match any variant of untagged enum ";

/// Where in `value` it stops being a `T`, and why. `None` when this reading
/// accepts what serde_json refused, so there is nothing to say in `T`'s terms.
pub(super) fn why_not<T: DeserializeOwned>(value: &Value) -> Option<Refusal> {
    T::deserialize(Input(value)).err()
}

/// Why a value is not a `T`: built from the type's own terms, so it can never
/// hold any part of the value.
#[derive(Debug)]
pub(super) struct Refusal {
    /// The keys and indexes from the value read down to the one refused.
    pub(super) path: Vec<String>,
    reason: Reason,
}

#[derive(Debug)]
enum Reason {
    WrongType {
        found: &'static str,
        expected: String,
    },
    BadValue {
        found: &'static str,
        expected: String,
    },
    Length {
        expected: String,
    },
    UnknownVariant(&'static [&'static str]),
    UnknownField(&'static [&'static str]),
    Missing(&'static str),
    Duplicate(&'static str),
    /// A free-text message known to carry nothing of the input.
    Known(String),
    Other,
}

impl Refusal {
    fn new(reason: Reason) -> Self {
        Self {
            path: Vec::new(),
            reason,
        }
    }

    /// The same refusal, one key or index further out.
    fn within(mut self, segment: &str) -> Self {
        self.path.insert(0, segment.to_string());
        self
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            Reason::WrongType { found, expected } => {
                write!(f, "invalid type: {found}, expected {expected}")
            }
            Reason::BadValue { found, expected } => {
                write!(f, "invalid value: {found}, expected {expected}")
            }
            Reason::Length { expected } => write!(f, "invalid length, expected {expected}"),
            Reason::UnknownVariant([]) => f.write_str("unknown variant: there are none"),
            Reason::UnknownVariant(names) => {
                write!(f, "unknown variant, expected {}", OneOf(names))
            }
            Reason::UnknownField([]) => f.write_str("unknown field: there are none"),
            Reason::UnknownField(names) => write!(f, "unknown field, expected {}", OneOf(names)),
            Reason::Missing(field) => write!(f, "missing field `{field}`"),
            Reason::Duplicate(field) => write!(f, "duplicate field `{field}`"),
            Reason::Known(message) => f.write_str(message),
            Reason::Other => f.write_str("not a valid value here"),
        }
    }
}

impl std::error::Error for Refusal {}

impl de::Error for Refusal {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        let message = msg.to_string();
        let known = KNOWN_MESSAGES.contains(&message.as_str())
            || message.strip_prefix(UNTAGGED_PREFIX).is_some_and(|name| {
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            });
        Self::new(if known {
            Reason::Known(message)
        } else {
            Reason::Other
        })
    }

    fn invalid_type(unexp: Unexpected<'_>, exp: &dyn de::Expected) -> Self {
        Self::new(Reason::WrongType {
            found: kind_of(&unexp),
            expected: exp.to_string(),
        })
    }

    fn invalid_value(unexp: Unexpected<'_>, exp: &dyn de::Expected) -> Self {
        Self::new(Reason::BadValue {
            found: kind_of(&unexp),
            expected: exp.to_string(),
        })
    }

    fn invalid_length(_len: usize, exp: &dyn de::Expected) -> Self {
        Self::new(Reason::Length {
            expected: exp.to_string(),
        })
    }

    fn unknown_variant(_variant: &str, expected: &'static [&'static str]) -> Self {
        Self::new(Reason::UnknownVariant(expected))
    }

    fn unknown_field(_field: &str, expected: &'static [&'static str]) -> Self {
        Self::new(Reason::UnknownField(expected))
    }

    fn missing_field(field: &'static str) -> Self {
        Self::new(Reason::Missing(field))
    }

    fn duplicate_field(field: &'static str) -> Self {
        Self::new(Reason::Duplicate(field))
    }
}

/// The kind of value serde found, without the value.
fn kind_of(unexp: &Unexpected<'_>) -> &'static str {
    match unexp {
        Unexpected::Bool(_) => "a boolean",
        Unexpected::Unsigned(_) | Unexpected::Signed(_) | Unexpected::Float(_) => "a number",
        Unexpected::Char(_) | Unexpected::Str(_) => "a string",
        Unexpected::Bytes(_) => "bytes",
        Unexpected::Unit => "null",
        Unexpected::Seq => "an array",
        Unexpected::Map => "an object",
        Unexpected::Option
        | Unexpected::NewtypeStruct
        | Unexpected::Enum
        | Unexpected::UnitVariant
        | Unexpected::NewtypeVariant
        | Unexpected::TupleVariant
        | Unexpected::StructVariant
        | Unexpected::Other(_) => "a value",
    }
}

/// `` `a` ``, `` `a` or `b` ``, or `` one of `a`, `b`, `c` ``, as serde
/// lists the names a type knows.
struct OneOf(&'static [&'static str]);

impl fmt::Display for OneOf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            [] => Ok(()),
            [one] => write!(f, "`{one}`"),
            [a, b] => write!(f, "`{a}` or `{b}`"),
            names => {
                f.write_str("one of ")?;
                for (i, name) in names.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "`{name}`")?;
                }
                Ok(())
            }
        }
    }
}

// =============================================================================
// The deserializer
// =============================================================================

/// A JSON value read the way serde_json reads one, with [`Refusal`] as the
/// error and each map key and array index recorded on the way down.
struct Input<'a>(&'a Value);

impl<'de> de::Deserializer<'de> for Input<'_> {
    type Error = Refusal;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Refusal> {
        match self.0 {
            Value::Null => visitor.visit_unit(),
            Value::Bool(b) => visitor.visit_bool(*b),
            Value::Number(n) => match (n.as_u64(), n.as_i64(), n.as_f64()) {
                (Some(u), _, _) => visitor.visit_u64(u),
                (None, Some(i), _) => visitor.visit_i64(i),
                (None, None, Some(f)) => visitor.visit_f64(f),
                (None, None, None) => Err(de::Error::custom("a number out of range")),
            },
            Value::String(s) => visitor.visit_str(s),
            Value::Array(items) => visitor.visit_seq(Items(items.iter().enumerate())),
            Value::Object(map) => visitor.visit_map(Entries {
                entries: map.iter(),
                pending: None,
            }),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Refusal> {
        match self.0 {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Refusal> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Refusal> {
        match self.0 {
            Value::String(variant) => visitor.visit_enum(name(variant)),
            Value::Object(map) => match (map.len(), map.iter().next()) {
                (1, Some((variant, value))) => visitor
                    .visit_enum(Variant { variant, value })
                    .map_err(|e| e.within(variant)),
                _ => Err(de::Error::invalid_type(Unexpected::Map, &ONE_VARIANT)),
            },
            other => Err(de::Error::invalid_type(kind_of_value(other), &ONE_VARIANT)),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}

/// What an externally tagged enum is given as.
const ONE_VARIANT: &str = "a variant name, or an object with one key";

/// A key or variant name, read as a string with [`Refusal`] as the error.
fn name(text: &str) -> de::value::StrDeserializer<'_, Refusal> {
    text.into_deserializer()
}

fn kind_of_value(value: &Value) -> Unexpected<'static> {
    match value {
        Value::Null => Unexpected::Unit,
        Value::Bool(_) => Unexpected::Bool(false),
        Value::Number(_) => Unexpected::Unsigned(0),
        Value::String(_) => Unexpected::Str(""),
        Value::Array(_) => Unexpected::Seq,
        Value::Object(_) => Unexpected::Map,
    }
}

struct Items<'a>(std::iter::Enumerate<std::slice::Iter<'a, Value>>);

impl<'de> de::SeqAccess<'de> for Items<'_> {
    type Error = Refusal;

    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Refusal> {
        match self.0.next() {
            Some((i, value)) => seed
                .deserialize(Input(value))
                .map(Some)
                .map_err(|e| e.within(&i.to_string())),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }
}

struct Entries<'a> {
    entries: serde_json::map::Iter<'a>,
    /// The entry whose key was just read, for its value.
    pending: Option<(&'a str, &'a Value)>,
}

impl<'de> de::MapAccess<'de> for Entries<'_> {
    type Error = Refusal;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Refusal> {
        match self.entries.next() {
            Some((key, value)) => {
                self.pending = Some((key, value));
                seed.deserialize(name(key))
                    .map(Some)
                    .map_err(|e| e.within(key))
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<S: DeserializeSeed<'de>>(&mut self, seed: S) -> Result<S::Value, Refusal> {
        let Some((key, value)) = self.pending.take() else {
            return Err(de::Error::custom("a map value read before its key"));
        };
        seed.deserialize(Input(value)).map_err(|e| e.within(key))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

/// An externally tagged enum's `{"variant": value}`.
struct Variant<'a> {
    variant: &'a str,
    value: &'a Value,
}

impl<'de, 'a> de::EnumAccess<'de> for Variant<'a> {
    type Error = Refusal;
    type Variant = Input<'a>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Input<'a>), Refusal> {
        let variant = seed.deserialize(name(self.variant))?;
        Ok((variant, Input(self.value)))
    }
}

impl<'de> de::VariantAccess<'de> for Input<'_> {
    type Error = Refusal;

    fn unit_variant(self) -> Result<(), Refusal> {
        match self.0 {
            Value::Null => Ok(()),
            other => Err(de::Error::invalid_type(
                kind_of_value(other),
                &"unit variant",
            )),
        }
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(self, seed: S) -> Result<S::Value, Refusal> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, Refusal> {
        de::Deserializer::deserialize_any(self, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Refusal> {
        de::Deserializer::deserialize_any(self, visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    const MARKER: &str = "MARKER-7f3a";

    /// A field whose own deserializer quotes what it was given.
    #[derive(Debug, Deserialize)]
    struct Quotes {
        #[serde(deserialize_with = "quote")]
        _field: (),
    }

    fn quote<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
        let text = String::deserialize(deserializer)?;
        Err(de::Error::custom(format!("'{text}' is not allowed")))
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    enum Shape {
        Unit,
        Pair(u8, u8),
        Named { n: u8 },
    }

    fn refused<T: DeserializeOwned>(value: Value) -> (String, String) {
        let refusal = why_not::<T>(&value).expect("refused");
        (refusal.path.join("."), refusal.to_string())
    }

    #[test]
    fn a_free_text_message_is_kept_only_when_known() {
        assert_eq!(
            refused::<Quotes>(json!({"_field": MARKER})),
            ("_field".into(), "not a valid value here".into())
        );
        let known = <Refusal as de::Error>::custom(EXTERNAL_ACCOUNT_CONFIG_FORMS);
        assert_eq!(known.to_string(), EXTERNAL_ACCOUNT_CONFIG_FORMS);
        let untagged = format!("{UNTAGGED_PREFIX}ConfigValue");
        assert_eq!(
            <Refusal as de::Error>::custom(&untagged).to_string(),
            untagged
        );
        let lookalike = <Refusal as de::Error>::custom(format!("{UNTAGGED_PREFIX}{MARKER}"));
        assert_eq!(lookalike.to_string(), "not a valid value here");
    }

    #[test]
    fn a_refusal_names_the_path_and_the_kind_never_the_value() {
        assert_eq!(
            refused::<Shape>(json!({"Named": {"n": MARKER}})),
            (
                "Named.n".into(),
                "invalid type: a string, expected u8".into()
            )
        );
        assert_eq!(
            refused::<Shape>(json!(MARKER)),
            (
                String::new(),
                "unknown variant, expected one of `Unit`, `Pair`, `Named`".into()
            )
        );
        assert_eq!(
            refused::<Vec<Option<u8>>>(json!([1, null, MARKER])),
            ("2".into(), "invalid type: a string, expected u8".into())
        );
        assert_eq!(
            refused::<Shape>(json!({"Pair": [1]})),
            (
                "Pair".into(),
                "invalid length, expected tuple variant Shape::Pair with 2 elements".into()
            )
        );
        assert_eq!(
            refused::<Shape>(json!({"Named": {}})),
            ("Named".into(), "missing field `n`".into())
        );
        for accepted in [
            json!("Unit"),
            json!({"Unit": null}),
            json!({"Pair": [1, 2]}),
        ] {
            assert!(why_not::<Shape>(&accepted).is_none(), "{accepted}");
        }
    }
}
