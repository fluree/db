//! Typed Bolt messages over PackStream structures.
//!
//! Every message is a PackStream structure whose signature identifies the
//! message type. Requests (client → server) decode into [`Request`];
//! responses (server → client) build from [`Response`] constructors.

use crate::packstream::{self, Decoder};
use crate::value::{MapValue, Structure, Value};
use crate::DecodeError;

// Request signatures.
pub const HELLO: u8 = 0x01;
pub const GOODBYE: u8 = 0x02;
pub const RESET: u8 = 0x0F;
pub const RUN: u8 = 0x10;
pub const BEGIN: u8 = 0x11;
pub const COMMIT: u8 = 0x12;
pub const ROLLBACK: u8 = 0x13;
pub const DISCARD: u8 = 0x2F;
pub const PULL: u8 = 0x3F;
pub const ROUTE: u8 = 0x66;
pub const LOGON: u8 = 0x6A;
pub const LOGOFF: u8 = 0x6B;
pub const TELEMETRY: u8 = 0x54;

// Response signatures.
pub const SUCCESS: u8 = 0x70;
pub const RECORD: u8 = 0x71;
pub const IGNORED: u8 = 0x7E;
pub const FAILURE: u8 = 0x7F;

/// A decoded client request.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Hello {
        extra: MapValue,
    },
    Logon {
        auth: MapValue,
    },
    Logoff,
    Goodbye,
    Reset,
    Run {
        query: String,
        parameters: MapValue,
        extra: MapValue,
    },
    Begin {
        extra: MapValue,
    },
    Commit,
    Rollback,
    Discard {
        extra: MapValue,
    },
    Pull {
        extra: MapValue,
    },
    Route {
        routing: MapValue,
        bookmarks: Vec<Value>,
        extra: Value,
    },
    Telemetry {
        api: i64,
    },
}

impl Request {
    /// Decode one message payload (as produced by
    /// [`crate::chunk::ChunkAssembler`]) into a typed request.
    pub fn decode(payload: &[u8]) -> Result<Request, DecodeError> {
        let mut decoder = Decoder::new(payload);
        let value = decoder.decode_value()?;
        if decoder.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes after message"));
        }
        let Value::Structure(Structure { signature, fields }) = value else {
            return Err(DecodeError::new("message is not a structure"));
        };
        let mut fields = fields.into_iter();
        fn next_map(
            fields: &mut std::vec::IntoIter<Value>,
            what: &str,
        ) -> Result<MapValue, DecodeError> {
            match fields.next() {
                Some(Value::Map(m)) => Ok(m),
                other => Err(DecodeError::new(format!(
                    "{what}: expected map, got {other:?}"
                ))),
            }
        }
        match signature {
            HELLO => Ok(Request::Hello {
                extra: next_map(&mut fields, "HELLO extra")?,
            }),
            LOGON => Ok(Request::Logon {
                auth: next_map(&mut fields, "LOGON auth")?,
            }),
            LOGOFF => Ok(Request::Logoff),
            GOODBYE => Ok(Request::Goodbye),
            RESET => Ok(Request::Reset),
            RUN => {
                let query = match fields.next() {
                    Some(Value::String(s)) => s.to_string(),
                    other => {
                        return Err(DecodeError::new(format!(
                            "RUN query: expected string, got {other:?}"
                        )))
                    }
                };
                let parameters = next_map(&mut fields, "RUN parameters")?;
                // Bolt 3 RUN had two fields; 4.0+ adds extra.
                let extra = match fields.next() {
                    Some(Value::Map(m)) => m,
                    None => MapValue::new(),
                    other => {
                        return Err(DecodeError::new(format!(
                            "RUN extra: expected map, got {other:?}"
                        )))
                    }
                };
                Ok(Request::Run {
                    query,
                    parameters,
                    extra,
                })
            }
            BEGIN => Ok(Request::Begin {
                extra: next_map(&mut fields, "BEGIN extra")?,
            }),
            COMMIT => Ok(Request::Commit),
            ROLLBACK => Ok(Request::Rollback),
            DISCARD => Ok(Request::Discard {
                extra: next_map(&mut fields, "DISCARD extra")?,
            }),
            PULL => Ok(Request::Pull {
                extra: next_map(&mut fields, "PULL extra")?,
            }),
            ROUTE => {
                let routing = next_map(&mut fields, "ROUTE routing")?;
                let bookmarks = match fields.next() {
                    Some(Value::List(l)) => l,
                    other => {
                        return Err(DecodeError::new(format!(
                            "ROUTE bookmarks: expected list, got {other:?}"
                        )))
                    }
                };
                // 4.3 carries a db string; 4.4+ an extra map. Pass through.
                let extra = fields.next().unwrap_or(Value::Null);
                Ok(Request::Route {
                    routing,
                    bookmarks,
                    extra,
                })
            }
            TELEMETRY => {
                let api = match fields.next() {
                    Some(Value::Integer(i)) => i,
                    other => {
                        return Err(DecodeError::new(format!(
                            "TELEMETRY api: expected integer, got {other:?}"
                        )))
                    }
                };
                Ok(Request::Telemetry { api })
            }
            other => Err(DecodeError::new(format!(
                "unknown message signature 0x{other:02X}"
            ))),
        }
    }
}

/// A server response, ready to encode.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Success(MapValue),
    Record(Vec<Value>),
    Ignored,
    Failure { code: String, message: String },
}

impl Response {
    pub fn success(metadata: MapValue) -> Self {
        Response::Success(metadata)
    }

    pub fn success_empty() -> Self {
        Response::Success(MapValue::new())
    }

    pub fn failure(code: impl Into<String>, message: impl Into<String>) -> Self {
        Response::Failure {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Encode to a message payload (before chunking).
    pub fn encode(&self) -> Vec<u8> {
        let structure = match self {
            Response::Success(meta) => Structure {
                signature: SUCCESS,
                fields: vec![Value::Map(meta.clone())],
            },
            Response::Record(values) => Structure {
                signature: RECORD,
                fields: vec![Value::List(values.clone())],
            },
            Response::Ignored => Structure {
                signature: IGNORED,
                fields: vec![],
            },
            Response::Failure { code, message } => {
                let mut meta = MapValue::new();
                meta.insert("code", code.as_str());
                meta.insert("message", message.as_str());
                Structure {
                    signature: FAILURE,
                    fields: vec![Value::Map(meta)],
                }
            }
        };
        packstream::encode_to_vec(&Value::Structure(structure))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_hello() {
        // HELLO {"user_agent": "test/1.0"} — struct B1 0x01, tiny map.
        let mut extra = MapValue::new();
        extra.insert("user_agent", "test/1.0");
        let payload = packstream::encode_to_vec(&Value::Structure(Structure {
            signature: HELLO,
            fields: vec![Value::Map(extra.clone())],
        }));
        assert_eq!(Request::decode(&payload).unwrap(), Request::Hello { extra });
    }

    #[test]
    fn decodes_bolt44_style_run() {
        // RUN "RETURN 1 AS num" {} {"db": "appdb"}
        let mut extra = MapValue::new();
        extra.insert("db", "appdb");
        let payload = packstream::encode_to_vec(&Value::Structure(Structure {
            signature: RUN,
            fields: vec![
                Value::from("RETURN 1 AS num"),
                Value::empty_map(),
                Value::Map(extra.clone()),
            ],
        }));
        assert_eq!(
            Request::decode(&payload).unwrap(),
            Request::Run {
                query: "RETURN 1 AS num".into(),
                parameters: MapValue::new(),
                extra,
            }
        );
    }

    #[test]
    fn decodes_two_field_run() {
        // Bolt 3-shaped RUN (no extra) still decodes.
        let payload = packstream::encode_to_vec(&Value::Structure(Structure {
            signature: RUN,
            fields: vec![Value::from("RETURN 1"), Value::empty_map()],
        }));
        assert!(matches!(
            Request::decode(&payload).unwrap(),
            Request::Run { extra, .. } if extra.is_empty()
        ));
    }

    #[test]
    fn decodes_pull_with_n() {
        let mut extra = MapValue::new();
        extra.insert("n", 1000i64);
        let payload = packstream::encode_to_vec(&Value::Structure(Structure {
            signature: PULL,
            fields: vec![Value::Map(extra.clone())],
        }));
        assert_eq!(Request::decode(&payload).unwrap(), Request::Pull { extra });
    }

    #[test]
    fn simple_signals_decode() {
        for (sig, expected) in [
            (RESET, Request::Reset),
            (GOODBYE, Request::Goodbye),
            (COMMIT, Request::Commit),
            (ROLLBACK, Request::Rollback),
            (LOGOFF, Request::Logoff),
        ] {
            let payload = packstream::encode_to_vec(&Value::Structure(Structure {
                signature: sig,
                fields: vec![],
            }));
            assert_eq!(Request::decode(&payload).unwrap(), expected);
        }
    }

    #[test]
    fn unknown_signature_rejected() {
        let payload = packstream::encode_to_vec(&Value::Structure(Structure {
            signature: 0x42,
            fields: vec![],
        }));
        assert!(Request::decode(&payload).is_err());
    }

    #[test]
    fn failure_encodes_code_and_message() {
        let failure = Response::failure("Neo.ClientError.Statement.SyntaxError", "boom");
        let decoded = packstream::decode_exact(&failure.encode()).unwrap();
        let Value::Structure(s) = decoded else {
            panic!("not a structure")
        };
        assert_eq!(s.signature, FAILURE);
        let Value::Map(meta) = &s.fields[0] else {
            panic!("no metadata map")
        };
        assert_eq!(
            meta.get_str("code"),
            Some("Neo.ClientError.Statement.SyntaxError")
        );
        assert_eq!(meta.get_str("message"), Some("boom"));
    }

    #[test]
    fn record_wire_shape() {
        // RECORD [1, "a"] -> B1 71 92 01 81 61
        let rec = Response::Record(vec![Value::Integer(1), Value::from("a")]);
        assert_eq!(rec.encode(), vec![0xB1, 0x71, 0x92, 0x01, 0x81, 0x61]);
    }
}
