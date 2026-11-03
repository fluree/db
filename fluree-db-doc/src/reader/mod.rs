//! Extraction by an HTTP reader: a model that takes the whole document, its
//! ontology and its catalog as one RDF dataset and answers with a reading,
//! an RDF graph of mentions, entities and claims anchored to the text's
//! offsets. Where the language-model extractor asks about each chunk, a
//! reader reads the document as the parser left it, in as few calls as its
//! limits allow.
//!
//! - [`request`] builds the dataset and cuts a long document into calls;
//! - [`client`] makes the call, and resends only what was refused for
//!   capacity;
//! - [`reading`] turns the answer into nodes for the document's commit;
//! - [`session`] reads one document with all three, as the CLI and the
//!   Python package both do.

pub mod client;
pub mod reading;
pub mod request;
pub mod session;

pub use client::{Answer, Credential, ReaderClient, Usage};
pub use reading::{profile, read_answer, Frame, Profile, Reading, Vocabulary};
pub use request::{Document, Limits, Part, PromptSize, Structure};
pub use session::{Read, Session, CACHE_MODES};
