//! Extraction by an HTTP reader: a model that takes the whole document, its
//! ontology and its catalog as one RDF dataset and answers with a reading:
//! a Fluree DB transaction holding the run, the facts it read in the
//! ontology's terms, the claims behind them and, when asked, the mentions
//! anchored to the text's offsets. Where the language-model extractor asks
//! about each chunk, a reader reads the document as the parser left it, in
//! as few calls as its limits allow.
//!
//! - [`request`] builds the dataset and cuts a long document into calls;
//! - [`client`] makes the call with its controls as headers, and resends
//!   only what was refused for capacity;
//! - [`reading`] reads the answer (response layout 2) and prints it;
//! - [`session`] reads one document with all three, as the CLI and the
//!   Python package both do.

pub mod client;
pub mod reading;
pub mod request;
pub mod session;

pub use client::{Answer, Controls, Credential, Include, ReaderClient, Usage};
pub use reading::{read_answer, Claim, Frame, Mention, Reading, LAYOUT, XT_NS};
pub use request::{Document, Limits, Part, PromptSize, Structure};
pub use session::{Read, Session, CACHE_MODES};
