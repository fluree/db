#[path = "support/mod.rs"]
mod support;

// Own binary: the link-lowering tests set a process-wide environment flag.
#[path = "it_triple_term_links.rs"]
mod it_triple_term_links;
