//! SDL rendering.
//!
//! async-graphql derives SDL from the registered dynamic schema, so rendering goes
//! through the same registration path execution does — the printed schema and the
//! executable schema cannot drift.

use crate::error::Result;
use crate::runtime::build_schema;
use crate::schema::model::SchemaModel;

/// Render a model as SDL.
pub fn sdl(model: &SchemaModel) -> Result<String> {
    sdl_with_mutations(model, &[])
}

/// Render a model as SDL, including the given mutation fields.
pub fn sdl_with_mutations(
    model: &SchemaModel,
    mutations: &[crate::mutate::MutationField],
) -> Result<String> {
    // Limits do not appear in SDL and this schema is never executed, so the
    // defaults stand in for whatever the endpoint is configured with.
    Ok(build_schema(model, mutations, &crate::limits::Limits::default())?.sdl())
}
