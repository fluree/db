//! Builder entry points on GraphDb and DataSetDb.
//!
//! These `impl` blocks provide the `.query()` method that returns the
//! appropriate builder type (ViewQueryBuilder or DatasetQueryBuilder).

use crate::query::builder::{DatasetQueryBuilder, ViewQueryBuilder};
use crate::view::{DataSetDb, GraphDb};
use crate::Fluree;

impl GraphDb {
    /// Create a query builder for this graph/view.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = fluree.db("mydb:main").await?;
    /// let result = view.query(&fluree)
    ///     .jsonld(&query)
    ///     .execute().await?;
    /// ```
    pub fn query<'a>(&'a self, fluree: &'a Fluree) -> ViewQueryBuilder<'a> {
        ViewQueryBuilder::new(fluree, self)
    }
}

impl DataSetDb {
    /// Create a query builder for this dataset.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let dataset = DataSetDb::new()
    ///     .with_default(view_a)
    ///     .with_named("other", view_b);
    /// let result = dataset.query(&fluree)
    ///     .jsonld(&query)
    ///     .execute().await?;
    /// ```
    pub fn query<'a>(&'a self, fluree: &'a Fluree) -> DatasetQueryBuilder<'a> {
        DatasetQueryBuilder::new(fluree, self)
    }
}
