//! Scan planning for Iceberg tables.
//!
//! This module provides:
//! - [`predicate`] - Filter expression types for pushdown
//! - [`pruning`] - Partition and file pruning using statistics
//! - [`planner`] - Scan planning and file task generation
//! - [`send_planner`] - Send-safe scan planning for AWS SDK integration

pub mod planner;
pub mod predicate;
pub mod pruning;
#[cfg(feature = "aws")]
pub mod send_planner;

pub use planner::{FileScanTask, ScanConfig, ScanPlan, ScanPlanner};
pub use predicate::{ComparisonOp, Expression, LiteralValue};
pub use pruning::{can_contain_file, can_contain_partition};
#[cfg(feature = "aws")]
pub use send_planner::SendScanPlanner;
