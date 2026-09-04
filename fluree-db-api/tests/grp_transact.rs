#[path = "support/mod.rs"]
mod support;

#[path = "it_cached_handle_cow_cancel.rs"]
mod it_cached_handle_cow_cancel;
#[path = "it_cached_handle_cow_recovery.rs"]
mod it_cached_handle_cow_recovery;
#[path = "it_concurrent_update_reconcile.rs"]
mod it_concurrent_update_reconcile;
#[path = "it_enforce_unique_upsert_indexed.rs"]
mod it_enforce_unique_upsert_indexed;
#[path = "it_jsonld_empty_list.rs"]
mod it_jsonld_empty_list;
#[path = "it_raw_txn_parallel_upload.rs"]
mod it_raw_txn_parallel_upload;
#[path = "it_stable_blank_nodes.rs"]
mod it_stable_blank_nodes;
#[path = "it_transact.rs"]
mod it_transact;
#[path = "it_transact_conditional.rs"]
mod it_transact_conditional;
#[path = "it_transact_datatype_cancellation.rs"]
mod it_transact_datatype_cancellation;
#[path = "it_transact_growth_slope.rs"]
mod it_transact_growth_slope;
#[path = "it_transact_list_container.rs"]
mod it_transact_list_container;
#[path = "it_transact_list_retract.rs"]
mod it_transact_list_retract;
#[path = "it_transact_object_var.rs"]
mod it_transact_object_var;
#[path = "it_transact_pure_delete_dedup.rs"]
mod it_transact_pure_delete_dedup;
#[path = "it_transact_update.rs"]
mod it_transact_update;
#[path = "it_transact_update_indexed.rs"]
mod it_transact_update_indexed;
#[path = "it_transact_upsert.rs"]
mod it_transact_upsert;
#[path = "it_transact_upsert_indexed.rs"]
mod it_transact_upsert_indexed;
#[path = "it_turtle_empty_collection.rs"]
mod it_turtle_empty_collection;
#[path = "it_txn_meta.rs"]
mod it_txn_meta;
#[path = "it_update_wildcard_delete_indexed.rs"]
mod it_update_wildcard_delete_indexed;
#[path = "it_upsert_duplicate_ids_repro.rs"]
mod it_upsert_duplicate_ids_repro;
