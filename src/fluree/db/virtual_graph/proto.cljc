(ns fluree.db.virtual-graph.proto)

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result")
  (serialize [this] "Returns a JSON serializable representation of the virtual graph (does not serialize to JSON)")
  (deserialize [this source-db data] "Reifies the virtual graph from the provided data structure"))
