(ns fluree.db.conn.proto)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iConnection
  (-close [conn] "Closes all resources for this connection")
  (-closed? [conn] "Indicates if connection is open or closed")
  (-method [conn] "Returns connection method type (as keyword)")
  (-parallelism [conn] "Returns parallelism integer to use for running multi-thread operations (1->8)")
  (-id [conn] "Returns internal id for connection object")
  (-default-context [conn] [conn context-type] "Returns optional default context set at connection level")
  (-context-type [conn] "Returns the context-type for the default-context")
  (-new-indexer [conn opts] "Returns optional default new indexer object for a new ledger with optional opts.")
  (-did [conn] "Returns optional default did map if set at connection level")
  (-msg-in [conn msg] "Handler for incoming message from nameservices")
  (-msg-out [conn msg] "Pushes outgoing messages/commands to connection service")
  (-nameservices [conn] "Returns a sequence of all nameservices configured for the connection.")
  (-state [conn] [conn ledger] "Returns internal state-machine information for connection, or specific ledger"))

(defprotocol iStorage
  (-c-read [conn commit-key] "Reads a commit from storage")
  (-c-write [conn ledger commit-data] "Writes a commit to storage")
  (-ctx-write [conn ledger context-data] "Writes a context to storage and returns the key. Expects string keys.")
  (-ctx-read [conn context-key] "Reads a context from storage")
  (-txn-write [conn ledger txn-data] "Writes a transaction to storage and returns the key. Expects string keys.")
  (-txn-read [conn txn-key] "Reads a transaction from storage")
  (-index-file-write [conn ledger idx-type index-data] "Writes an index item to storage")
  (-index-file-read [conn file-address] "Reads an index item from storage"))
