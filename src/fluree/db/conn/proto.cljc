(ns fluree.db.conn.proto)

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iConnection
  (-close [conn] "Closes all resources for this connection")
  (-closed? [conn] "Indicates if connection is open or closed")
  (-method [conn] "Returns connection method type (as keyword)")
  (-parallelism [conn] "Returns parallelism integer to use for running multi-thread operations (1->8)")
  (-transactor? [conn] "Returns true if this connection is running on a transactor service")
  (-id [conn] "Returns internal id for connection object")
  (-read-only? [conn] "Returns true if a read-only connection")
  (-context [conn] "Returns optional default context set at connection level")
  (-new-indexer [conn opts] "Returns optional default new indexer object for a new ledger with optional opts.")
  (-did [conn] "Returns optional default did map if set at connection level")
  (-msg-in [conn msg] "Handler for incoming message from connection service")
  (-msg-out [conn msg] "Pushes outgoing messages/commands to connection service")
  (-state [conn] [conn ledger] "Returns internal state-machine information for connection, or specific ledger")
  )

(defprotocol iStorage
  (-c-read [conn commit-key] "Reads a commit from storage")
  (-c-write [conn commit-data] "Writes a commit to storage"))

(defprotocol iNameService
  (-push [conn address commit-data] "Pushes ledger metadata to all name service destinations")
  (-pull [conn ledger-address] "Performs a pull operation from all name service destinations")
  (-subscribe [conn ledger] "Creates a subscription to nameservice(s) for ledger events")
  (-lookup [conn ledger-address] "Performs lookup operation on ledger address and returns latest commit address")
  (-address [conn ledger-alias key] "Returns address/iri for provided ledger alias specific to the connection type"))
