(ns fluree.db.conn.proto
  (:refer-clojure :exclude [-lookup]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iConnection
  (-close [conn] "Closes all resources for this connection")
  (-closed? [conn] "Indicates if connection is open or closed")
  (-method [conn] "Returns connection method type (as keyword)")
  (-parallelism [conn] "Returns parallelism integer to use for running multi-thread operations (1->8)")
  (-id [conn] "Returns internal id for connection object")
  (-context [conn] "Returns optional default context set at connection level")
  (-new-indexer [conn opts] "Returns optional default new indexer object for a new ledger with optional opts.")
  (-did [conn] "Returns optional default did map if set at connection level")
  (-msg-in [conn msg] "Handler for incoming message from connection service")
  (-msg-out [conn msg] "Pushes outgoing messages/commands to connection service")
  (-state [conn] [conn ledger] "Returns internal state-machine information for connection, or specific ledger"))

(defprotocol iStorage
  (-c-read [conn commit-key] "Reads a commit from storage")
  (-c-write [conn ledger commit-data] "Writes a commit to storage")
  (-ctx-write [conn ledger context-data] "Writes a context to storage and returns the key. Expects string keys.")
  (-ctx-read [conn context-key] "Reads a context from storage"))

(defprotocol iNameService
  (-push [conn address commit-data] "Pushes ledger metadata to all name service destinations")
  (-pull [conn ledger-address] "Performs a pull operation from all name service destinations")
  (-subscribe [conn ledger] "Creates a subscription to nameservice(s) for ledger events")
  (-lookup [conn ledger-address] "Performs lookup operation on ledger address and returns latest commit address")
  (-alias [conn ledger-address] "Given a ledger address, returns ledger's default alias name else nil, if not avail")
  (-address [conn ledger-alias key] "Returns address/iri for provided ledger alias specific to the connection type")
  (-exists? [conn ledger-address] "Returns true if ledger exists (must have had at least one commit), false otherwise"))
