(ns fluree.sdk.clojure
  (:require [fluree.db.json-ld.api :as api]
            [fluree.db.json-ld.transact :as ftx]
            [fluree.db.util.log :as log]
            [fluree.db.util.validation :as v]
            [fluree.db.query.fql.syntax :as fql]
            [fluree.db.query.fql.parse :as fqp]
            [fluree.db.query.history :as fqh]
            [fluree.json-ld :as json-ld])
  (:refer-clojure :exclude [load]))

(defn connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network connection.

  Options include:
    - :defaults - (optional) with any of the following values:
      - :did - (optional) DiD information to use, if storing blocks as verifiable
              credentials, or issuing queries against a permissioned database.
      - :context - (optional) Default @context map to use for ledgers formed with
                  this connection."
  [opts]
  (let [opts* (v/coerce-connect-opts opts)]
    (log/debug "connect opts:" opts*)
    (api/connect opts*)))

(defn create
  "Creates a new json-ld ledger. A connection (conn) must always be supplied.

  Ledger-alias (optional) is a friendly name that is used for:
  - When publishing to a naming service that allows multiple pointers for the
    same namespace (e.g. IPNS), this becomes a sub-directory off the namespace.
    For multiple directories deep, use '/' for a
    e.g. the ledgers movies/popular, books/authors, books/best-sellers could
    use the same IPNS id (in this example using IPNS DNSLink):
    fluree:ipns://my.dns.com/books/authors
    fluree:ipns://my.dns.com/books/best-sellers
    fluree:ipns://my.dns.com/movies/top-rated
  - When combining multiple ledgers, each ledger becomes an individual named
    graph which can be referenced by name.

  Options map (opts) can include:
  - :defaults
    - :did - DiD information to use, if storing blocks as verifiable credentials
    - :context - Default @context map to use for ledgers formed with this connection"
  ([conn] (create conn nil nil))
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (let [opts*  (v/coerce-create-opts opts)]
     (api/create conn ledger-alias opts*))))

(defn load
  "Loads an existing ledger by its alias (which will be converted to a
  connection-specific address first)."
  [conn ledger-alias]
  (api/load conn ledger-alias))

(defn exists?
  "Returns a promise with true if the ledger alias or address exists, false
  otherwise."
  [conn ledger-alias-or-address]
  (api/exists? conn ledger-alias-or-address))

(defn stage
  "Performs a transaction and queues change if valid (does not commit)"
  ([db json-ld] (stage db json-ld nil))
  ([db json-ld opts]
   (let [json-ld* (ftx/coerce-txn json-ld)]
     (api/stage db json-ld* opts))))

(defn commit!
  ([ledger db] (commit! ledger db nil))
  ([ledger db opts] (api/commit! ledger db opts)))

(defn db
  "Retrieves latest db, or optionally a db at a moment in time
  and/or permissioned to a specific identity."
  ([ledger] (db ledger nil))
  ([ledger opts] (api/db ledger opts)))

(defn query
  [db query]
  (let [context        (json-ld/parse-context (fqp/parse-context query db))
        encode-results (fql/analytical-query-results-encoder context)]
    (future
     (->> query
          fql/coerce-analytical-query
          (api/query db)
          deref
          (log/debug->>val "pre-encoded query results:")
          encode-results))))

(defn multi-query
  [db query]
  (let [context        (json-ld/parse-context (fqp/parse-context query db))
        encode-result  (fql/analytical-query-results-encoder context)
        encode-results #(reduce-kv (fn [rs k r]
                                     (assoc rs k (encode-result r)))
                                   {} %)]
    (future
     (->> query
          fql/coerce-multi-query
          (api/multi-query db)
          deref
          (log/debug->>val "pre-encoded multi query results:")
          encode-results))))

(defn history
  [ledger query]
  (let [latest-db (db ledger)
        context (json-ld/parse-context (fqp/parse-context query latest-db))
        encode-results (fqh/results-encoder context)]
    (future
     (->> query
          fqh/coerce-query
          (api/history ledger)
          deref
          (log/debug->>val "pre-encoded history query results:")
          encode-results))))
