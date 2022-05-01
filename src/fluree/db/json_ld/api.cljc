(ns fluree.db.json-ld.api
  (:require [fluree.db.conn.ipfs :as ipfs-conn]
            [fluree.db.conn.file :as file-conn]
            [fluree.db.json-ld.commit :as jld-commit]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.api.query :as query-api]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.util.log :as log])
  (:refer-clojure :exclude [merge]))

#?(:clj (set! *warn-on-reflection* true))

;; ledger operations

(defn connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network connection.

  Options include:
    - did - (optional) DiD information to use, if storing blocks as verifiable credentials,
            or issuing queries against a permissioned database.
    - context - (optional) Default @context map to use for ledgers formed with this connection.
    - write - (optional) Function to use for all writes, if empty will store in memory until a commit is performed
    - read - (optional) Function to use for reads of persisted blocks/data
    - commit - (optional) Function to use to write commits. If persistence desired, this must be defined
    - push - (optional) Function(s) in a vector that will attempt to push the commit to naming service(s)
    "
  [{:keys [method parallelism] :as opts}]
  ;; TODO - do some validation
  (let [opts* (assoc opts :parallelism (or parallelism 4))]
    (case method
      :ipfs (ipfs-conn/connect opts*)
      :file (file-conn/connect opts*)
      ))

  )

(defn connect-ipfs
  "Forms an ipfs connection using default settings.
  - server - (optional) IPFS http api server endpoint, defaults to http://127.0.0.1:5001/
  - profile - (optional) IPFS stored profile to use.
  - did - (optional) DiD information to use, if storing blocks as verifiable credentials
  - context - (optional) Default @context map to use for ledgers formed with this connection."
  [{:keys [server profile did context] :as opts}]
  (connect (assoc opts :method :ipfs))
  )


(defn create
  "Creates a new json-ld ledger. A connection (conn)
  must always be supplied.

  Ledger-name (optional) is a friendly name that is used for:
  - When publishing to a naming service that allows multiple pointers for the
    same namespace (e.g. IPNS), this becomes a sub-directory off the namespace.
    For multple directories deep, use '/' for a
    e.g. the ledgers movies/popular, books/authors, books/best-sellers could
    use the same IPNS id (in this example using IPNS DNSLink):
    fluree:ipns:my.dns.com/books/authors
    fluree:ipns:my.dns.com/books/best-sellers
    fluree:ipns:my.dns.com/movies/top-rated
  - When combining multiple ledgers, each ledger becomes an individual named
    graph which can be referenced by name.

  Options map (opts) can include:
  - did - DiD information to use, if storing blocks as verifiable credentials
  - context - Default @context map to use for ledgers formed with this connection
    "
  ([conn] (create conn nil nil))
  ([conn ledger-name] (create conn ledger-name nil))
  ([conn ledger-name opts]
   #?(:clj
      (let [p (promise)]
        (async/go
          (let [res (async/<! (jld-ledger/create conn ledger-name opts))]
            (when (util/exception? res)
              (log/error res (str "Error created new ledger: " ledger-name)))
            (deliver p res)))
        p)
      :cljs
      (js/Promise.
        (fn [resolve reject]
          (async/go
            (let [res (async/<! (jld-ledger/create conn ledger-name opts))]
              (if (util/exception? res)
                (reject res)
                (resolve res))
              )))))))

(defn index
  "Performs indexing operation on the specified ledger"
  [ledger]
  )

;; MAYBE CHALLENGE?
(defn validate
  "Validates a ledger, checks block integrity along with signatures."
  []

  )

(defn pull
  "Checks name service for ledger and pulls latest version locally."
  []
  )


(defn combine
  "Combines multiple ledgers into a new, read-only ledger."
  []
  )


;; mutations
(defn stage
  "Performs a transaction and queues change if valid (does not commit)"
  ([db-or-ledger json-ld] (stage db-or-ledger json-ld nil))
  ([db-or-ledger json-ld opts]
   (let [ledger*   (if (jld-ledger/is-ledger? db-or-ledger)
                     db-or-ledger
                     (:ledger db-or-ledger))
         result-ch (ledger-proto/-stage ledger* json-ld opts)]
     #?(:clj
        (let [p (promise)]
          (async/go
            (deliver p (async/<! result-ch)))
          p)
        :cljs
        (js/Promise.
          (fn [resolve reject]
            (async/go
              (let [res (async/<! result-ch)]
                (if (util/exception? res)
                  (reject res)
                  (resolve res))))))))))


(defn commit
  "Commits one or more transactions that are queued."
  ([db] (commit db nil))
  ([db opts]
   (let [opts* (if (string? opts)
                 {:message opts}
                 opts)]
     (jld-commit/commit db opts*))))


(defn push
  "Pushes one or more commits to a naming service, e.g. a Fluree Network, IPNS, DNS, Fluree Nexus.
  Depending on consensus requirements for a Fluree Network, will accept or reject push as newest update."
  []
  )


(defn squash
  "Squashes multiple transactions into a single transaction"
  []
  )


(defn merge
  "Merges changes from one branch into another branch."
  []
  )


(defn branch
  "Creates a new branch of a given ledger"
  []
  )

;; db operations

(defn db
  "Retrieves latest db, or optionally a db at a moment in time
  potentially with permissions of a specific user."
  ([ledger] (db ledger nil))
  ([ledger {:keys [t branch] :as opts}]
   (if opts
     (throw (ex-info "DB opts not yet implemented"
                     {:status 500 :error :db/unexpected-error}))
     (ledger-proto/-db-latest ledger branch))))


(defn query
  [db query]
  #?(:clj
     (let [p (promise)]
       (async/go
         (deliver p (async/<! (query-api/query-async db query))))
       p)
     :cljs
     (js/Promise.
       (fn [resolve reject]
         (async/go
           (resolve (async/<! (query-api/query-async db query))))))))
