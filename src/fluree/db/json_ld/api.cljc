(ns fluree.db.json-ld.api
  (:require [fluree.db.conn.ipfs :as ipfs-conn]
            [fluree.db.conn.file :as file-conn]
            [fluree.db.conn.memory :as memory-conn]
            [fluree.db.platform :as platform]
            [clojure.core.async :as async]
            [fluree.db.api.query :as query-api]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.util.log :as log]
            [fluree.db.query.range :as query-range])
  (:refer-clojure :exclude [merge load range]))

#?(:clj (set! *warn-on-reflection* true))

(defn- promise-wrap
  "Wraps an async channel that will contain a response in a promise."
  [port]
  #?(:clj
     (let [p (promise)]
       (async/go
         (let [res (async/<! port)]
           (when (util/exception? res)
             (log/error res))
           (deliver p res)))
       p)
     :cljs
     (js/Promise.
       (fn [resolve reject]
         (async/go
           (let [res (async/<! port)]
             (if (util/exception? res)
               (reject res)
               (resolve res))))))))

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
  (promise-wrap
    (let [opts*   (assoc opts :parallelism (or parallelism 4))
          method* (keyword method)]
      (case method*
        :ipfs (ipfs-conn/connect opts*)
        :file (if platform/BROWSER
                (throw (ex-info "File connection not supported in the browser" opts))
                (file-conn/connect opts*))
        :memory (memory-conn/connect opts*)))))

(defn connect-ipfs
  "Forms an ipfs connection using default settings.
  - server - (optional) IPFS http api server endpoint, defaults to http://127.0.0.1:5001/
  - profile - (optional) IPFS stored profile to use.
  - did - (optional) DiD information to use, if storing blocks as verifiable credentials
  - context - (optional) Default @context map to use for ledgers formed with this connection."
  [opts]
  (connect (assoc opts :method :ipfs)))

(defn connect-memory
  "Forms an in-memory connection using default settings.
  - did - (optional) DiD information to use, if storing blocks as verifiable credentials
  - context - (optional) Default @context map to use for ledgers formed with this connection."
  [opts]
  (connect (assoc opts :method :memory)))


(defn create
  "Creates a new json-ld ledger. A connection (conn)
  must always be supplied.

  Ledger-name (optional) is a friendly name that is used for:
  - When publishing to a naming service that allows multiple pointers for the
    same namespace (e.g. IPNS), this becomes a sub-directory off the namespace.
    For multple directories deep, use '/' for a
    e.g. the ledgers movies/popular, books/authors, books/best-sellers could
    use the same IPNS id (in this example using IPNS DNSLink):
    fluree:ipns://my.dns.com/books/authors
    fluree:ipns://my.dns.com/books/best-sellers
    fluree:ipns://my.dns.com/movies/top-rated
  - When combining multiple ledgers, each ledger becomes an individual named
    graph which can be referenced by name.

  Options map (opts) can include:
  - did - DiD information to use, if storing blocks as verifiable credentials
  - context - Default @context map to use for ledgers formed with this connection
    "
  ([conn] (create conn nil nil))
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (let [res-ch (jld-ledger/create conn ledger-alias opts)]
     (promise-wrap res-ch))))

(defn load
  "Loads a ledger defined with a Fluree address, e.g.:
  fluree:ipfs://Qmaq4ip1bJq6255S5PhU8veo6gxaq2yyucKZmJkV1WW8YG
  fluree:ipns://k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2
  fluree:ipns://my.dns.com/movies/top-rated
  fluree:local://my/db
  fluree:s3:...."
  ([address]
   ;; TODO - when given an address only, can create or retrieve from cache a conn
   ;; TODO - for that particular method
   (throw (ex-info "Not yet implemented" {:status 500 :error :db/unexpected-error})))
  ([conn address]
   (promise-wrap
     (jld-ledger/load conn address))))

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
  ([db-or-ledger json-ld {:keys [branch] :as opts}]
   (let [db        (if (jld-ledger/is-ledger? db-or-ledger)
                     (ledger-proto/-db db-or-ledger {:branch branch})
                     db-or-ledger)
         result-ch (db-proto/-stage db json-ld opts)]
     (promise-wrap result-ch))))


(defn commit!
  "Commits a staged database to the ledger with all changes since the last commit
  aggregated together.

  Commits are tracked in the local environment, but if the ledger is distributed
  it will still need a 'push' to ensure it is published and verified as per the
  distributed rules."
  ([db]
   (promise-wrap
     (ledger-proto/-commit! db)))
  ([ledger-or-db db-or-opts]
   (let [[ledger db opts]
         [(when (jld-ledger/is-ledger? ledger-or-db)
            ledger-or-db)
          (cond (db-proto/db? ledger-or-db) ledger-or-db
                (db-proto/db? db-or-opts) db-or-opts)
          (when (not (db-proto/db? db-or-opts))
            db-or-opts)]

         res-ch (if ledger
                  (ledger-proto/-commit! ledger db opts)
                  (ledger-proto/-commit! db opts))]
     (promise-wrap res-ch)))
  ([ledger db opts]
   (promise-wrap
     (ledger-proto/-commit! ledger db opts))))


(defn status
  "Returns current status of ledger branch."
  ([ledger] (ledger-proto/-status ledger))
  ([ledger branch] (ledger-proto/-status ledger branch)))


(defn push
  "Pushes all commits since last push to a naming service, e.g. a Fluree Network, IPNS, DNS, Fluree Nexus.
  Depending on consensus requirements for a Fluree Network, will accept or reject push as newest update."
  []
  )


(defn squash
  "Squashes multiple unpublished commits into a single unpublished commit"
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
  and/or permissioned to a specific identity."
  ([ledger] (db ledger nil))
  ([ledger {:keys [t branch] :as opts}]
   (if opts
     (throw (ex-info "DB opts not yet implemented"
                     {:status 500 :error :db/unexpected-error}))
     (ledger-proto/-db ledger opts))))


(defn query
  [db query]
  (let [res-chan (query-api/query-async db query)]
    (promise-wrap res-chan)))

(defn range
  "Performs a range scan against the specified index using test functions
  of >=, <=, >, <"
  ;; TODO - assert index is valid index type
  ([db index test match]
   (promise-wrap
     (query-range/index-range db index test match)))
  ([db index start-test start-match end-test end-match]
   (promise-wrap
     (query-range/index-range db index start-test start-match end-test end-match))))

(defn slice
  "Like range, but returns all flakes that match the supplied flake parts."
  [db index match]
  (promise-wrap
    (query-range/index-range db index = match)))

(defn expand-iri
  "Expands given IRI with the default database context, or provided context."
  ([db compact-iri]
   (db-proto/-expand-iri db compact-iri))
  ([db compact-iri context]
   (db-proto/-expand-iri db compact-iri context)))

(defn internal-id
  "Returns the internal Fluree integer id for a given IRI.
  This can be used for doing range scans, slices and for other
  more advanced needs.

  Returns promise"
  [db iri]
  (promise-wrap
    (->> (expand-iri db iri)
         (db-proto/-subid db))))

