(ns fluree.db.json-ld.api
  (:require [clojure.string :as str]
            [fluree.db.conn.ipfs :as ipfs-conn]
            [fluree.db.conn.file :as file-conn]
            [fluree.db.conn.memory :as memory-conn]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.platform :as platform]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.db.api.query :as query-api]
            [fluree.db.util.core :as util]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.util.log :as log]
            [fluree.db.query.range :as query-range]
            [fluree.db.json-ld.policy :as perm])
  (:refer-clojure :exclude [merge load range exists?]))

#?(:clj (set! *warn-on-reflection* true))

(defn promise-wrap
  "Wraps an async channel that will contain a response in a promise."
  [port]
  #?(:clj
     (let [p (promise)]
       (go
         (let [res (<! port)]
           (when (util/exception? res)
             (log/error res))
           (deliver p res)))
       p)
     :cljs
     (js/Promise.
       (fn [resolve reject]
         (go
           (let [res (<! port)]
             (if (util/exception? res)
               (reject res)
               (resolve res))))))))

;; ledger operations

(defn connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network connection.

  Options include (keys are strings):
    - defaults:
      - did - (optional) DiD information to use, if storing blocks as verifiable credentials,
              or issuing queries against a permissioned database.
      - context - (optional) Default @context map to use for ledgers formed with this connection."
  [{:strs [method parallelism] :as opts}]
  ;; TODO - do some validation
  (promise-wrap
    (let [parallelism* (or parallelism 4)
          opts*        (assoc opts "parallelism" parallelism*)
          shared-opts  (-> opts*
                           (util/update-in-if-contains ["defaults" "did"]
                                                       util/assoc-from-str-opts
                                                       #{"id" "public" "private"})
                           (util/update-in-if-contains ["defaults"]
                                                       util/assoc-from-str-opts
                                                       #{"did"
                                                         {"@context" :context}})
                           (util/assoc-from-str-opts
                             #{"defaults" "lru-cache-atom" "memory" "parallelism"}))
          method*      (keyword method)]
      (log/debug "connect shared opts:" shared-opts)
      (case method*
        :ipfs (ipfs-conn/connect (->> shared-opts
                                      (util/assoc-from-str-opts
                                        opts
                                        #{"server" "memory" "serializer"})))
        :file (if platform/BROWSER
                (throw (ex-info "File connection not supported in the browser" opts))
                (file-conn/connect (->> shared-opts
                                        (util/assoc-from-str-opts
                                          opts
                                          #{"storage-path"}))))
        :memory (memory-conn/connect shared-opts)))))

(defn connect-ipfs
  "Forms an ipfs connection using default settings.
  - server - (optional) IPFS http api server endpoint, defaults to http://127.0.0.1:5001/
  - profile - (optional) IPFS stored profile to use.
  - did - (optional) DiD information to use, if storing blocks as verifiable credentials
  - context - (optional) Default @context map to use for ledgers formed with this connection."
  [opts]
  (connect (assoc opts "method" :ipfs)))

(defn connect-memory
  "Forms an in-memory connection using default settings.
  - did - (optional) DiD information to use, if storing blocks as verifiable credentials
  - context - (optional) Default @context map to use for ledgers formed with this connection."
  [opts]
  (connect (assoc opts "method" :memory)))

(defn address?
  "Returns true if the argument is a full ledger address, false if it is just an
  alias."
  [ledger-alias-or-address]
  (str/starts-with? ledger-alias-or-address "fluree:"))

(defn create
  "Creates a new json-ld ledger. A connection (conn) must always be supplied.

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

  Options map (opts) can include (keys are strings):
  - did - DiD information to use, if storing blocks as verifiable credentials
  - defaults
    - @context - default @context map to use for ledgers formed with this connection"
  ([conn] (create conn nil nil))
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (let [opts* (-> opts
                   (util/update-in-if-contains ["defaults"]
                                               util/assoc-from-str-opts
                                               #{{"@context" :context}})
                   (util/assoc-from-str-opts
                    #{"defaults" "did" "branch" "pub-fn" "ipns"
                      "indexer" "include" "reindex-min-bytes" "reindex-max-bytes"
                      "initial-tx"}))]
     (log/debug "create opts*:" opts*)
     (promise-wrap (jld-ledger/create conn ledger-alias opts*)))))

(defn load-from-address
  "Loads a ledger defined with a Fluree address, e.g.:
  fluree:ipfs://Qmaq4ip1bJq6255S5PhU8veo6gxaq2yyucKZmJkV1WW8YG
  fluree:ipns://k51qzi5uqu5dljuijgifuqz9lt1r45lmlnvmu3xzjew9v8oafoqb122jov0mr2
  fluree:ipns://my.dns.com/movies/top-rated
  fluree:file://my/db
  fluree:s3:...."
  ([address]
   ;; TODO - when given an address only, can create or retrieve from cache a conn
   ;; TODO - for that particular method
   (throw (ex-info "Not yet implemented" {:status 500 :error :db/unexpected-error})))
  ([conn address]
   (promise-wrap
     (jld-ledger/load conn address))))

(defn alias->address
  "Returns a core.async channel with the connection-specific address of the
  given ledger-alias."
  [conn ledger-alias]
  (log/debug "Looking up address for ledger alias" ledger-alias)
  (conn-proto/-address conn ledger-alias nil))

(defn load
  "Loads an existing ledger by its alias (which will be converted to a
  connection-specific address first)."
  [conn ledger-alias]
  (promise-wrap
    (go
      (let [address (<! (alias->address conn ledger-alias))]
        (log/debug "Loading ledger from" address)
        (<! (jld-ledger/load conn address))))))

(defn exists?
  "Returns a promise with true if the ledger alias or address exists, false
  otherwise."
  [conn ledger-alias-or-address]
  (promise-wrap
    (go
      (let [address (if (address? ledger-alias-or-address)
                      ledger-alias-or-address
                      (<! (alias->address conn ledger-alias-or-address)))]
        (log/debug "exists? - ledger address:" address)
        (<! (conn-proto/-exists? conn address))))))

(defn default-context
  "Returns the current default context set on the db."
  [db]
  (dbproto/-default-context db))

(defn update-default-context
  "Updates the default context on a given database.
  Currently, the updated default context will only be
  written with a new commit, which requires staging
  changed data.

  Returns an updated db."
  [db default-context]
  (dbproto/-default-context-update db default-context))


(defn index
  "Performs indexing operation on the specified ledger"
  [ledger])


;; MAYBE CHALLENGE?
(defn validate
  "Validates a ledger, checks block integrity along with signatures."
  [])



(defn pull
  "Checks name service for ledger and pulls latest version locally."
  [])



(defn combine
  "Combines multiple ledgers into a new, read-only ledger."
  [])



;; mutations
(defn stage
  "Performs a transaction and queues change if valid (does not commit)"
  ([db json-ld] (stage db json-ld nil))
  ([db json-ld opts]
   (log/debug "stage JSON-LD:" json-ld)
   (log/debug "staging in db:" db "w/ opts:" opts "\n" json-ld)
   (let [result-ch (db-proto/-stage db json-ld opts)]
     (promise-wrap result-ch))))


(defn commit!
  "Commits a staged database to the ledger with all changes since the last commit
  aggregated together.

  Commits are tracked in the local environment, but if the ledger is distributed
  it will still need a 'push' to ensure it is published and verified as per the
  distributed rules."
  ([ledger db]
   (promise-wrap
     (ledger-proto/-commit! ledger db)))
  ([ledger db opts]
   (log/debug "commit! incoming opts:" opts)
   (let [opts* (if (map? opts)
                 (-> opts
                     (util/update-in-if-contains ["did"]
                                                 util/assoc-from-str-opts
                                                 #{"id" "public" "private"})
                     (util/assoc-from-str-opts #{"message" "branch" "tag"
                                                 {"@context" :context}
                                                 "did" "private" "push?"}))
                 opts)]
     (log/debug "commit! decoded opts:" opts*)
     (promise-wrap
       (ledger-proto/-commit! ledger db opts*)))))


(defn status
  "Returns current status of ledger branch."
  ([ledger] (ledger-proto/-status ledger))
  ([ledger branch] (ledger-proto/-status ledger branch)))


(defn push
  "Pushes all commits since last push to a naming service, e.g. a Fluree Network, IPNS, DNS, Fluree Nexus.
  Depending on consensus requirements for a Fluree Network, will accept or reject push as newest update."
  [])



(defn squash
  "Squashes multiple unpublished commits into a single unpublished commit"
  [])



(defn merge
  "Merges changes from one branch into another branch."
  [])



(defn branch
  "Creates a new branch of a given ledger"
  [])


;; db operations

(defn db
  "Retrieves latest db, or optionally a db at a moment in time
  and/or permissioned to a specific identity."
  ([ledger] (db ledger nil))
  ([ledger opts]
   (if opts
     (throw (ex-info "DB opts not yet implemented"
                     {:status 500 :error :db/unexpected-error}))
     ;; TOOD: When db opts are allowed, convert from string keys to keyword keys
     (ledger-proto/-db ledger opts))))


(defn wrap-policy
  "Wraps a db object with specified permission attributes.
  When requesting a db from a ledger, permission attributes can
  be requested at that point, however if one has a db already, this
  allows the permission attributes to be modified.

  Returns promise"
  [db identity-map]
  (promise-wrap
    (perm/wrap-policy db identity-map)))


(defn query
  [db query]
  (promise-wrap (query-api/query db query)))

(defn multi-query
  [db multi-query]
  (-> db
      (query-api/multi-query multi-query)
      promise-wrap))

(defn history
  "Return the change history over a specified time range. Optionally include the commit
  that produced the changes."
  [ledger query]
  (let [latest-db (ledger-proto/-db ledger)
        res-chan  (query-api/history latest-db query)]
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
