(ns fluree.db.json-ld.api
  {:deprecated "3.0"
   :superseded-by "fluree.db"}
  (:require [fluree.db.conn.ipfs :as ipfs-conn]
            [fluree.db.conn.file :as file-conn]
            [fluree.db.conn.memory :as memory-conn]
            [fluree.db.conn.remote :as remote-conn]
            [fluree.json-ld :as json-ld]
            [fluree.db.flake.flake-db :as flake-db]
            #?(:clj [fluree.db.conn.s3 :as s3-conn])
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.platform :as platform]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.db.api.transact :as transact-api]
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger :as ledger]
            [fluree.db.util.log :as log]
            [fluree.db.query.api :as query-api]
            [fluree.db.query.range :as query-range]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.connection :refer [notify-ledger]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy :as policy])
  (:refer-clojure :exclude [merge load range exists?]))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/promise-wrap"}
  promise-wrap
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

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/connect"}
  connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network connection.

  Options include:
    - did - (optional) DId information to use, if storing blocks as verifiable credentials,
            or issuing queries against a permissioned database.
    - write - (optional) Function to use for all writes, if empty will store in memory until a commit is performed
    - read - (optional) Function to use for reads of persisted blocks/data
    - commit - (optional) Function to use to write commits. If persistence desired, this must be defined
    - push - (optional) Function(s) in a vector that will attempt to push the commit to naming service(s)
    "
  [{:keys [method parallelism remote-servers] :as opts}]
  ;; TODO - do some validation
  (log/warn "DEPRECATED function `connect` superseded by `fluree.db.api/connect`")
  (promise-wrap
    (let [opts* (assoc opts :parallelism (or parallelism 4))

          method* (cond
                    method         (keyword method)
                    remote-servers :remote
                    :else          (throw (ex-info (str "No Fluree connection method type specified in configuration: " opts)
                                                   {:status 500 :error :db/invalid-configuration})))]
      (case method*
        :remote (remote-conn/connect opts*)
        :ipfs   (ipfs-conn/connect opts*)
        :file   (if platform/BROWSER
                  (throw (ex-info "File connection not supported in the browser" opts))
                  (file-conn/connect opts*))
        :memory (memory-conn/connect opts*)
        :s3     #?(:clj  (s3-conn/connect opts*)
                   :cljs (throw (ex-info "S3 connections not yet supported in ClojureScript"
                                         {:status 400, :error :db/unsupported-operation})))))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/connect-file"}
  connect-file
  [opts]
  (log/warn "DEPRECATED function `connect-file` superseded by `fluree.db.api/connect-file`")
  (connect (assoc opts :method :file)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/connect-ipfs"}
  connect-ipfs
  "Forms an ipfs connection using default settings.
  - server - (optional) IPFS http api server endpoint, defaults to http://127.0.0.1:5001/
  - profile - (optional) IPFS stored profile to use.
  - did - (optional) DId information to use, if storing blocks as verifiable credentials"
  [opts]
  (log/warn "DEPRECATED function `connect-ipfs` superseded by `fluree.db.api/connect-ipfs`")
  (connect (assoc opts :method :ipfs)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/connect-memory"}
  connect-memory
  "Forms an in-memory connection using default settings.
  - did - (optional) DId information to use, if storing blocks as verifiable credentials"
  [opts]
  (log/warn "DEPRECATED function `connect-memory` superseded by `fluree.db.api/connect-memory`")
  (connect (assoc opts :method :memory)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/address?"}
  address?
  "Returns true if the argument is a full ledger address, false if it is just an
  alias."
  [ledger-alias-or-address]
  (log/warn "DEPRECATED function `address?` superseded by `fluree.db.api/address?`")
  (jld-ledger/fluree-address? ledger-alias-or-address))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/create"}
  create
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
  - did - DId information to use, if storing blocks as verifiable credentials"
  ([conn] (create conn nil nil))
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (log/warn "DEPRECATED function `create` superseded by `fluree.db.api/create`")
   (promise-wrap
    (do
      (log/info "Creating ledger" ledger-alias)
      (jld-ledger/create conn ledger-alias opts)))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/alias->address"}
  alias->address
  "Returns a core.async channel with the connection-specific address of the
  given ledger-alias."
  [conn ledger-alias]
  (log/warn "DEPRECATED function `alias->address` superseded by `fluree.db.api/alias->address`")
  (log/debug "Looking up address for ledger alias" ledger-alias)
  (nameservice/primary-address conn ledger-alias nil))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/load"}
  load
  "Loads an existing ledger by its alias (which will be converted to a
  connection-specific address first)."
  [conn alias-or-address]
  (log/warn "DEPRECATED function `load` superseded by `fluree.db.api/load`")
  (promise-wrap
    (jld-ledger/load conn alias-or-address)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/exists?"}
  exists?
  "Returns a promise with true if the ledger alias or address exists, false
  otherwise."
  [conn ledger-alias-or-address]
  (log/warn "DEPRECATED function `exists?` superseded by `fluree.db.api/exists?`")
  (promise-wrap
    (go
      (let [address (if (address? ledger-alias-or-address)
                      ledger-alias-or-address
                      (<! (alias->address conn ledger-alias-or-address)))]
        (log/debug "exists? - ledger address:" address)
        (<! (nameservice/exists? conn address))))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/notify"}
  notify
  "Notifies the connection with a new commit map (parsed JSON commit with string keys).

  If the connection knows of the ledger, and is currently maintaining
  an in-memory version of the ledger, will attempt to update the db if the commit
  is for the next 't' value. If a commit is for a past 't' value, noop.
  If commit is for a future 't' value, will drop in-memory ledger for reload upon next request."
  [conn commit-map]
  (log/warn "DEPRECATED function `notify` superseded by `fluree.db.api/notify`")
  (promise-wrap
    (if (map? commit-map)
      (notify-ledger conn commit-map)
      (go
        (ex-info (str "Invalid commit map, perhaps it is JSON that needs to be parsed first?: " commit-map)
                 {:status 400 :error :db/invalid-commit-map})))))


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/index"}
  index
  "Performs indexing operation on the specified ledger"
  [ledger])


;; MAYBE CHALLENGE?
(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/validate"}
  validate
  "Validates a ledger, checks block integrity along with signatures."
  [])


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/pull"}
  pull
  "Checks name service for ledger and pulls latest version locally."
  [])


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/combine"}
  combine
  "Combines multiple ledgers into a new, read-only ledger."
  [])


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/stage"}
  stage
  "Performs a transaction and queues change if valid (does not commit)"
  ([db json-ld] (stage db json-ld nil))
  ([db json-ld opts]
   (log/warn "DEPRECATED function `stage` superseded by `fluree.db.api/stage`")
   (let [result-ch (transact-api/stage db json-ld opts)]
     (promise-wrap result-ch))))


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/commit!"}
  commit!
  "Commits a staged database to the ledger with all changes since the last commit
  aggregated together.

  Commits are tracked in the local environment, but if the ledger is distributed
  it will still need a 'push' to ensure it is published and verified as per the
  distributed rules."
  ([ledger db]
   (promise-wrap
     (ledger/-commit! ledger db)))
  ([ledger db opts]
   (log/warn "DEPRECATED function `commit!` superseded by `fluree.db.api/commit!`")
   (promise-wrap
     (ledger/-commit! ledger db opts))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/transact!"}
  transact!
  [conn txn]
  (log/warn "DEPRECATED function `transact!` superseded by `fluree.db.api/transact!`")
  (promise-wrap
    (transact-api/transact! conn txn)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/create-with-txn"}
  create-with-txn
  [conn txn]
  (log/warn "DEPRECATED function `create-with-txn` superseded by `fluree.db.api/create-with-txn`")
  (promise-wrap
    (transact-api/create-with-txn conn txn)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/status"}
  status
  "Returns current status of ledger branch."
  ([ledger] (ledger/-status ledger))
  ([ledger branch]
   (log/warn "DEPRECATED function `status` superseded by `fluree.db.api/status`")
   (ledger/-status ledger branch)))


(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/push"}
  push
  "Pushes all commits since last push to a name service, e.g. a Fluree Network, IPNS, DNS, Fluree Nexus.
  Depending on consensus requirements for a Fluree Network, will accept or reject push as newest update."
  [])



(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/squash"}
  squash
  "Squashes multiple unpublished commits into a single unpublished commit"
  [])



(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/merge"}
  merge
  "Merges changes from one branch into another branch."
  [])



(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/branch"}
  branch
  "Creates a new branch of a given ledger"
  [])


;; db operations

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/db"}
  db
  "Retrieves latest db, or optionally a db at a moment in time
  and/or permissioned to a specific identity."
  ([ledger]
   (db ledger nil))
  ([ledger opts]
   (log/warn "DEPRECATED function `db` superseded by `fluree.db.api/db`")
   (if opts
     (throw (ex-info "DB opts not yet implemented"
                     {:status 500 :error :db/unexpected-error}))
     (ledger/-db ledger))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/wrap-policy"}
  wrap-policy
  ([db policy]
   (wrap-policy db policy nil))
  ([db policy values-map]
   (log/warn "DEPRECATED function `wrap-policy` superseded by `fluree.db.api/wrap-policy`")
   (promise-wrap
    (policy/wrap-policy db policy values-map))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/wrap-identity-policy"}
  wrap-identity-policy
  "For provided identity, locates specific property f:policyClass on
  the identity containing a list of class IRIs that identity the policies
  that should be applied to the identity.

  With the policy classes, finds all policies containing that class
  declaration."
  ([db identity]
   (wrap-identity-policy db identity nil))
  ([db identity values-map]
   (log/warn "DEPRECATED function `wrap-identity-policy` superseded by `fluree.db.api/wrap-identity-policy`")
   (promise-wrap
    (policy/wrap-identity-policy db identity values-map))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/dataset"}
  dataset
  "Creates a composed dataset from multiple resolved graph databases.

  The databases to be composed are supplied as a map with a string alias
  as they key, and the resolved graph db as the value.

  By default, every resolved graph db will be composed together as a new
  default graph which will be used for all where clauses in queries that
  do *not* specify a specific graph to target, which is done using the
  special `graph` syntax in the where clause.

  If just one or more of the supplied graph dbs should instead be used as
  the default graph (instead of all of them), supply the second argument
  as a list of the db aliases in the db-map that should be used as the
  default.

  Targeting a single named graph in a query (as opposed to the default graph)
  is done by using the `graph` syntax within the 'where' clause, for example:
  {...
   'where': [...
             ['graph' <graph-alias> <query-pattern>]]
   ...}"
  ([named-graphs] (dataset named-graphs (keys named-graphs)))
  ([named-graphs default-graphs]
   (log/warn "DEPRECATED function `dataset` superseded by `fluree.db.api/dataset`")
   (query-api/dataset named-graphs default-graphs)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/query"}
  query
  "Queries a dataset or single db and returns a promise with the results."
  ([ds q] (query ds q {}))
  ([ds q opts]
   (log/warn "DEPRECATED function `query` superseded by `fluree.db.api/query`")
   (promise-wrap (query-api/query ds q opts))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/credential-query"}
  credential-query
  "Issues a policy-enforced query to the specified dataset/db as a verifiable
  credential.

  Extracts the query from the credential, and cryptographically verifies the
  signing identity, which is then used by `wrap-identity-policy` to extract
  the policy classes and apply the policies to the query."
  ([ds cred-query] (credential-query ds cred-query {}))
  ([ds cred-query {:keys [values-map] :as opts}]
   (log/warn "DEPRECATED function `credential-query` superseded by `fluree.db.api/credential-query`")
   (promise-wrap
    (go-try
      (let [{query :subject, identity :did} (<? (cred/verify cred-query))]
       (log/debug "Credential query with identity: " identity " and query: " query)
       (cond
         (and query identity)
         (let [policy-db (<? (policy/wrap-identity-policy ds identity values-map))]
           (<? (query-api/query policy-db query opts)))

         identity
         (throw (ex-info "Query not present in credential"
                         {:status 400 :error :db/invalid-credential}))

         :else
         (throw (ex-info "Invalid credential"
                         {:status 400 :error :db/invalid-credential}))))))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/query-connection"}
  query-connection
  "Queries the latest db in the ledger specified by the 'from' parameter in the
  query (what that actually looks like is format-specific). Returns a promise
  with the results."
  ([conn q] (query-connection conn q {}))
  ([conn q opts]
   (log/warn "DEPRECATED function `query-connection` superseded by `fluree.db.api/query-connection`")
   (promise-wrap (query-api/query-connection conn q opts))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/history"}
  history
  "Return the change history over a specified time range. Optionally include the commit
  that produced the changes."
  ([ledger query]
   (let [latest-db (ledger/-db ledger)
         res-chan  (query-api/history latest-db query)]
     (promise-wrap res-chan)))
  ([ledger query {:keys [policy identity values-map] :as _opts}]
   (log/warn "DEPRECATED function `history` superseded by `fluree.db.api/history`")
   (promise-wrap
     (let [latest-db (ledger/-db ledger)
           policy-db (if identity
                       (<? (policy/wrap-identity-policy latest-db identity values-map))
                       (<? (policy/wrap-policy latest-db policy values-map)))]
      (query-api/history policy-db query)))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/credential-history"}
  credential-history
  "Issues a policy-enforced history query to the specified ledger as a
  verifiable credential.

  Extracts the query from the credential, and cryptographically verifies the
  signing identity, which is then used by `wrap-identity-policy` to extract
  the policy classes and apply the policies to the query."
  ([ledger cred-query] (credential-history ledger cred-query {}))
  ([ledger cred-query {:keys [values-map] :as opts}]
   (log/warn "DEPRECATED function `credential-history` superseded by `fluree.db.api/credential-history`")
   (promise-wrap
    (go-try
      (let [latest-db                       (ledger/-db ledger)
            {query :subject, identity :did} (<? (cred/verify cred-query))]
       (log/debug "Credential history query with identity: " identity " and query: " query)
       (cond
         (and query identity)
         (let [policy-db (<? (policy/wrap-identity-policy latest-db identity values-map))]
           (<? (query-api/history policy-db query)))

         identity
         (throw (ex-info "Query not present in credential"
                         {:status 400 :error :db/invalid-credential}))

         :else
         (throw (ex-info "Invalid credential"
                         {:status 400 :error :db/invalid-credential}))))))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/range"}
  range
  "Performs a range scan against the specified index using test functions
  of >=, <=, >, <"
  ;; TODO - assert index is valid index type
  ([db index test match]
   (promise-wrap
     (query-range/index-range db index test match)))
  ([db index start-test start-match end-test end-match]
   (log/warn "DEPRECATED function `range` superseded by `fluree.db.api/range`")
   (promise-wrap
     (query-range/index-range db index start-test start-match end-test end-match))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/slice"}
  slice
  "Like range, but returns all flakes that match the supplied flake parts."
  [db index match]
  (log/warn "DEPRECATED function `slice` superseded by `fluree.db.api/slice`")
  (promise-wrap
    (query-range/index-range db index = match)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/expand-iri"}
  expand-iri
  "Expands given IRI with the default database context, or provided context."
  ([context compact-iri]
   (log/warn "DEPRECATED function `expand-iri` superseded by `fluree.db.api/expand-iri`")
   (json-ld/expand-iri compact-iri
                       (json-ld/parse-context context))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/encode-iri"}
  encode-iri
  "Returns the internal Fluree IRI identifier (a compact form).
  This can be used for doing range scans, slices and for other
  more advanced needs."
  [db iri]
  (log/warn "DEPRECATED function `encode-iri` superseded by `fluree.db.api/encode-iri`")
  (iri/encode-iri db iri))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/internal-id"}
  internal-id
  "Deprecated, use encode-iri instead."
  {:deprecated true}
  [db iri]
  (do
    (println "WARNING: (internal-id db iri) is deprecated, use (encode-iri db iri).")
    (encode-iri db iri)))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/decode-iri"}
  decode-iri
  "Opposite of encode-iri. When doing more advanced features
  like direct range-scans of indexes, IRIs are returned in their
  internal compact format. This allows the IRI to be returned
  as a full string IRI."
  [db iri]
  (log/warn "DEPRECATED function `decode-iri` superseded by `fluree.db.api/decode-iri`")
  (iri/decode-sid db iri))

;; reasoning APIs

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/reason"}
  reason
  "Sets the reasoner methods(s) to perform on a db.
  Supported methods are :datalog and :owl2rl.
  One or more methods can be supplied as a sequential list/vector.

  Reasoning is done in-memory at the db-level and is not persisted.

  A rules graph containing rules in JSON-LD format can be supplied,
  or if no rules graph is supplied, the rules will be looked for within
  the db itself."
  ([db methods] (reason db methods nil nil))
  ([db methods rule-sources] (reason db methods rule-sources nil))
  ([db methods rule-sources opts]
   (log/warn "DEPRECATED function `reason` superseded by `fluree.db.api/reason`")
   (promise-wrap
     (reasoner/reason db methods rule-sources opts))))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/reasoned-count"}
  reasoned-count
  "Returns a count of reasoned facts in the provided db."
  [db]
  (log/warn "DEPRECATED function `reasoned-count` superseded by `fluree.db.api/reasoned-count`")
  (reasoner/reasoned-count db))

(defn ^{:deprecated    "3.0"
        :superseded-by "fluree.db/reasoned-facts"}
  reasoned-facts
  "Returns all reasoned facts in the provided db as  4-tuples of:
  [subject property object rule-iri]
  where the rule-iri is the @id of the rule that generated the fact

  Returns 4-tuples of  where
  the rule-iri is the @id of the rule that generated the fact.

  NOTE: Currently returns internal fluree ids for subject, property and object.
  This will be changed to return IRIs in a future release.

  Optional opts map specified grouping, or no grouping (default):
  {:group-by :rule} - group by rule IRI
  {:group-by :subject} - group by the reasoned triples' subject
  {:group-by ::property} - group by the reasoned triples' property IRI"
  ([db] (reasoned-facts db nil))
  ([db opts]
   (log/warn "DEPRECATED function `reasoned-facts` superseded by `fluree.db.api/reasoned-facts`")
   (reasoner/reasoned-facts (:group-by opts))))
