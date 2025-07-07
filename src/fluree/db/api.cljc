(ns fluree.db.api
  (:require [camel-snake-kebab.core :refer [->camelCaseString]]
            [clojure.core.async :as async :refer [go <!]]
            [clojure.walk :refer [postwalk]]
            [fluree.db.api.transact :as transact-api]
            [fluree.db.connection :as connection :refer [connection?]]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.ledger :as ledger]
            [fluree.db.query.api :as query-api]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.range :as query-range]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld])
  (:refer-clojure :exclude [merge load range exists? update drop]))

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

(defn- validate-connection
  "Throws exception if x is not a valid connection"
  [x]
  (when-not (connection? x)
    (throw (ex-info "Unable to create new ledger, connection is not valid. fluree/connect returns a promise, did you deref it?"
                    {:status 400 :error :db/invalid-connection}))))

(defn connect
  "Forms connection to ledger, enabling automatic pulls of new updates, event
  services, index service.

  Multiple connections to same endpoint will share underlying network
  connection.

  Options include:
    - did - (optional) DId information to use, if storing blocks as verifiable
            credentials, or issuing queries against a permissioned database."
  [config]
  ;; TODO - do some validation
  (promise-wrap
   (go-try
     (let [system-map (-> config config/parse system/initialize)
           conn       (reduce-kv (fn [x k v]
                                   (if (isa? k :fluree.db/connection)
                                     (reduced v)
                                     x))
                                 nil system-map)]
       (assoc conn ::system-map system-map)))))

(defn disconnect
  [conn]
  (promise-wrap
   (go-try
     (-> conn ::system-map system/terminate))))

(defn convert-config-key
  [[k v]]
  (if (#{:id :type} k)
    [(str "@" (name k)) v]
    (if (#{:public :private} k)
      [(-> k name (str "Key")) v]
      [(->camelCaseString k) v])))

(defn convert-keys
  [m]
  (postwalk (fn [x]
              (if (map? x)
                (into {} (map convert-config-key) x)
                x))
            m))

(defn connect-memory
  "Forms an in-memory connection using default settings.
  - did - (optional) DId information to use, if storing blocks as verifiable credentials"
  ([]
   (connect-memory {}))
  ([{:keys [parallelism cache-max-mb defaults],
     :or   {parallelism 4, cache-max-mb 1000}}]
   (let [memory-config (cond-> {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                            "@vocab" "https://ns.flur.ee/system#"}
                                "@id"      "memory"
                                "@graph"   [{"@id"   "memoryStorage"
                                             "@type" "Storage"}
                                            {"@id"              "connection"
                                             "@type"            "Connection"
                                             "parallelism"      parallelism
                                             "cacheMaxMb"       cache-max-mb
                                             "commitStorage"    {"@id" "memoryStorage"}
                                             "indexStorage"     {"@id" "memoryStorage"}
                                             "primaryPublisher" {"@type"   "Publisher"
                                                                 "storage" {"@id" "memoryStorage"}}}]}
                         defaults (assoc-in ["@graph" 1 "defaults"] (convert-keys defaults)))]
     (connect memory-config))))

(defn connect-file
  ([]
   (connect-file {}))
  ([{:keys [storage-path parallelism cache-max-mb defaults],
     :or   {storage-path "data", parallelism 4, cache-max-mb 1000}}]
   (let [file-config (cond-> {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                          "@vocab" "https://ns.flur.ee/system#"}
                              "@id"      "file"
                              "@graph"   [{"@id"      "fileStorage"
                                           "@type"    "Storage"
                                           "filePath" storage-path}
                                          {"@id"              "connection"
                                           "@type"            "Connection"
                                           "parallelism"      parallelism
                                           "cacheMaxMb"       cache-max-mb
                                           "commitStorage"    {"@id" "fileStorage"}
                                           "indexStorage"     {"@id" "fileStorage"}
                                           "primaryPublisher" {"@type"   "Publisher"
                                                               "storage" {"@id" "fileStorage"}}}]}
                       defaults (assoc-in ["@graph" 1 "defaults"] (convert-keys defaults)))]
     (connect file-config))))

#?(:clj
   (defn connect-s3
     "Forms a connection backed by S3 storage.
     
     Options:
       - s3-bucket (required): The S3 bucket name
       - s3-endpoint (required): S3 endpoint URL
         * For AWS S3: 'https://s3.<region>.amazonaws.com' (e.g., 'https://s3.us-east-1.amazonaws.com')
         * For LocalStack: 'http://localhost:4566'
         * For MinIO: 'http://localhost:9000'
       - s3-prefix (optional): The prefix within the bucket
       - parallelism (optional): Number of parallel operations (default: 4)
       - cache-max-mb (optional): Maximum memory for caching in MB (default: 1000)
       - defaults (optional): Default options for ledgers created with this connection"
     ([{:keys [s3-bucket s3-prefix s3-endpoint parallelism cache-max-mb defaults],
        :or   {parallelism 4, cache-max-mb 1000}}]
      (when-not s3-bucket
        (throw (ex-info "S3 bucket name is required for S3 connection"
                        {:status 400 :error :db/invalid-config})))
      (when-not s3-endpoint
        (throw (ex-info "S3 endpoint is required for S3 connection. Examples: 'https://s3.us-east-1.amazonaws.com' for AWS, 'http://localhost:4566' for LocalStack"
                        {:status 400 :error :db/invalid-config})))
      (let [s3-config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                           "@vocab" "https://ns.flur.ee/system#"}
                               "@id"      "s3"
                               "@graph"   [(cond-> {"@id"        "s3Storage"
                                                    "@type"      "Storage"
                                                    "s3Bucket"   s3-bucket
                                                    "s3Endpoint" s3-endpoint}
                                             s3-prefix (assoc "s3Prefix" s3-prefix))
                                           (cond-> {"@id"              "connection"
                                                    "@type"            "Connection"
                                                    "parallelism"      parallelism
                                                    "cacheMaxMb"       cache-max-mb
                                                    "commitStorage"    {"@id" "s3Storage"}
                                                    "indexStorage"     {"@id" "s3Storage"}
                                                    "primaryPublisher" {"@type"   "Publisher"
                                                                        "storage" {"@id" "s3Storage"}}}
                                             defaults (assoc "defaults" (convert-keys defaults)))]}]
        (connect s3-config)))))

(defn address?
  "Returns true if the argument is a full ledger address, false if it is just an
  alias."
  [ledger-alias-or-address]
  (connection/fluree-address? ledger-alias-or-address))

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
  - did - DId information to use, if storing blocks as verifiable credentials"
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (validate-connection conn)
   (promise-wrap
    (do
      (log/info "Creating ledger" ledger-alias)
      (connection/create-ledger conn ledger-alias opts)))))

(defn alias->address
  "Returns a core.async channel with the connection-specific address of the
  given ledger-alias."
  [conn ledger-alias]
  (validate-connection conn)
  (connection/primary-address conn ledger-alias))

(defn load
  "Loads an existing ledger by its alias (which will be converted to a
  connection-specific address first)."
  [conn alias-or-address]
  (validate-connection conn)
  (promise-wrap
   (connection/load-ledger conn alias-or-address)))

(defn drop
  [conn ledger-alias]
  (promise-wrap
   (connection/drop-ledger conn ledger-alias)))

(defn exists?
  "Returns a promise with true if the ledger alias or address exists, false
  otherwise."
  [conn ledger-alias-or-address]
  (validate-connection conn)
  (promise-wrap
   (go-try
     (let [address (if (address? ledger-alias-or-address)
                     ledger-alias-or-address
                     (<? (alias->address conn ledger-alias-or-address)))]
       (log/debug "exists? - ledger address:" address)
       (<? (connection/ledger-exists? conn address))))))

(defn notify
  "Notifies the connection of a new commit stored at address `commit-address`.

  If the connection knows of the ledger, and is currently maintaining an
  in-memory version of the ledger, will attempt to update the db if the commit
  is for the next 't' value. If a commit is for a past 't' value, noop. If
  commit is for a future 't' value, will drop in-memory ledger for reload upon
  next request."
  [conn commit-address commit-hash]
  (validate-connection conn)
  (promise-wrap
   (connection/notify conn commit-address commit-hash)))

(defn insert
  "Inserts a new set of data into the database if valid (does not commit).
   Multiple inserts and updates can be staged together and will be merged into a single
   transaction when committed.

   Supports JSON-LD (default) and Turtle (TTL) formats.

   The 'opts' key is a map with the following key options:
    - `:context` - (optional) and externally provided context that will be used
                   for JSON-LD document expansition, the @context in the json-ld
                   will be ignored if present.
    - `:format`  - (optional) the format of the data, currently json-ld is assumed
                  unless `:format` is set to `:turtle`. If `:turtle` is set,
                  the `:context` option will be ignored if provided."
  ([db json-ld] (insert db json-ld nil))
  ([db json-ld opts]
   (promise-wrap
    (transact-api/insert db json-ld opts))))

(defn upsert
  "Performs an upsert operation, which will insert the data if it does not exist,
   or update the existing data if it does. This is useful for ensuring that a
   specific document is present in the database with the desired values.

   Supports JSON-LD and Turtle (TTL) formats.

   The 'opts' key is a map with the following key options:
    - `:context` - (optional) and externally provided context that will be used
                   for document expansion, the @context in the json-ld will be
                   ignored if present.
   - `:format`  - (optional) the format of the data, currently json-ld is assumed
                  unless `:format` is set to `:turtle`. If `:turtle` is set,
                  the `:context` option will be ignored if provided.

   The data is expected to be in JSON-LD format, and will be expanded before
   being inserted into the database."
  ([db json-ld] (upsert db json-ld nil))
  ([db json-ld opts]
   (promise-wrap
    (transact-api/upsert db json-ld opts))))

(defn update
  "Performs an update and queues change if valid (does not commit).
   Multiple updates can be staged together and will be merged into a single
   transaction when committed."
  ([db json-ld] (update db json-ld nil))
  ([db json-ld opts]
   (promise-wrap
    (transact-api/update db json-ld opts))))

;; TODO - deprecate `stage` in favor of `update` eventually
(defn stage
  "Renamed to `update`, prefer that API instead."
  ([db json-ld] (update db json-ld nil))
  ([db json-ld opts] (update db json-ld opts)))

(defn format-txn
  "Reformats the transaction `txn` as JSON-QL if it is formatted as SPARQL,
  returning it unchanged otherwise."
  [txn override-opts]
  (parse/parse-sparql txn override-opts))

(defn commit!
  "Commits a staged database to the ledger with all changes since the last commit
  aggregated together.

  Commits are tracked in the local environment, but if the ledger is distributed
  it will still need a 'push' to ensure it is published and verified as per the
  distributed rules."
  ([ledger db]
   (promise-wrap
    (transact/commit! ledger db {})))
  ([ledger db opts]
   (promise-wrap
    (transact/commit! ledger db opts))))

(defn transact!
  "Stages the given transaction with update semantics and then commits."
  ([conn txn] (transact! conn txn nil))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/update! conn txn opts))))

(defn update!
  "Stages the given transaction with update semantics and then commits."
  ([conn txn] (update! conn txn nil))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/update! conn txn opts))))

(defn upsert!
  "Stages the given transaction with upsert semantics and then commits."
  ([conn ledger-id txn] (upsert! conn ledger-id txn nil))
  ([conn ledger-id txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/upsert! conn ledger-id txn opts))))

(defn insert!
  "Stages the given transaction with insert semantics and then commits."
  ([conn ledger-id txn] (insert! conn ledger-id txn nil))
  ([conn ledger-id txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/insert! conn ledger-id txn opts))))

(defn credential-transact!
  ([conn txn] (credential-transact! conn txn nil))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/credential-transact! conn txn opts))))

(defn create-with-txn
  ([conn txn]
   (validate-connection conn)
   (promise-wrap
    (transact-api/create-with-txn conn txn)))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/create-with-txn conn txn opts))))

(defn status
  "Returns current status of ledger branch."
  ([ledger] (ledger/status ledger))
  ([ledger branch] (ledger/status ledger branch)))

;; db operations

(defn db
  "Retrieves latest db, or optionally a db at a moment in time
  and/or permissioned to a specific identity."
  [ledger]
  (ledger/current-db ledger))

(defn wrap-policy
  "Restricts the provided db with the provided json-ld
  policy restrictions"
  ([db policy]
   (wrap-policy db policy nil))
  ([db policy policy-values]
   (promise-wrap
    (let [policy* (json-ld/expand policy)]
      (policy/wrap-policy db policy* policy-values)))))

(defn wrap-class-policy
  "Restricts the provided db with policies in the db
  which have a class @type of the provided class(es)."
  ([db policy-classes]
   (wrap-class-policy db policy-classes nil))
  ([db policy-classes policy-values]
   (promise-wrap
    (policy/wrap-class-policy db nil policy-classes policy-values))))

(defn wrap-identity-policy
  "For provided identity, locates specific property f:policyClass on
  the identity containing a list of class IRIs that identity the policies
  that should be applied to the identity.

  With the policy classes, finds all policies containing that class
  declaration."
  ([db identity]
   (wrap-identity-policy db identity nil))
  ([db identity policy-values]
   (promise-wrap
    (policy/wrap-identity-policy db nil identity policy-values))))

(defn dataset
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
   (query-api/dataset named-graphs default-graphs)))

(defn query
  "Queries a dataset or single db and returns a promise with the results."
  ([ds q]
   (query ds q {}))
  ([ds q opts]
   (if (util/exception? ds)
     (throw ds)
     (promise-wrap (query-api/query ds q opts)))))

(defn credential-query
  "Issues a policy-enforced query to the specified dataset/db as a verifiable
  credential.

  Extracts the query from the credential, and cryptographically verifies the
  signing identity, which is then used by `wrap-identity-policy` to extract
  the policy classes and apply the policies to the query."
  ([ds cred-query] (credential-query ds cred-query {}))
  ([ds cred-query {:keys [values-map format] :as opts}]
   (promise-wrap
    (go-try
      (let [{query :subject, identity :did} (if (= :sparql format)
                                              (cred/verify-jws cred-query)
                                              (<? (cred/verify cred-query)))]
        (log/debug "Credential query with identity: " identity " and query: " query)
        (let [policy-db (<? (policy/wrap-identity-policy ds nil identity values-map))]
          (<? (query-api/query policy-db query opts))))))))

(defn query-connection
  "Queries the latest db in the ledger specified by the 'from' parameter in the
  query (what that actually looks like is format-specific). Returns a promise
  with the results."
  ([conn q] (query-connection conn q {}))
  ([conn q opts]
   (validate-connection conn)
   (promise-wrap (query-api/query-connection conn q opts))))

(defn credential-query-connection
  ([conn cred-query] (credential-query-connection conn cred-query {}))
  ([conn cred-query {:keys [format] :as opts}]
   (validate-connection conn)
   (promise-wrap
    (go-try
      (let [{query :subject, identity :did} (if (= :sparql format)
                                              (cred/verify-jws cred-query)
                                              (<? (cred/verify cred-query)))]
        (log/debug "Credential query connection with identity: " identity " and query: " query)
        @(query-connection conn query (assoc opts :identity identity)))))))

(defn history
  "Return the change history over a specified time range. Optionally include the commit
  that produced the changes."
  ([ledger query]
   (history ledger query nil))
  ([ledger query override-opts]
   (promise-wrap
    (query-api/history ledger query override-opts))))

(defn credential-history
  "Issues a policy-enforced history query to the specified ledger as a
  verifiable credential.

  Extracts the query from the credential, and cryptographically verifies the
  signing identity, which is then used by `wrap-identity-policy` to extract
  the policy classes and apply the policies to the query."
  ([ledger cred-query] (credential-history ledger cred-query {}))
  ([ledger cred-query override-opts]
   (promise-wrap
    (go-try
      (let [{query :subject, identity :did} (<? (cred/verify cred-query))]
        (<? (query-api/history ledger query (assoc override-opts :identity identity))))))))

(defn range
  "Performs a range scan against the specified index using test functions
  of >=, <=, >, <"
  ;; TODO - assert index is valid index type
  ([db index test match]
   (promise-wrap
    (query-range/index-range db index test match)))
  ([db index start-test start-match end-test end-match]
   (promise-wrap
    (query-range/index-range db nil index start-test start-match end-test end-match))))

(defn slice
  "Like range, but returns all flakes that match the supplied flake parts."
  [db index match]
  (promise-wrap
   (query-range/index-range db index = match)))

(defn expand-iri
  "Expands given IRI with the default database context, or provided context."
  ([context compact-iri]
   (json-ld/expand-iri compact-iri
                       (json-ld/parse-context context))))

(defn encode-iri
  "Returns the internal Fluree IRI identifier (a compact form).
  This can be used for doing range scans, slices and for other
  more advanced needs."
  [db iri]
  (iri/encode-iri db iri))

(defn decode-iri
  "Opposite of encode-iri. When doing more advanced features
  like direct range-scans of indexes, IRIs are returned in their
  internal compact format. This allows the IRI to be returned
  as a full string IRI."
  [db iri]
  (iri/decode-sid db iri))

;; reasoning APIs

(defn reason
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
   (promise-wrap
    (reasoner/reason db methods rule-sources opts))))

(defn reasoned-count
  "Returns a count of reasoned facts in the provided db."
  [db]
  (reasoner/reasoned-count db))

(defn reasoned-facts
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
   (let [grouping (:group-by opts)]
     (reasoner/reasoned-facts db grouping))))
