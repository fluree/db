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
            [fluree.db.nameservice.query :as ns-query]
            [fluree.db.query.api :as query-api]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.range :as query-range]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [go-try <?]]
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
  "Creates a connection from a JSON-LD configuration map.

  Config should contain @graph with storage, connection, and optional system definitions.
  Returns a promise that resolves to a connection object.

  See documentation for configuration schema."
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
  "Terminates a connection and releases associated resources.
  Returns a promise that resolves when disconnection is complete."
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
  "Creates an in-memory connection with default settings.

  Options map (all optional):
    :parallelism - Number of parallel operations (default: 4)
    :cache-max-mb - Maximum cache size in MB (default: 1000)
    :defaults - Default settings map for operations"
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
  "Forms a connection backed by local file storage.
  
  Options:
    - storage-path (optional): Directory path for file storage (default: \"data\")
    - parallelism (optional): Number of parallel operations (default: 4)
    - cache-max-mb (optional): Maximum memory for caching in MB (default: 1000)
    - aes256-key (optional): AES-256 encryption key for file storage encryption.
      When provided, all data will be encrypted using AES-256-CBC with PKCS5 padding.
      The key should be exactly 32 bytes long for optimal security.
      Example: \"my-secret-32-byte-encryption-key!\"
    - defaults (optional): Default options for ledgers created with this connection
  
  Returns a core.async channel that resolves to a connection, or an exception if
  the connection cannot be established.
  
  Examples:
    ;; Basic file storage
    (connect-file {:storage-path \"./my-data\"})
    
    ;; File storage with encryption
    (connect-file {:storage-path \"./secure-data\"
                   :aes256-key \"my-secret-32-byte-encryption-key!\"})
                   
    ;; Full configuration
    (connect-file {:storage-path \"./data\"
                   :parallelism 8
                   :cache-max-mb 2000
                   :aes256-key \"my-secret-32-byte-encryption-key!\"})"
  ([]
   (connect-file {}))
  ([{:keys [storage-path parallelism cache-max-mb defaults aes256-key],
     :or   {storage-path "data", parallelism 4, cache-max-mb 1000}}]
   (let [file-config (cond-> {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                          "@vocab" "https://ns.flur.ee/system#"}
                              "@id"      "file"
                              "@graph"   [(cond-> {"@id"      "fileStorage"
                                                   "@type"    "Storage"
                                                   "filePath" storage-path}
                                            aes256-key (assoc "AES256Key" aes256-key))
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
  "Creates a new ledger with an initial empty commit at t=0.

  Parameters:
    conn - Connection object
    ledger-alias - Unique alias/name for the ledger
    opts - (optional) Options map:
      :branch - Branch name (default: \"main\")
      :did - DID for signing commits
      :context - Default JSON-LD context
      :indexing - Indexing configuration"
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (validate-connection conn)
   (promise-wrap
    (do
      (log/info "Creating ledger" ledger-alias)
      (connection/create-ledger conn ledger-alias opts)))))

(defn alias->address
  "Resolves a ledger alias to its address.

  Returns a core.async channel containing the address."
  [conn ledger-alias]
  (validate-connection conn)
  (connection/primary-address conn ledger-alias))

(defn load
  "Loads an existing ledger by alias or address.
  Returns a promise that resolves to the ledger object."
  [conn alias-or-address]
  (validate-connection conn)
  (promise-wrap
   (connection/load-ledger conn alias-or-address)))

(defn drop
  "Deletes a ledger and its associated data.
  Returns a promise that resolves when deletion is complete."
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
  "Notifies the connection of a new commit for maintaining current db state.

  Parameters:
    conn - Connection object
    commit-address - Address where commit is stored
    commit-hash - Hash of the commit

  Updates in-memory ledger if commit is next in sequence.
  Returns promise resolving when notification is processed."
  [conn commit-address commit-hash]
  (validate-connection conn)
  (promise-wrap
   (connection/notify conn commit-address commit-hash)))

(defn insert
  "Stages insertion of new entities into a database.

  Parameters:
    db - Database value
    rdf - JSON-LD or Turtle RDF data to insert
    opts - (optional) Options map:
      :context - Context override (ignored if present in json-ld)
      :format - Data format (:json-ld default, or :turtle)

  Throws exception if any entity @id already exists.
  For insert-or-update behavior, use `upsert`.

  Returns promise resolving to updated database."
  ([db rdf] (insert db rdf nil))
  ([db rdf opts]
   (promise-wrap
    (transact-api/insert db rdf opts))))

(defn upsert
  "Stages insertion or update of entities.

  Parameters:
    db - Database value
    rdf - JSON-LD or Turtle RDF data to upsert
    opts - (optional) Options map:
      :context - Context override (ignored if present in json-ld)
      :format - Data format (:json-ld default, or :turtle)

  Creates new entities or updates existing ones based on @id.

  Returns promise resolving to updated database."
  ([db rdf] (upsert db rdf nil))
  ([db rdf opts]
   (promise-wrap
    (transact-api/upsert db rdf opts))))

(defn update
  "Stages updates to a database without committing.

  Parameters:
    db - Database value
    json-ld - JSON-LD document with transaction operations
    opts - (optional) Options map:
      :context - Override default context

  Multiple updates can be staged and committed together.
  Returns promise resolving to updated database."
  ([db json-ld] (update db json-ld nil))
  ([db json-ld opts]
   (promise-wrap
    (transact-api/update db json-ld opts))))

;; TODO - deprecate `stage` in favor of `update` eventually
(defn ^:deprecated stage
  "Renamed to `update`, prefer that API instead."
  ([db json-ld] (update db json-ld nil))
  ([db json-ld opts] (update db json-ld opts)))

(defn format-txn
  "Converts SPARQL Update syntax to Fluree transaction format.

  Parameters:
    txn - Transaction data (string or map)
    override-opts - Options map with :format key

  If :format is :sparql, parses SPARQL Update and converts to JSON-LD Query.
  Otherwise returns txn unchanged."
  [txn override-opts]
  (parse/parse-sparql txn override-opts))

(defn commit!
  "Persists a staged database as a new immutable version in the ledger.

  Parameters:
    ledger - Ledger object
    db - Staged database with changes
    opts - (optional) Options map

  Creates a new commit and notifies the nameservice of the new version.
  Returns promise resolving to the committed database."
  ([ledger db]
   (promise-wrap
    (transact/commit! ledger db {})))
  ([ledger db opts]
   (promise-wrap
    (transact/commit! ledger db opts))))

(defn ^:deprecated transact!
  "Deprecated: Use `update!` instead.
  
  Updates a ledger and commits the changes in one operation."
  ([conn txn] (transact! conn txn nil))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/update! conn txn opts))))

(defn update!
  "Stages updates to a database and commits in one atomic operation.

  Parameters:
    conn - Connection object
    txn - Transaction map with:
      'from' or 'ledger' - Ledger identifier
      JSON-LD document with transaction operations
    opts - (optional) Options map:
      :context - Override default context

  Equivalent to calling `update` then `commit!`.
  Returns promise resolving to committed database."
  ([conn txn] (update! conn txn nil))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/update! conn txn opts))))

(defn upsert!
  "Stages insertion or update of entities and commits in one atomic operation.

  Parameters:
    conn - Connection object
    ledger-id - Ledger alias or address
    rdf - JSON-LD or Turtle RDF data to upsert
    opts - (optional) Options map:
      :context - Context override (ignored if present in json-ld)
      :format - Data format (:json-ld default, or :turtle)

  Creates new entities or updates existing ones based on @id.
  Equivalent to calling `upsert` then `commit!`.
  Returns promise resolving to committed database."
  ([conn ledger-id txn] (upsert! conn ledger-id txn nil))
  ([conn ledger-id txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/upsert! conn ledger-id txn opts))))

(defn insert!
  "Stages insertion of new entities and commits in one atomic operation.

  Parameters:
    conn - Connection object
    ledger-id - Ledger alias or address
    rdf - JSON-LD or Turtle RDF data to insert
    opts - (optional) Options map:
      :context - Context override (ignored if present in json-ld)
      :format - Data format (:json-ld default, or :turtle)

  Throws exception if any entity @id already exists.
  For insert-or-update behavior, use `upsert!`.
  Equivalent to calling `insert` then `commit!`.
  Returns promise resolving to committed database."
  ([conn ledger-id txn] (insert! conn ledger-id txn nil))
  ([conn ledger-id txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/insert! conn ledger-id txn opts))))

(defn credential-update!
  "Stages updates to a database and commits using a verifiable credential.

  Parameters:
    conn - Connection object
    credential - Verifiable credential containing transaction with:
      'from' or 'ledger' - Ledger identifier
      JSON-LD document with transaction operations
    opts - (optional) Options map:
      :context - Override default context

  Verifies credential signature and applies identity-based policies.
  Equivalent to calling `update!` with credential verification.
  Returns promise resolving to committed database."
  ([conn credential] (credential-update! conn credential nil))
  ([conn credential opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/credential-transact! conn credential opts))))

(defn ^:deprecated credential-transact!
  "Deprecated: Use `credential-update!` instead.
  
  Executes a transaction using a verifiable credential."
  ([conn txn] (credential-update! conn txn nil))
  ([conn txn opts] (credential-update! conn txn opts)))

(defn create-with-txn
  "Creates a new ledger and applies an initial transaction.

  Parameters:
    conn - Connection object
    txn - Transaction map containing:
      'ledger' - Ledger alias (required)
      'insert'/'delete'/'where' - Transaction operations
    opts - (optional) Additional options

  Returns promise resolving to initial database."
  ([conn txn]
   (validate-connection conn)
   (promise-wrap
    (transact-api/create-with-txn conn txn)))
  ([conn txn opts]
   (validate-connection conn)
   (promise-wrap
    (transact-api/create-with-txn conn txn opts))))

(defn status
  "Returns current status of a ledger branch.

  Parameters:
    ledger - Ledger object
    branch - (optional) Branch name (default: current branch)

  Returns status map with commit and index information."
  ([ledger] (ledger/status ledger))
  ([ledger branch] (ledger/status ledger branch)))

;; db operations

(defn db
  "Returns the current database value from a ledger."
  [ledger]
  (ledger/current-db ledger))

(defn wrap-policy
  "Applies policy restrictions to a database.

  Parameters:
    db - Database value
    policy - JSON-LD policy document
    policy-values - (optional) Values for policy variables

  Returns promise resolving to policy-wrapped database."
  ([db policy]
   (wrap-policy db policy nil))
  ([db policy policy-values]
   (promise-wrap
    (let [policy* (json-ld/expand policy)]
      (policy/wrap-policy db policy* policy-values)))))

(defn wrap-class-policy
  "Applies policy restrictions based on policy classes in the database.

  Parameters:
    db - Database value
    policy-classes - IRI or vector of IRIs of policy classes
    policy-values - (optional) Values for policy variables

  Finds and applies all policies with matching @type.
  Returns promise resolving to policy-wrapped database."
  ([db policy-classes]
   (wrap-class-policy db policy-classes nil))
  ([db policy-classes policy-values]
   (promise-wrap
    (policy/wrap-class-policy db nil policy-classes policy-values))))

(defn wrap-identity-policy
  "Applies policy restrictions based on an identity's policy classes.

  Parameters:
    db - Database value
    identity - IRI of the identity
    policy-values - (optional) Values for policy variables

  Looks up the identity's f:policyClass property and applies
  all policies with those class IRIs.

  Returns promise resolving to policy-wrapped database."
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
  "Executes a query against a database or dataset.

  Parameters:
    ds - Database value or dataset
    q - Query map (JSON-LD or analytical)
    opts - (optional) Options map

  Returns promise resolving to query results."
  ([ds q]
   (query ds q {}))
  ([ds q opts]
   (if (util/exception? ds)
     (throw ds)
     (promise-wrap (query-api/query ds q opts)))))

(defn credential-query
  "Executes a query using a verifiable credential.

  Parameters:
    ds - Database value or dataset
    cred-query - Verifiable credential containing query
    opts - (optional) Options map:
      :values-map - Values for policy variables
      :format - Query format (:sparql or default)

  Verifies credential signature and applies identity-based policies.
  Returns promise resolving to query results."
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
  "Executes a query using the connection's query engine.

  Parameters:
    conn - Connection object
    q - Query map with 'from' key specifying ledger
    opts - (optional) Options map

  Uses the current database state at query time.
  Returns promise resolving to query results."
  ([conn q] (query-connection conn q {}))
  ([conn q opts]
   (validate-connection conn)
   (promise-wrap (query-api/query-connection conn q opts))))

(defn credential-query-connection
  "Executes a query via connection using a verifiable credential.

  Parameters:
    conn - Connection object
    cred-query - Verifiable credential containing query
    opts - (optional) Options map

  Verifies credential and applies identity-based policies.
  Returns promise resolving to query results."
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

(defn query-nameservice
  "Executes a query against all nameservice records.

  Parameters:
    conn - Connection object
    query - Query map in JSON-LD format
    opts - (optional) Options map

  Creates a temporary in-memory ledger from all nameservice records and
  executes the query against it. Useful for queries like 'find all branches
  for ledger xyz' or 'find all ledgers where t=42'.

  Returns promise resolving to query results."
  ([conn query] (query-nameservice conn query {}))
  ([conn query opts]
   (validate-connection conn)
   (promise-wrap
    (go-try
      ;; Get the nameservice from the connection's primary publisher
      (if-some [primary-publisher (:primary-publisher conn)]
        (<? (ns-query/query-nameservice primary-publisher query opts))
        (throw (ex-info "No nameservice available for querying"
                        {:status 400 :error :db/no-nameservice})))))))

(defn history
  "Queries the history of entities across commits.

  Parameters:
    ledger - Ledger object
    query - Query map with:
      'history' - Subject IRI or pattern
      't' - Specific time or {'from': t1, 'to': t2}
      'commit-details' - Include commit metadata (default: false)
    opts - (optional) Options map

  Returns promise resolving to historical flakes."
  ([ledger query]
   (history ledger query nil))
  ([ledger query override-opts]
   (promise-wrap
    (query-api/history ledger query override-opts))))

(defn credential-history
  "Executes a history query using a verifiable credential.

  Parameters:
    ledger - Ledger object
    cred-query - Verifiable credential containing history query
    opts - (optional) Options map

  Verifies credential and applies identity-based policies.
  Returns promise resolving to historical data."
  ([ledger cred-query] (credential-history ledger cred-query {}))
  ([ledger cred-query override-opts]
   (promise-wrap
    (go-try
      (let [{query :subject, identity :did} (<? (cred/verify cred-query))]
        (<? (query-api/history ledger query (assoc override-opts :identity identity))))))))

(defn range
  "Performs a range scan on a database index.

  Parameters:
    db - Database value
    index - Index name (:spot, :psot, :post, :opst, :tspo)
    test - Test function (>=, <=, >, <) or start-test for two-sided
    match - Value to match or start-match for two-sided
    end-test - (optional) End test function for two-sided range
    end-match - (optional) End value for two-sided range

  Returns promise resolving to matching flakes."
  ;; TODO - assert index is valid index type
  ([db index test match]
   (promise-wrap
    (query-range/index-range db index test match)))
  ([db index start-test start-match end-test end-match]
   (promise-wrap
    (query-range/index-range db nil index start-test start-match end-test end-match))))

(defn slice
  "Returns all flakes that exactly match the supplied pattern.

  Parameters:
    db - Database value
    index - Index name (:spot, :psot, :post, :opst, :tspo)
    match - Flake pattern to match

  Returns promise resolving to matching flakes."
  [db index match]
  (promise-wrap
   (query-range/index-range db index = match)))

(defn expand-iri
  "Expands a compact IRI to its full form using the context.

  Parameters:
    context - JSON-LD context for expansion
    compact-iri - The compact IRI to expand

  Returns the expanded IRI string."
  ([context compact-iri]
   (json-ld/expand-iri compact-iri
                       (json-ld/parse-context context))))

(defn encode-iri
  "Encodes an IRI to Fluree's internal compact format.

  Parameters:
    db - Database value  
    iri - IRI string to encode

  Used for range scans, slices and advanced index operations.
  Returns the encoded identifier."
  [db iri]
  (iri/encode-iri db iri))

(defn decode-iri
  "Decodes a Fluree internal identifier back to an IRI string.

  Parameters:
    db - Database value
    iri - Encoded identifier to decode

  Opposite of encode-iri. Used when working with raw index data.
  Returns the full IRI string."
  [db iri]
  (iri/decode-sid db iri))

;; reasoning APIs

(defn reason
  "Applies reasoning rules to a database.

  Parameters:
    db - Database value
    methods - Reasoner method or vector of methods (:datalog, :owl2rl)
    rule-sources - (optional) JSON-LD rules or nil to use rules from db
    opts - (optional) Options map

  Reasoning is done in-memory and not persisted.
  Returns promise resolving to reasoning-enabled database."
  ([db methods] (reason db methods nil nil))
  ([db methods rule-sources] (reason db methods rule-sources nil))
  ([db methods rule-sources opts]
   (promise-wrap
    (reasoner/reason db methods rule-sources opts))))

(defn reasoned-count
  "Returns the number of facts inferred by reasoning.

  Must have reasoning enabled on the database."
  [db]
  (reasoner/reasoned-count db))

(defn reasoned-facts
  "Returns facts inferred by reasoning.

  Parameters:
    db - Database value with reasoning enabled
    opts - (optional) Options map:
      :group-by - Grouping option (:rule, :subject, or :property)

  Returns 4-tuples of [subject-iri property-iri object rule-id]
  where rule-id is the identifier of the rule that generated the fact."
  ([db] (reasoned-facts db nil))
  ([db opts]
   (let [grouping (:group-by opts)]
     (reasoner/reasoned-facts db grouping))))
