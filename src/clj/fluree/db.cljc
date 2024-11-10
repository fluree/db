(ns fluree.db
  (:refer-clojure :exclude [load range exists?])
  (:require [camel-snake-kebab.core :refer [->camelCaseString]]
            [clojure.walk :refer [postwalk]]
            [fluree.db.constants :as const]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.connection :as connection :refer [notify-ledger]]
            [fluree.db.transact :as transact]
            [fluree.db.util.context :as context]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.iri :as iri]
            [clojure.core.async :as async :refer [go <!]]
            [fluree.db.query.api :as query-api]
            [fluree.db.transact.api :as transact-api]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.ledger :as ledger]
            [fluree.db.util.log :as log]
            [fluree.db.query.range :as query-range]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.dataset :as dataset]))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

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
  (go-try
    (-> conn ::system-map system/terminate)))

(defn connect-ipfs
  "Forms an ipfs connection using default settings.
  - server - (optional) IPFS http api server endpoint, defaults to http://127.0.0.1:5001/
  - profile - (optional) IPFS stored profile to use.
  - did - (optional) DId information to use, if storing blocks as verifiable credentials"
  [opts]
  (connect (assoc opts :method :ipfs)))

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
  ([conn] (create conn nil nil))
  ([conn ledger-alias] (create conn ledger-alias nil))
  ([conn ledger-alias opts]
   (promise-wrap
    (do
      (log/info "Creating ledger" ledger-alias)
      (connection/create-ledger conn ledger-alias opts)))))

(defn alias->address
  "Returns a core.async channel with the connection-specific address of the
  given ledger-alias."
  [conn ledger-alias]
  (connection/primary-address conn ledger-alias))

(defn load
  "Loads an existing ledger by its alias (which will be converted to a
  connection-specific address first)."
  [conn alias-or-address]
  (promise-wrap
    (connection/load-ledger conn alias-or-address)))

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
        (<! (connection/ledger-exists? conn address))))))

(defn notify
  "Notifies the connection with a new commit map (parsed JSON commit with string keys).

  If the connection knows of the ledger, and is currently maintaining
  an in-memory version of the ledger, will attempt to update the db if the commit
  is for the next 't' value. If a commit is for a past 't' value, noop.
  If commit is for a future 't' value, will drop in-memory ledger for reload upon next request."
  [conn commit-map]
  (promise-wrap
    (if (map? commit-map)
      (notify-ledger conn commit-map)
      (go
        (ex-info (str "Invalid commit map, perhaps it is JSON that needs to be parsed first?: " commit-map)
                 {:status 400 :error :db/invalid-commit-map})))))

(defn parse-transaction-opts
  [expanded-txn override-opts txn-context]
  (let [txn-opts (some-> (util/get-first-value expanded-txn const/iri-opts)
                         (syntax/coerce-txn-opts))
        opts     (merge txn-opts (some-> override-opts syntax/coerce-txn-opts))]
    (-> opts
        (assoc :context txn-context)
        (update :identity #(or % (:did opts)))
        (dissoc :did))))

(defn stage
  "Performs a transaction and queues change if valid (does not commit)"
  ([db json-ld]
   (stage db json-ld nil))
  ([db json-ld opts]
   (promise-wrap
    (go-try
      (let [txn-context (or (context/txn-context json-ld)
                            (:context opts))
            expanded    (json-ld/expand (context/use-fluree-context json-ld))
            parsed-opts (parse-transaction-opts expanded opts txn-context)
            parsed-txn  (parse/parse-txn expanded txn-context)]
        (<? (transact/stage-triples db parsed-txn parsed-opts)))))))

(defn commit!
  "Commits a staged database to the ledger with all changes since the last commit
  aggregated together.

  Commits are tracked in the local environment, but if the ledger is distributed
  it will still need a 'push' to ensure it is published and verified as per the
  distributed rules."
  ([ledger db]
   (promise-wrap
     (connection/commit! ledger db)))
  ([ledger db opts]
   (promise-wrap
     (connection/commit! ledger db opts))))

(defn transact!
  ([conn txn] (transact! conn txn nil))
  ([conn txn opts]
   (promise-wrap
    (transact-api/transact! conn txn opts))))

(defn credential-transact!
  ([conn txn] (credential-transact! conn txn nil))
  ([conn txn opts]
   (promise-wrap
    (transact-api/credential-transact! conn txn opts))))

(defn create-with-txn
  [conn txn]
  (promise-wrap
    (transact-api/create-with-txn conn txn)))

(defn status
  "Returns current status of ledger branch."
  ([ledger] (ledger/status ledger))
  ([ledger branch] (ledger/status ledger branch)))


;; db operations

(defn db
  "Retrieves latest db, or optionally a db at a moment in time
  and/or permissioned to a specific identity."
  ([ledger]
   (ledger/current-db ledger)))

(defn wrap-policy
  "Restricts the provided db with the provided json-ld
  policy restrictions"
  ([db policy]
   (wrap-policy db policy nil))
  ([db policy values-map]
   (promise-wrap
    (let [policy* (json-ld/expand policy)]
      (policy/wrap-policy db policy* values-map)))))

(defn wrap-class-policy
  "Restricts the provided db with policies in the db
  which have a class @type of the provided class(es)."
  ([db policy-classes]
   (wrap-class-policy db policy-classes nil))
  ([db policy-classes values-map]
   (promise-wrap
    (policy/wrap-class-policy db policy-classes values-map))))

(defn wrap-identity-policy
  "For provided identity, locates specific property f:policyClass on
  the identity containing a list of class IRIs that identity the policies
  that should be applied to the identity.

  With the policy classes, finds all policies containing that class
  declaration."
  ([db identity]
   (wrap-identity-policy db identity nil))
  ([db identity values-map]
   (promise-wrap
    (policy/wrap-identity-policy db identity values-map))))

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
   (dataset/dataset named-graphs default-graphs)))

(defn q
  "Queries a dataset or single db and returns a promise with the results."
  ([ds query]
   (q ds query {}))
  ([ds query opts]
   (promise-wrap (query-api/query ds query opts))))

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
       (let [policy-db (<? (policy/wrap-identity-policy ds identity values-map))]
         (<? (query-api/query policy-db query opts))))))))

(defn query-connection
  "Queries the latest db in the ledger specified by the 'from' parameter in the
  query (what that actually looks like is format-specific). Returns a promise
  with the results."
  ([conn q] (query-connection conn q {}))
  ([conn q opts]
   (promise-wrap (query-api/query-connection conn q opts))))

(defn credential-query-connection
  ([conn cred-query] (credential-query-connection conn cred-query {}))
  ([conn cred-query {:keys [format] :as opts}]
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
   (promise-wrap
    (query-api/history (ledger/current-db ledger) query)))
  ([ledger query override-opts]
   (promise-wrap
    (go-try
      (let [latest-db (ledger/current-db ledger)
            context   (context/extract query)
            {:keys [opts] :as sanitized-query} (query-api/sanitize-query-options query override-opts)
            {:keys [policy identity policy-class policy-values]} opts
            policy-db (cond
                        identity
                        (<? (policy/wrap-identity-policy latest-db identity policy-values))

                        policy
                        (<? (policy/wrap-policy latest-db (json-ld/expand policy context) policy-values))

                        policy-class
                        (<? (policy/wrap-class-policy latest-db (json-ld/expand policy-class context) policy-values))

                        :else
                        latest-db)]
        (<? (query-api/history policy-db sanitized-query)))))))

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
      (let [latest-db (ledger/current-db ledger)
            {query :subject, identity :did} (<? (cred/verify cred-query))]
        (log/debug "Credential history query with identity: " identity " and query: " query)
        (cond
          (and query identity)
          (let [{:keys [opts] :as sanitized-query} (query-api/sanitize-query-options query (assoc override-opts :identity identity))
                {:keys [identity policy-values]} opts
                policy-db (<? (policy/wrap-identity-policy latest-db identity policy-values))]
            (<? (query-api/history policy-db sanitized-query)))

          identity
          (throw (ex-info "Query not present in credential"
                          {:status 400 :error :db/invalid-credential}))

          :else
          (throw (ex-info "Invalid credential"
                          {:status 400 :error :db/invalid-credential}))))))))

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
