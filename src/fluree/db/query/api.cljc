(ns fluree.db.query.api
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require #?(:clj [fluree.db.connection.system :as system])
            #?(:clj [fluree.db.util.json :as json])
            #?(:clj [fluree.db.virtual-graph.nameservice-loader :as vg-loader])
            [fluree.db.connection :as connection]
            [fluree.db.dataset :as dataset :refer [dataset?]]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.history :as history]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.track :as track]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as context]
            [fluree.db.util.ledger :as ledger-util]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn sanitize-query-options
  [query override-opts]
  (update query :opts (fn [{:keys [max-fuel] :as opts}]
                        ;; ensure :max-fuel key is present
                        (-> opts
                            (assoc :max-fuel max-fuel)
                            (merge override-opts)
                            (update :output #(or % :fql))
                            ;; get rid of :did, :issuer opts
                            (update :identity #(or % (:did opts) (:issuer opts)))
                            (dissoc :did :issuer)))))

(defn load-aliased-rule-dbs
  [conn rule-sources]
  (go-try
    (loop [rule-sources rule-sources
           rule-results []]
      (if-let [rule-source (first rule-sources)]
        (let [updated-rule-results (into rule-results
                                         (if (string? rule-source)
                                           (ledger/current-db (<? (connection/load-ledger conn rule-source)))
                                           rule-source))]
          (recur (rest rule-sources) updated-rule-results))
        rule-results))))

(defn restrict-db
  ([db sanitized-query]
   (restrict-db db nil sanitized-query))
  ([db tracker sanitized-query]
   (restrict-db db tracker sanitized-query nil))
  ([db tracker {:keys [t opts] :as sanitized-query} conn]
   (go-try
     (let [{:keys [reasoner-methods rule-sources]} opts
           processed-rule-sources (when (and rule-sources conn)
                                    (<? (load-aliased-rule-dbs conn rule-sources)))
           policy-db              (if (perm/policy-enforced-opts? opts)
                                    (let [parsed-context (context/extract sanitized-query)]
                                      (<? (perm/policy-enforce-db db tracker parsed-context opts)))
                                    db)
           time-travel-db         (-> (if t
                                        (<? (time-travel/as-of policy-db t))
                                        policy-db))
           reasoned-db            (if reasoner-methods
                                    (<? (reasoner/reason time-travel-db
                                                         reasoner-methods
                                                         processed-rule-sources
                                                         opts))
                                    time-travel-db)]
       (assoc-in reasoned-db [:policy :cache] (atom {}))))))

(defn track-execution
  "Track fuel usage in query. `exec-fn` is a thunk that when called with no arguments
  returns a result or throws an exception."
  [_ds tracker exec-fn]
  (go-try
    (try* (let [result        (<? (exec-fn))
                tally         (track/tally tracker)]
            (assoc tally :status 200, :result result))
          (catch* e
            (let [data (-> tracker
                           track/tally
                           (assoc :status (-> e ex-data :status)))]
              (throw (ex-info "Error executing query"
                              data
                              e)))))))

(defn history
  "Return a summary of the changes over time, optionally with the full commit
  details included."
  [ledger query override-opts]
  (go-try
    (let [{:keys [opts] :as query*} (sanitize-query-options query override-opts)

          tracker   (track/init opts)
          context   (context/extract query*)
          latest-db (ledger/current-db ledger)
          policy-db (if (perm/policy-enforced-opts? opts)
                      (<? (perm/policy-enforce-db latest-db tracker context opts))
                      latest-db)]
      (if (track/track-query? opts)
        (<? (track-execution policy-db tracker #(history/query policy-db tracker context query*)))
        (<? (history/query policy-db context query*))))))

(defn query-fql
  "Execute a query against a database source. Returns core async channel
  containing result or exception."
  ([ds query] (query-fql ds query nil))
  ([ds query override-opts]
   (go-try
     (let [{:keys [opts] :as query*} (-> query
                                         syntax/coerce-query
                                         (sanitize-query-options override-opts))

           tracker (track/init opts)

           ;; TODO - remove restrict-db from here, restriction should happen
           ;;      - upstream if needed
           ds*      (if (dataset? ds)
                      ds
                      (<? (restrict-db ds tracker query*)))
           query**  (update query* :opts dissoc :meta :max-fuel)]
       (if (track/track-query? opts)
         (<? (track-execution ds* tracker #(fql/query ds* tracker query**)))
         (<? (fql/query ds* query**)))))))

(defn query-sparql
  [db query override-opts]
  (go-try
    (let [fql (sparql/->fql query)]
      (<? (query-fql db fql override-opts)))))

(defn query
  [db query {:keys [format] :as override-opts :or {format :fql}}]
  (case format
    :fql (query-fql db query override-opts)
    :sparql (query-sparql db query override-opts)))

(defn explain
  "Returns a query execution plan without executing the query. The plan shows
  pattern reordering and selectivity scores based on database statistics.

  Parameters:
    db - Database value or dataset
    query - Query map (JSON-LD or analytical)

  Returns channel resolving to a query plan map."
  [db query {:keys [format] :as _override-opts :or {format :fql}}]
  (go-try
    (let [fql (if (= :sparql format)
                (sparql/->fql query)
                query)
          q (-> fql
                syntax/coerce-query
                (sanitize-query-options nil))
          q* (update q :opts dissoc :meta :max-fuel)]

      (<? (fql/explain db q*)))))

(defn contextualize-ledger-400-error
  [info-str e]
  (let [e-data (ex-data e)]
    (if (= 400
           (:status e-data))
      (ex-info
       (str info-str
            (ex-message e))
       e-data
       e)
      e)))

(defn extract-query-string-t
  "Extracts time travel specification from ledger alias.
  Delegates to util.ledger/parse-ledger-alias and returns in
  the format expected by load-alias.

  Returns [base-alias time-travel-value] where:
   - base-alias includes ledger:branch if branch is present
   - time-travel-value can be nil, Long, String, or {:sha ...} map"
  [alias]
  (let [{:keys [ledger branch t]} (ledger-util/parse-ledger-alias alias)
        base-alias (if branch
                     (str ledger ":" branch)
                     (ledger-util/ensure-ledger-branch ledger))]
    [base-alias t]))

(def ledger-specific-opts #{:policy-class :policy :policy-values})

(defn ledger-opts-override
  [{:keys [opts] :as q} {:keys [alias] :as _db}]
  (let [;; First try the full alias (ledger:branch), then fall back to ledger name only
        base-name (ledger-util/ledger-base-name alias)
        ledger-opts (or (some-> opts (get alias) (select-keys ledger-specific-opts))
                        (some-> opts (get base-name) (select-keys ledger-specific-opts)))]
    (update q :opts merge ledger-opts)))

(defn- virtual-graph-record?
  "Returns true if a nameservice record represents a virtual graph."
  [ns-record]
  (when ns-record
    (let [types (get ns-record "@type")]
      (some #{"f:VirtualGraphDatabase"} types))))

#?(:clj
   (defn- r2rml-virtual-graph?
     "Returns true if a nameservice record represents an R2RML virtual graph."
     [ns-record]
     (when ns-record
       (let [types (set (get ns-record "@type" []))]
         (contains? types "fidx:R2RML")))))

#?(:clj
   (defn- iceberg-virtual-graph?
     "Returns true if a nameservice record represents an Iceberg virtual graph."
     [ns-record]
     (when ns-record
       (let [types (set (get ns-record "@type" []))]
         (contains? types "fidx:Iceberg")))))

(defn load-alias
  [conn tracker alias {:keys [t] :as sanitized-query}]
  (go-try
    (log/debug "load-alias called with:" alias)
    (let [[base-alias explicit-t] (extract-query-string-t alias)
          ;; Normalize to ensure branch (e.g., "docs" -> "docs:main")
          normalized-alias (ledger-util/ensure-ledger-branch base-alias)
          publisher        (connection/primary-publisher conn)
          ns-record        (<? (nameservice/lookup publisher normalized-alias))]
      (if (virtual-graph-record? ns-record)
        ;; Virtual graph - load via VG loader (JVM only)
        #?(:clj
           (cond
             (r2rml-virtual-graph? ns-record)
             ;; R2RML VGs connect to external databases, don't need a source ledger
             (<? (vg-loader/load-virtual-graph-from-nameservice nil publisher normalized-alias))

             (iceberg-virtual-graph? ns-record)
             ;; Iceberg VGs - create directly and apply time-travel if specified
             ;; Uses requiring-resolve for dynamic loading (db-iceberg module)
             (if-let [create-fn (requiring-resolve 'fluree.db.virtual-graph.iceberg/create)]
               (let [raw-config (get-in ns-record ["fidx:config" "@value"])
                     ;; Config is stored as JSON string, need to parse it
                     config (if (string? raw-config)
                              (json/parse raw-config false)
                              raw-config)
                     ;; Get publisher-level Iceberg config and shared cache
                     iceberg-config (system/get-iceberg-config publisher)
                     cache-instance (system/get-iceberg-cache publisher)
                     vg (create-fn {:alias normalized-alias
                                    :config config
                                    :iceberg-config iceberg-config
                                    :cache-instance cache-instance})
                     ;; Apply time-travel if specified in alias (e.g., airlines@t:12345)
                     parse-time-travel (requiring-resolve 'fluree.db.virtual-graph.iceberg/parse-time-travel)
                     with-time-travel (requiring-resolve 'fluree.db.virtual-graph.iceberg/with-time-travel)
                     time-travel (when explicit-t (parse-time-travel explicit-t))]
                 (with-time-travel vg time-travel))
               (throw (ex-info "Iceberg support not available. Add com.fluree/db-iceberg dependency."
                               {:status 501 :error :db/missing-iceberg-module})))

             :else
             ;; Other VGs (BM25, etc.) need a source ledger from dependencies
             (let [deps          (get ns-record "fidx:dependencies")
                   source-alias  (first deps)
                   source-ledger (<? (connection/load-ledger-alias conn source-alias))
                   source-db     (ledger/current-db source-ledger)]
               (<? (vg-loader/load-virtual-graph-from-nameservice
                    source-db publisher normalized-alias))))
           :cljs
           (throw (ex-info "Virtual graphs are not supported in ClojureScript"
                           {:status 400 :error :db/unsupported})))
        ;; Regular ledger
        (if ns-record
          (let [ledger (<? (connection/load-ledger-alias conn normalized-alias))
                db     (ledger/current-db ledger)
                t*     (or explicit-t t)
                query* (-> sanitized-query
                           (assoc :t t*)
                           (ledger-opts-override db))]
            (<? (restrict-db db tracker query* conn)))
          (throw (ex-info (str "Load for " normalized-alias " failed due to failed address lookup.")
                          {:status 404 :error :db/unkown-ledger})))))))

(defn load-aliases
  [conn tracker aliases sanitized-query]
  (when (some? (:t sanitized-query))
    (try*
      (util/str->epoch-ms (:t sanitized-query))
      (catch* e
        (throw
         (contextualize-ledger-400-error
          (str "Error in federated query: top-level `t` values "
               "must be iso-8601 wall-clock times. ")
          e)))))
  (go-try
    (loop [[alias & r] aliases
           db-map      {}]
      (if alias
        ;; TODO: allow restricting federated dataset components individually by t
        (let [db      (<? (load-alias conn tracker alias sanitized-query))
              db-map* (assoc db-map alias db)]
          (recur r db-map*))
        db-map))))

(defn dataset
  [named-graphs default-aliases]
  (let [default-coll (some->> default-aliases
                              util/sequential
                              (select-keys named-graphs)
                              vals)]
    (dataset/combine named-graphs default-coll)))

(defn load-dataset
  [conn tracker defaults named sanitized-query]
  (go-try
    (if (and (= (count defaults) 1)
             (empty? named))
      (let [alias (first defaults)]
        ;; return an unwrapped db if the data set consists of one ledger
        (<? (load-alias conn tracker alias sanitized-query)))
      (let [all-aliases (->> defaults (concat named) distinct)
            db-map      (<? (load-aliases conn tracker all-aliases sanitized-query))]
        (dataset db-map defaults)))))

(defn query-connection-fql
  [conn query override-opts]
  (go-try
    (let [{:keys [opts] :as sanitized-query} (-> query
                                                 syntax/coerce-query
                                                 (sanitize-query-options override-opts))

          tracker         (track/init opts)
          default-aliases (or (some-> opts :from util/sequential)
                              (some-> opts :ledger util/sequential)
                              (some-> sanitized-query :from util/sequential))
          named-aliases   (or (some-> opts :from-named util/sequential)
                              (some-> sanitized-query :from-named util/sequential))]
      (if (or (seq default-aliases)
              (seq named-aliases))
        (let [ds            (<? (load-dataset conn tracker default-aliases named-aliases sanitized-query))
              _ (log/debug "Loaded dataset:" (type ds) "for aliases:" default-aliases)
              trimmed-query (update sanitized-query :opts dissoc :meta :max-fuel)]
          (if (track/track-query? opts)
            (<? (track-execution ds tracker #(fql/query ds tracker trimmed-query)))
            (<? (fql/query ds trimmed-query))))
        (throw (ex-info "Missing ledger specification in connection query"
                        {:status 400, :error :db/invalid-query}))))))

(defn query-connection-sparql
  [conn query override-opts]
  (go-try
    (let [fql (sparql/->fql query)]
      (log/debug "query-connection SPARQL fql: " fql "override-opts:" override-opts)
      (<? (query-connection-fql conn fql override-opts)))))

(defn query-connection
  [conn query {:keys [format] :as override-opts :or {format :fql}}]
  (case format
    :fql (query-connection-fql conn query override-opts)
    :sparql (query-connection-sparql conn query override-opts)))
