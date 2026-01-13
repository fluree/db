(ns fluree.db.query.api
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.core.async :as async]
            [fluree.db.connection :as connection]
            [fluree.db.dataset :as dataset :refer [dataset?]]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.ledger :as ledger]
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

(defn load-alias
  [conn tracker alias {:keys [t] :as sanitized-query}]
  (go-try
    (try*
      (let [[base-alias explicit-t] (extract-query-string-t alias)
            ledger       (<? (connection/load-ledger-alias conn base-alias))
            db           (ledger/current-db ledger)
            t*           (or explicit-t t)
            query*       (-> sanitized-query
                             (assoc :t t*)
                             (ledger-opts-override db))]
        (<? (restrict-db db tracker query* conn)))
      (catch* e
        (throw (contextualize-ledger-400-error
                (str "Error loading ledger " alias ": ")
                e))))))

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

(defn query-fql-stream
  "Internal streaming query implementation. Handles db restriction, policy tracking,
   and metadata emission. Returns channel emitting individual results + optional
   final :_fluree-meta map."
  ([ds query]
   (query-fql-stream ds query nil))
  ([ds query override-opts]
   (let [out-ch       (async/chan)
         track-init   track/init
         track-query? track/track-query?
         track-tally  track/tally]
     (async/go
       (try*
         (let [{:keys [opts] :as query*} (-> query
                                             syntax/coerce-query
                                             (sanitize-query-options override-opts))
               tracker      (track-init opts)
               track-meta?  (track-query? opts)
               ds*          (if (dataset? ds)
                              ds
                              (async/<! (restrict-db ds tracker query*)))
               query**      (update query* :opts dissoc :meta :max-fuel)]

           (if (util/exception? ds*)
             (do
               (async/>! out-ch ds*)
               (async/close! out-ch))
             (let [result-ch (async/<! (fql/query-stream ds* tracker query**))]
               (if (util/exception? result-ch)
                 (do
                   (async/>! out-ch result-ch)
                   (async/close! out-ch))
                 (do
                   (loop []
                     (when-some [result (async/<! result-ch)]
                       (async/>! out-ch result)
                       (recur)))

                   (when track-meta?
                     (let [tally (track-tally tracker)]
                       (async/>! out-ch {:_fluree-meta (assoc tally :status 200)})))

                   (async/close! out-ch))))))
         (catch* e
           (async/>! out-ch e)
           (async/close! out-ch))))
     out-ch)))

(defn query-sparql-stream
  "Converts SPARQL to FQL and delegates to query-fql-stream."
  [db query override-opts]
  (let [fql (sparql/->fql query)]
    (query-fql-stream db fql override-opts)))

(defn query-stream
  "Dispatches to query-fql-stream or query-sparql-stream based on :format option."
  [db query {:keys [format] :as override-opts :or {format :fql}}]
  (case format
    :fql (query-fql-stream db query override-opts)
    :sparql (query-sparql-stream db query override-opts)))

(defn query-connection-fql-stream
  "Loads ledger(s) from connection and executes streaming query. Like
   query-connection-fql but streams individual results instead of collecting."
  [conn query override-opts]
  (let [out-ch       (async/chan)
        track-init   track/init
        track-query? track/track-query?
        track-tally  track/tally]
    (async/go
      (try*
        (let [{:keys [opts] :as sanitized-query} (-> query
                                                     syntax/coerce-query
                                                     (sanitize-query-options override-opts))
              tracker         (track-init opts)
              track-meta?     (track-query? opts)
              default-aliases (some-> sanitized-query :from util/sequential)
              named-aliases   (some-> sanitized-query :from-named util/sequential)]

          (log/debug "query-connection-fql-stream - from:" default-aliases
                     "from-named:" named-aliases)

          (if (or (seq default-aliases) (seq named-aliases))
            (let [ds            (async/<! (load-dataset conn tracker default-aliases
                                                        named-aliases sanitized-query))
                  trimmed-query (update sanitized-query :opts dissoc :meta :max-fuel)]
              (if (util/exception? ds)
                (do
                  (async/>! out-ch ds)
                  (async/close! out-ch))
                (let [result-ch (async/<! (fql/query-stream ds tracker trimmed-query))]
                  (if (util/exception? result-ch)
                    (do
                      (async/>! out-ch result-ch)
                      (async/close! out-ch))
                    (do
                      (loop []
                        (when-some [result (async/<! result-ch)]
                          (async/>! out-ch result)
                          (recur)))

                      (when track-meta?
                        (let [tally (track-tally tracker)]
                          (async/>! out-ch {:_fluree-meta (assoc tally :status 200)})))

                      (async/close! out-ch))))))

            (throw (ex-info "Missing ledger specification in connection query"
                            {:status 400, :error :db/invalid-query}))))
        (catch* e
          (async/>! out-ch e)
          (async/close! out-ch))))
    out-ch))

(defn query-connection-sparql-stream
  "Converts SPARQL to FQL and delegates to query-connection-fql-stream."
  [conn query override-opts]
  (let [fql (sparql/->fql query)]
    (log/debug "query-connection-sparql-stream fql:" fql
               "override-opts:" override-opts)
    (query-connection-fql-stream conn fql override-opts)))

(defn query-connection-stream
  "Dispatches to query-connection-fql-stream or query-connection-sparql-stream
   based on :format option."
  [conn query {:keys [format] :as override-opts :or {format :fql}}]
  (case format
    :fql (query-connection-fql-stream conn query override-opts)
    :sparql (query-connection-sparql-stream conn query override-opts)))
