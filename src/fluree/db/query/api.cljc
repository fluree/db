(ns fluree.db.query.api
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.dataset :as dataset :refer [dataset?]]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice.virtual-graph :as ns-vg]
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
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.nameservice-loader :as vg-loader]))

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
  [ds tracker exec-fn]
  (go-try
    (track/register-policies! tracker ds)
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

(defn query-str->map
  "Converts the string query parameters of
  k=v&k2=v2&k3=v3 into a map of {k v, k2 v2, k3 v3}"
  [query-str]
  (->> (str/split query-str #"&")
       (map str/trim)
       (map (fn [s]
              (str/split s #"=")))
       (reduce
        (fn [acc [k v]]
          (assoc acc k v))
        {})))

(defn parse-t-val
  "If t-val is an integer in string form, coerces
  it to an integer, otherwise assumes it is an
  ISO-8601 datetime string and returns it as is."
  [t-val]
  (if (re-matches #"^\d+$" t-val)
    (util/str->long t-val)
    t-val))

(defn extract-query-string-t
  "This uses the http query string format to as a generic way to
  select a specific 'db' that can be used in queries. For now there
  is only one parameter/key we look for, and that is `t` which can
  be used to specify the moment in time.

  e.g.:
   - my/db?t=42
   - my/db?t=2020-01-01T00:00:00Z"
  [alias]
  (let [[alias query-str] (str/split alias #"\?")]
    (if query-str
      [alias (-> query-str
                 query-str->map
                 (get "t")
                 parse-t-val)]
      [alias nil])))

(defn- extract-primary-ledger-name
  "Extracts the primary ledger name from a collection of dependencies.
  Looks for the first dependency with a ledger reference pattern (e.g., 'mydb@main')
  and returns just the ledger name part (e.g., 'mydb')."
  [dependencies]
  (some->> dependencies
           (map #(get % "@id"))
           (filter #(re-matches #"^[^#]+@\w+$" %))  ; Match ledger@branch pattern
           first
           (re-find #"^([^@]+)@")                    ; Extract ledger name before @
           second))

(defn load-virtual-graph
  "Loads a virtual graph from nameservice and returns it as a DB-like object."
  [conn vg-name]
  (go-try
    (let [primary-publisher (connection/primary-publisher conn)
          vg-record (<? (ns-vg/get-virtual-graph primary-publisher vg-name))]
      (if (= :not-found vg-record)
        ;; Not a virtual graph, return nil
        nil
        ;; This is a virtual graph - need to instantiate it
        ;; For now, we need to get a ledger to associate with the VG
        ;; In the future, VGs could be completely independent
        (let [dependencies (get vg-record "f:dependencies")
              ;; Find first ledger dependency
              primary-ledger (extract-primary-ledger-name dependencies)]
          (if primary-ledger
            (let [ledger (<? (connection/load-ledger-alias conn primary-ledger))
                  db (ledger/current-db ledger)]
              ;; Load the VG directly using the nameservice
              (<? (vg-loader/load-virtual-graph-from-nameservice db primary-publisher vg-name)))
            (throw (ex-info (str "Virtual graph has no ledger dependencies: " vg-name)
                            {:status 400 :error :db/invalid-configuration}))))))))

(defn load-alias
  [conn tracker alias {:keys [t] :as sanitized-query}]
  (go-try
    (try*
      ;; First try to load as virtual graph
      (if-let [vg (<? (load-virtual-graph conn alias))]
        ;; Virtual graphs don't need restrict-db as they handle their own restrictions
        vg
        ;; Not a virtual graph, load as regular ledger
        (let [[alias explicit-t] (extract-query-string-t alias)
              ledger       (<? (connection/load-ledger-alias conn alias))
              db           (ledger/current-db ledger)
              t*           (or explicit-t t)
              query*       (assoc sanitized-query :t t*)]
          (<? (restrict-db db tracker query* conn))))
      (catch* e
        (throw (contextualize-ledger-400-error
                (str "Error loading resource " alias ": ")
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
          default-aliases (some-> sanitized-query :from util/sequential)
          named-aliases   (some-> sanitized-query :from-named util/sequential)]
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
