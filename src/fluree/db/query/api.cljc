(ns fluree.db.query.api
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
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

(defn- extract-primary-ledger-name
  "Extracts the primary ledger alias from a collection of dependencies.
  Looks for the first dependency with a ledger reference pattern (e.g., 'mydb:main')
  and returns the full alias (e.g., 'mydb:main')."
  [dependencies]
  (some->> dependencies
           (map #(get % "@id"))
           ;; Filter for valid ledger aliases (those containing ':')
           (filter #(and (string? %)
                        ;; Verify it's a valid ledger alias by checking if we can parse it
                         (let [[ledger branch] (ledger-util/ledger-parts %)]
                           (and ledger branch))))
           first))

(defn load-virtual-graph
  "Loads a virtual graph from nameservice and returns it as a DB-like object.
  Returns nil if the alias is not a virtual graph."
  [conn vg-name]
  (go-try
    (log/debug "load-virtual-graph called for:" vg-name)
    (let [primary-publisher (connection/primary-publisher conn)
          vg-record (<? (nameservice/lookup primary-publisher vg-name))]
      (log/debug "VG record from nameservice:" vg-record)
      (if (not (nameservice/virtual-graph-record? vg-record))
        (do
          (log/debug "Not a virtual graph:" vg-name)
          nil)
        ;; Instantiate virtual graph (currently requires an associated ledger; future VGs may be independent)
        (let [dependencies (get vg-record "f:dependencies")
              ;; Find first ledger dependency
              primary-ledger (extract-primary-ledger-name dependencies)]
          (log/debug "Dependencies:" dependencies "Primary ledger:" primary-ledger)
          (if primary-ledger
            (let [ledger (<? (connection/load-ledger-alias conn primary-ledger))
                  db (ledger/current-db ledger)]
              (log/debug "Loading VG from nameservice...")
              (<? (vg-loader/load-virtual-graph-from-nameservice db primary-publisher vg-name)))
            (throw (ex-info (str "Virtual graph has no ledger dependencies: " vg-name)
                            {:status 400 :error :db/invalid-configuration}))))))))

(defn load-alias
  [conn tracker alias {:keys [t] :as sanitized-query}]
  (go-try
    (log/debug "load-alias called with:" alias)
    (let [[base-alias explicit-t] (extract-query-string-t alias)
          ;; Normalize to ensure branch (e.g., "docs" -> "docs:main")
          normalized-alias (ledger-util/ensure-ledger-branch base-alias)
          ;; Try to load as a ledger (most common case) - use <! to get result or exception
          ledger-result    (async/<! (connection/load-ledger-alias conn normalized-alias))
          valid-ledger?    (not (util/exception? ledger-result))]
      (if valid-ledger?
        ;; Successfully loaded ledger
        (let [ledger ledger-result
              db     (ledger/current-db ledger)
              t*     (or explicit-t t)
              query* (-> sanitized-query
                         (assoc :t t*)
                         (ledger-opts-override db))]
          (<? (restrict-db db tracker query* conn)))
        ;; Ledger load failed. If original alias has no ':', try as virtual graph
        (if (not (str/includes? alias ":"))
          (do
            (log/debug "Ledger load failed, trying as virtual graph:" alias)
            (if-let [vg (<? (load-virtual-graph conn alias))]
              (do
                (log/debug "Loaded virtual graph successfully:" alias)
                vg)
              ;; Neither ledger nor VG worked, throw original ledger error
              (throw (contextualize-ledger-400-error
                      (str "Error loading resource " alias ": ")
                      ledger-result))))
          ;; Original alias had ':', so it was meant to be a ledger - throw error
          (throw (contextualize-ledger-400-error
                  (str "Error loading ledger " alias ": ")
                  ledger-result)))))))

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
      (log/debug "query-connection-fql - from:" default-aliases "from-named:" named-aliases)
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
