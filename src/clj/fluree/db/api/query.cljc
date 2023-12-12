(ns fluree.db.api.query
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.core.async :as async]
            [fluree.json-ld :as json-ld]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.fuel :as fuel]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.query.dataset :as dataset]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.history :as history]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :as async-util :refer [<? go-try]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.validation :as v]))

#?(:clj (set! *warn-on-reflection* true))

(defn- history*
  [db query-map]
  (go-try
   (let [{:keys [opts]} query-map
         {:keys [history t commit-details] :as parsed} (history/parse-history-query query-map)

         ctx (some-> parsed ctx-util/extract-supplied-context json-ld/parse-context)
         db* (if-let [policy-identity (perm/parse-policy-identity opts ctx)]
               (<? (perm/wrap-policy db policy-identity))
               db)
         ;; from and to are positive ints, need to convert to negative or fill in default values
         {:keys [from to at]} t
         [from-t to-t] (if at
                         (let [t (cond (= :latest at) (:t db*)
                                       (string? at) (<? (time-travel/datetime->t db* at))
                                       (number? at) (- at))]
                           [t t])
                         ;; either (:from or :to)
                         [(cond (= :latest from) (:t db*)
                                (string? from) (<? (time-travel/datetime->t db* from))
                                (number? from) (- from)
                                (nil? from) -1)
                          (cond (= :latest to) (:t db*)
                                (string? to) (<? (time-travel/datetime->t db* to))
                                (number? to) (- to)
                                (nil? to) (:t db*))])

         context        (ctx-util/extract parsed)
         error-ch       (async/chan)]
     (if history
       ;; filter flakes for history pattern
       (let [[pattern idx] (<? (history/history-pattern db* context history))
             flake-slice-ch       (query-range/time-range db* idx = pattern {:from-t from-t :to-t to-t})
             flake-ch             (async/chan 1 cat)

             _                    (async/pipe flake-slice-ch flake-ch)

             flakes               (async/<! (async/into [] flake-ch))

             history-results-chan (history/history-flakes->json-ld db* context error-ch flakes)]

         (if commit-details
           ;; annotate with commit details
           (async/alt!
            (async/into [] (history/add-commit-details db* context error-ch history-results-chan))
            ([result] result)
            error-ch ([e] e))

           ;; we're already done
           (async/alt!
            (async/into [] history-results-chan) ([result] result)
            error-ch ([e] e))))

       ;; just commits over a range of time
       (let [flake-slice-ch    (query-range/time-range db* :tspo = [] {:from-t from-t :to-t to-t})
             commit-results-ch (history/commit-flakes->json-ld db* context error-ch flake-slice-ch)]
         (async/alt!
          (async/into [] commit-results-ch) ([result] result)
          error-ch ([e] e)))))))

(defn history
  "Return a summary of the changes over time, optionally with the full commit details included."
  [db query-map]
  (go-try
   (let [{query-map :subject, did :did} (or (<? (cred/verify query-map))
                                            {:subject query-map})
         coerced-query (try*
                         (history/coerce-history-query query-map)
                         (catch* e
                           (throw
                             (ex-info
                               (-> e
                                   v/explain-error
                                   (v/format-explained-errors nil))
                               {:status  400
                                :error   :db/invalid-query}))))
         history-query (cond-> coerced-query did (assoc-in [:opts :did] did))]
     (<? (history* db history-query)))))

(defn sanitize-query-options
  [opts did]
  (cond-> (util/parse-opts opts)
    did (assoc :did did :issuer did)))

(defn restrict-db
  [db t context opts]
  (go-try
    (let [db*  (if-let [policy-identity (perm/parse-policy-identity opts context)]
                 (<? (perm/wrap-policy db policy-identity))
                 db)
          db** (-> (if t
                     (<? (time-travel/as-of db* t))
                     db*))]
      (assoc-in db** [:policy :cache] (atom {})))))

(defn track-query
  [db max-fuel query]
  (go-try
    (let [start        #?(:clj  (System/nanoTime)
                          :cljs (util/current-time-millis))
          fuel-tracker (fuel/tracker max-fuel)]
      (try* (let [result (<? (fql/query db fuel-tracker query))]
              {:status 200
               :result result
               :time   (util/response-time-formatted start)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
                    (throw (ex-info "Error executing query"
                                    {:status (-> e ex-data :status)
                                     :time   (util/response-time-formatted start)
                                     :fuel   (fuel/tally fuel-tracker)}
                                    e)))))))

(defn query-fql
  "Execute a query against a database source. Returns core async channel
  containing result or exception."
  [db query]
  (go-try
    (let [{query :subject, did :did}  (or (<? (cred/verify query))
                                          {:subject query})
          {:keys [t opts] :as query*} (update query :opts sanitize-query-options did)

          ;; TODO: extracting query context here for policy only to do it later
          ;; while parsing the query. We need to consolidate both policy and
          ;; query parsing while cleaning up the query api call stack.
          q-ctx    (ctx-util/extract query*)
          db*      (<? (restrict-db db t q-ctx opts))
          query**  (update query* :opts dissoc   :meta :max-fuel ::util/track-fuel?)
          max-fuel (:max-fuel opts)]
      (if (::util/track-fuel? opts)
        (<? (track-query db* max-fuel query**))
        (<? (fql/query db* query**))))))

(defn query-sparql
  [db query]
  (go-try
    (let [fql (sparql/->fql query)]
      (<? (query-fql db fql)))))

(defn query
  [db query {:keys [format] :as _opts :or {format :fql}}]
  (case format
    :fql (query-fql db query)
    :sparql (query-sparql db query)))

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

(defn load-alias
  [conn alias t context opts]
  (go-try
   (try*
     (let [address (<? (nameservice/primary-address conn alias nil))
           ledger  (<? (jld-ledger/load conn address))
           db      (ledger-proto/-db ledger)]
       (<? (restrict-db db t context opts)))
     (catch* e
       (throw (contextualize-ledger-400-error
               (str "Error loading ledger " alias ": ")
               e))))))

(defn load-aliases
  [conn aliases global-t context opts]
  (when (some? global-t)
    (try*
      (util/str->epoch-ms global-t)
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
       (let [db      (<? (load-alias conn alias global-t context opts))
             db-map* (assoc db-map alias db)]
         (recur r db-map*))
       db-map))))

(defn load-dataset
  [conn defaults named global-t context opts]
  (go-try
    (if (and (= (count defaults) 1)
             (empty? named))
      (let [alias (first defaults)]
        (<? (load-alias conn alias global-t context opts))) ; return an
                                                            ; unwrapped db if
                                                            ; the data set
                                                            ; consists of one
                                                            ; ledger
      (let [all-aliases  (->> defaults (concat named) distinct)
            db-map       (<? (load-aliases conn all-aliases global-t context opts))
            default-coll (-> db-map
                             (select-keys defaults)
                             vals)
            named-map    (select-keys db-map named)]
        (dataset/combine named-map default-coll)))))

(defn query-connection-fql
  [conn query]
  (go-try
    (let [{query :subject, did :did} (or (<? (cred/verify query))
                                         {:subject query})
          {:keys [t opts] :as query*}  (update query :opts sanitize-query-options did)

          default-aliases (some-> query* :from util/sequential)
          named-aliases   (some-> query* :from-named util/sequential)]
      (if (or (seq default-aliases)
              (seq named-aliases))
        (let [s-ctx       (ctx-util/extract query)
              ds          (<? (load-dataset conn default-aliases named-aliases t
                                            s-ctx opts))
              query**     (update query* :opts dissoc :meta :max-fuel ::util/track-fuel?)
              max-fuel    (:max-fuel opts)]
          (if (::util/track-fuel? opts)
            (<? (track-query ds max-fuel query**))
            (<? (fql/query ds query**))))
        (throw (ex-info "Missing ledger specification in connection query"
                        {:status 400, :error :db/invalid-query}))))))


(defn query-connection-sparql
  [conn query]
  (go-try
    (let [fql (sparql/->fql query)]
      (<? (query-connection-fql conn fql)))))

(defn query-connection
  [conn query {:keys [format] :as _opts :or {format :fql}}]
  (case format
    :fql (query-connection-fql conn query)
    :sparql (query-connection-sparql conn query)))
