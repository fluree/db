(ns fluree.db.query.api
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.fuel :as fuel]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.ledger :as ledger]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.dataset :as dataset :refer [dataset?]]
            [fluree.db.query.fql :as fql]
            [fluree.db.util.log :as log]
            [fluree.db.query.history :as history]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :as async-util :refer [<? go-try]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.validation :as v]))

#?(:clj (set! *warn-on-reflection* true))

(defn q
  ([ds-or-db query]
   (fql/query ds-or-db query))
  ([ds-or-db fuel-tracker query]
   (fql/query ds-or-db fuel-tracker query)))

(defn history
  "Return a summary of the changes over time, optionally with the full commit
  details included."
  [db query]
  (go-try
    (let [context (ctx-util/extract query)]
      (<? (history/query db context query)))))

(defn sanitize-query-options
  [opts did]
  (cond-> (util/parse-opts opts)
    did (assoc :did did :issuer did)))

(defn db->policy-db
  [db context {:keys [did default-allow?] :as _opts}]
  (go-try
    (if did
      (<? (perm/wrap-identity-policy db did default-allow? nil))
      db)))

(defn db->time-travel-db
  [db t]
  (go-try
    (if t
      (<? (time-travel/as-of db t))
      db)))

(defn db->reasoned-db
  [db request-opts]
  (go-try
    (let [{:keys [reasoner-methods rule-graphs rule-dbs] :as reasoning} request-opts]
      (if reasoner-methods
        (<? (reasoner/reason db
                             reasoner-methods
                             reasoning
                             request-opts))
        db))))

(defn restrict-db
  [db t {:keys [did default-allow? reasoner-methods rule-sources] :as opts}]
  (go-try
    (let [policy-db      (if did
                           (<? (perm/wrap-identity-policy db did default-allow? nil))
                           db)
          time-travel-db (-> (if t
                               (<? (time-travel/as-of policy-db t))
                               policy-db))
          reasoned-db    (if reasoner-methods
                           (<? (reasoner/reason time-travel-db
                                                reasoner-methods
                                                rule-sources
                                                opts))
                           time-travel-db)]
      (assoc-in reasoned-db [:policy :cache] (atom {})))))
    
(defn track-query
  [ds max-fuel query]
  (go-try
    (let [start #?(:clj (System/nanoTime)
                   :cljs (util/current-time-millis))
          fuel-tracker  (fuel/tracker max-fuel)]
      (try* (let [result (<? (q ds fuel-tracker query))]
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

(defn query
  ([db query*]
   (query db query* nil))
  ([db query {:keys [format did] :as opts}]
   (go-try
     (let [fql-query   (if (= :sparql format)
                         (sparql/->fql query)
                         query)          
           
           {:keys [t opts] :as sanitized-query}
           (update fql-query :opts sanitize-query-options did)
           
           final-query (update sanitized-query :opts dissoc :meta :max-fuel ::util/track-fuel?)
           
           dataset     (if (dataset? db)
                         db
                         (as-> db $
                           (<? (db->policy-db $ (ctx-util/extract final-query) opts))
                           (<? (db->time-travel-db $ t))
                           (<? (db->reasoned-db $ opts))))]
       (if (::util/track-fuel? opts)
         (<? (track-query dataset (:max-fuel opts) final-query))
         (<? (q dataset final-query)))))))

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

(defn load-aliased-rule-dbs
  [conn rule-sources]
  (go-try
    (loop [[rule-source & rule-sources] rule-sources
           rule-results []]
      (when-let [rule-source (first rule-sources)]
        (if rule-source
          (into rule-results
                (if (string? rule-source)
                  (ledger/-db (<? (jld-ledger/load conn rule-source)))
                  rule-source))
          (recur rule-sources rule-results))))))

(defn load-alias
  [conn alias t opts]
  (go-try
    (try*
      (let [[alias explicit-t] (extract-query-string-t alias)
            address      (<? (nameservice/primary-address conn alias nil))
            ledger       (<? (jld-ledger/load conn address))
            db           (ledger/-db ledger)
            t*           (or explicit-t t)
            rule-sources (<? (load-aliased-rule-dbs conn (:rule-sources opts)))
            opts*        (assoc opts :rule-sources rule-sources)]
        (<? (restrict-db db t* opts*))) 
      (catch* e
              (throw (contextualize-ledger-400-error
                       (str "Error loading ledger " alias ": ")
                       e))))))

(defn load-aliases
  [conn aliases global-t opts]
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
       (let [db      (<? (load-alias conn alias global-t opts))
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
  [conn defaults named global-t opts]
  (go-try
    (if (and (= (count defaults) 1)
             (empty? named))
      (let [alias (first defaults)]
        (<? (load-alias conn alias global-t opts))) ; return an unwrapped db if
                                                    ; the data set consists of
                                                    ; one ledger
      (let [all-aliases (->> defaults (concat named) distinct)
            db-map      (<? (load-aliases conn all-aliases global-t opts))]
        (dataset db-map defaults)))))

(defn query-connection-fql
  [conn query did]
  (go-try
    (let [{:keys [t opts] :as sanitized-query} (-> query
                                                   syntax/coerce-query
                                                   (update :opts sanitize-query-options did))
          
          default-aliases (some-> sanitized-query :from util/sequential)
          named-aliases   (some-> sanitized-query :from-named util/sequential)]
      (if (or (seq default-aliases)
              (seq named-aliases))
        (let [ds            (<? (load-dataset conn default-aliases named-aliases t opts))
              trimmed-query (update sanitized-query :opts dissoc :meta :max-fuel ::util/track-fuel?)
              max-fuel      (:max-fuel opts)]
          (if (::util/track-fuel? opts)
            (<? (track-query ds max-fuel trimmed-query))
            (<? (q ds trimmed-query))))
        (throw (ex-info "Missing ledger specification in connection query"
                        {:status 400, :error :db/invalid-query}))))))

(defn query-connection-sparql
  [conn query did]
  (go-try
    (let [fql (sparql/->fql query)]
      (log/debug "query-connection SPARQL fql: " fql "did:" did)
      (<? (query-connection-fql conn fql did)))))

(defn query-connection
  [conn query {:keys [format did] :as opts :or {format :fql}}]
  (case format
    :fql (query-connection-fql conn query did)
    :sparql (query-connection-sparql conn query did)))
