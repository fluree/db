(ns fluree.db.api.query
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.fql.parse :as fql-parse]
            [fluree.db.query.range :as query-range]
            [fluree.db.session :as session]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.permissions :as permissions]
            [fluree.db.auth :as auth]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.fql.resp :refer [flakes->res]]
            [fluree.db.util.async :as async-util]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.json-ld :as json-ld]
            [fluree.db.db.json-ld :as jld-db]
            [malli.core :as m]))

#?(:clj (set! *warn-on-reflection* true))

;; main query interface for APIs, etc.


(declare query query-async)


(defn query-type
  "Returns keyword of query type by inspecting flureeQL query.

  Query types are:
    :standard - basic or analytical query
    :multi - multi-query
    :block - block query
    :history - history query"
  [flureeQL]
  (cond
    (:select flureeQL)
    :standard

    (:selectOne flureeQL)
    :standard

    (:history flureeQL)
    :history

    (:selectDistinct flureeQL)
    :standard

    (:selectReduced flureeQL)                               ;; SPARQL standard, no special treatment over :selectDistinct
    :standard

    (:block flureeQL)                                       ;; block checked last, as block is allowed in above query types
    :block

    ;; (:construct flureeQL) - we don't yet have support for SPARQL-like construct queries
    ;; :construct

    :else
    :multi))


(defn db-ident?
  [source]
  (= (-> source (str/split #"/") count) 2))


(defn- isolate-ledger-id
  [ledger-id]
  (re-find #"[a-z0-9]+/[a-z0-9]+" ledger-id))


(defn db
  "Returns a queryable database as a promise channel from the connection for the specified ledger."
  ([conn ledger]
   (session/db conn ledger nil))
  ([conn ledger opts]
   (let [pc (async/promise-chan)]
     (async/go
       (try*
         (let [rootdb        (<? (session/db conn ledger nil))
               {:keys [roles user auth block]} opts
               auth_id       (when (and auth (not= 0 auth))
                               (or
                                 (<? (dbproto/-subid rootdb auth))
                                 (throw (ex-info (str "Auth id: " auth " unknown.")
                                                 {:status 401
                                                  :error  :db/invalid-auth}))))
               roles         (or roles (if auth_id
                                         (<? (auth/roles rootdb auth_id)) nil))

               permissions-c (when roles (permissions/permission-map rootdb roles :query))
               dbt           (if block
                               (<? (time-travel/as-of-block rootdb (:block opts)))
                               rootdb)
               dba           (if auth
                               (assoc dbt :auth auth)
                               dbt)
               permdb        (if roles
                               (assoc dba :permissions (<? permissions-c))
                               dba)]
           (async/put! pc permdb))
         (catch* e
                 (async/put! pc e)
                 (async/close! pc))))
     ;; return promise chan immediately
     pc)))


(defn- get-sources
  [conn network auth prefixes]
  (reduce-kv (fn [acc key val]
               (when-not (re-matches #"[a-z]+" (util/keyword->str key))
                 (throw (ex-info (str "Source name must be only lowercase letters. Provided: " (util/keyword->str key))
                                 {:status 400
                                  :error  :db/invalid-query})))
               (let [db-ident? (db-ident? val)]
                 (if db-ident?
                   (let [ledger (isolate-ledger-id val)
                         opts   (if auth {:auth auth} {})
                         db     (db conn ledger opts)]
                     (assoc acc val db))
                   acc))) {} prefixes))


(defn resolve-block-range
  [db query-map]
  (go-try
    (let [range     (if (sequential? (:block query-map))
                      (:block query-map)
                      [(:block query-map) (:block query-map)])
          [block-start block-end]
          (if (some string? range)                          ;; do we need to convert any times to block integers?
            [(<? (time-travel/block-to-int-format db (first range)))
             (when-let [end (second range)]
               (<? (time-travel/block-to-int-format db end)))] range)
          db-block  (:block db)
          _         (when (> block-start db-block)
                      (throw (ex-info (str "Start block is out of range for this ledger. Start block provided: " (pr-str block-start) ". Database block: " (pr-str db-block)) {:status 400 :error :db/invalid-query})))
          [block-start block-end]
          (cond
            (and block-start block-end) [block-start block-end]
            block-start [block-start (:block db)]
            :else (throw (ex-info (str "Invalid block range provided: " (pr-str range)) {:status 400 :error :db/invalid-query})))
          _         (when (not (and (pos-int? block-start) (pos-int? block-end)))
                      (throw (ex-info (str "Invalid block range provided: " (pr-str range)) {:status 400 :error :db/invalid-query})))
          [block-start block-end]
          (if (< block-end block-start)
            [block-end block-start]                         ;; make sure smallest number comes first
            [block-start block-end])
          block-end (if (> block-end db-block)
                      db-block block-end)]
      [block-start block-end])))

(defn history-flakes->json-ld
  [db q flakes]
  (go-try
    (let [fuel    (volatile! 0)
          cache   (volatile! {})
          compact (json-ld/compact-fn (fql-parse/parse-context q db))]
      (->> (sort-by flake/t flakes)
           (partition-by flake/t)
           (map (fn [t-flakes]
                  (go-try
                    (let [assert-flakes (not-empty (filter flake/op t-flakes))
                          retract-flakes (not-empty (filter (complement flake/op) t-flakes))

                          asserts (<? (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                                {:wildcard? true, :depth 0} 0 assert-flakes))
                          retracts (<? (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                                 {:wildcard? true, :depth 0}
                                                                 0 retract-flakes))]

                      ;; t is always positive for users
                      (cond-> {:t (- (flake/t (first t-flakes)))}
                        asserts (assoc :assert asserts)
                        retracts (assoc :retract retracts))))))
           (async/merge)
           (async/into [])
           (<?)
           (sort-by :t)))))

(defn get-history-pattern
  [history]
  (let [[s p o t]     [(get history 0) (get history 1) (get history 2) (get history 3)]
        [pattern idx] (cond
                        (not (nil? s))
                        [history :spot]

                        (and (nil? s) (not (nil? p)) (nil? o))
                        [[p s o t] :psot]

                        (and (nil? s) (not (nil? p)) (not (nil? o)))
                        [[p o s t] :post])]
    [pattern idx]))

(def History
  [:map {:registry {::iri [:or :keyword :string]
                    ::context [:map-of :any :any]}}
   [:history
    [:orn
     [:subject ::iri]
     [:flake
      [:or
       [:catn
        [:s ::iri]]
       [:catn
        [:s [:maybe ::iri]]
        [:p ::iri]]
       [:catn
        [:s [:maybe ::iri]]
        [:p ::iri]
        [:o [:not :nil]]]]]]]
   [:context {:optional true} ::context]
   [:t {:optional true}
    [:and
     [:map
      [:from {:optional true} pos-int?]
      [:to {:optional true} pos-int?]]
     [:fn (fn [{:keys [from to] :as x}] (or from to))]]]])

(defn history-query?
  "A history query requires a :history key and a either a subject iri or a vector in the
  pattern [s p o] with either the s or the p is required. If the o is supplied it must not be nil."
  [query]
  (m/validate History query))

(defn history
  [db query-map]
  (go-try
    (if-not (history-query? query-map)
      (throw (ex-info (str "History query not properly formatted. Provided "
                           (pr-str query-map))
                      {:status 400
                       :error  :db/invalid-query}))

      (let [{:keys [history t context]} (m/parse History query-map)

            ;; parses to [:subject <:id>] or [:flake {:s <> :p <> :o <>}]}
            [query-type parsed-query] history

            {:keys [s p o]} (if (= :subject query-type)
                              {:s parsed-query}
                              parsed-query)

            query [(when s (<? (dbproto/-subid db s true)))
                   (when p (jld-db/expand-iri db p context))
                   (when o (jld-db/expand-iri db o context))]

            [pattern idx] (get-history-pattern query)

            ;; from and to are positive ints, need to convert to negative or fill in default values
            {:keys [from to]}  t
            [from-t to-t]      [(if from (- from) -1) (if to (- to) (:t db))]

            flakes  (<? (query-range/time-range db idx = pattern {:from-t from-t :to-t to-t}))
            results (<? (history-flakes->json-ld db query-map flakes))]
        results))))

(defn query-async
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns core async channel containing result."
  [sources query]
  (go-try
    (let [{query :subject issuer :issuer} (or (<? (cred/verify query))
                                              {:subject query})
          {:keys [select selectOne selectDistinct selectReduced construct
                  from where prefixes opts t]} query
          db            (if (async-util/channel? sources) ;; only support 1 source currently
                          (<? sources)
                          sources)
          db*           (-> (if t
                              (<? (time-travel/as-of db t))
                              db)
                            (assoc-in [:permissions :cache] (atom {})))
          source-opts   (if prefixes
                          (get-sources (:conn db*) (:network db*) (:auth-id db*) prefixes)
                          {})
          meta?         (:meta opts)
          fuel          (when (or (:fuel opts) meta?) (volatile! 0)) ;; only measure fuel if fuel budget provided, or :meta true
          opts*         (assoc opts :sources source-opts
                               :max-fuel (or (:fuel opts) 1000000)
                               :fuel fuel
                               :issuer issuer)
          _             (when-not (and (or select selectOne selectDistinct selectReduced construct)
                                       (or from where))
                          (throw (ex-info (str "Invalid query.")
                                          {:status 400
                                           :error  :db/invalid-query})))
          start #?(:clj (System/nanoTime) :cljs (util/current-time-millis))
          result        (<? (fql/query db* (assoc query :opts opts*)))]
      (if meta?
        {:status 200
         :result result
         :fuel   @fuel
         :time   (util/response-time-formatted start)
         :block  (:block db*)}
        result))))


(defn multi-query-async
  "Performs multiple queries in a map, with the key being the alias for the query
  and the value being the query itself - standard, history, and block queries are all supported.
  Each query result will be in a response map with its respective alias as the key.

  If a :block is specified at the top level, it will be used as a default for all queries

  If any errors occur, an :errors key will be present with a map of each alias to its error
  information. Check for the presence of this key if detection of an error is important.

  An optional :opts key contains options, which for now is limited to:
   - meta: true or false - If false, will just report out the result as a map.
           If true will roll up all status and fuel consumption. Response map will contain keys:
           - status - aggregate status (200 all good, 207 some good, or 400+ for differing errors
           - fuel   - aggregate fuel for all queries
           - result - query result
           - errors - map of query alias to their respective error"
  [source flureeQL]
  (async/go
    (try*
      (let [global-block       (:block flureeQL)            ;; use as default for queries
            global-meta?       (get-in flureeQL [:opts :meta]) ;; if true, need to collect meta for each query to total up
            ;; update individual queries for :meta and :block if not otherwise specified
            queries            (reduce-kv
                                 (fn [acc alias query]
                                   ;; block globally to all sub-queries unless already specified
                                   (let [query-meta?  (get-in query [:opts :meta])
                                         meta?        (or global-meta? query-meta?)
                                         remove-meta? (and meta? (not query-meta?)) ;; query didn't ask for meta, but multiquery did so must strip it

                                         opts*        (assoc (:opts query) :meta meta?
                                                                           :_remove-meta? remove-meta?)
                                         query*       (assoc query :opts opts*
                                                                   :block (or (:block query) global-block))]
                                     (assoc acc alias query*)))
                                 {} (dissoc flureeQL :opts :block))
            start-time #?(:clj (System/nanoTime) :cljs (util/current-time-millis))
            ;; kick off all queries in parallel, each alias now mapped to core async channel
            pending-resp       (map (fn [[alias q]] [alias (query source q)]) queries)]
        (loop [[[alias port] & r] pending-resp
               status-global nil                            ;; overall status.
               fuel-global   0
               response      {}]
          (if (nil? port)                                   ;; done?
            (if global-meta?
              {:result response
               :fuel   fuel-global
               :status status-global
               :time   (util/response-time-formatted start-time)}
              response)
            (let [{:keys [meta _remove-meta?]} (get-in queries [alias :opts])
                  res            (async/<! port)
                  error?         (:error res)               ;; if error key is present in response, it is an error
                  status-global* (when meta
                                   (let [status (:status res)]
                                     (cond
                                       (nil? status-global)
                                       status

                                       (= status-global status)
                                       status

                                       ;; any 200 response with any other is a 207
                                       (or (= 200 status) (= 200 status-global) (= 207 status-global))
                                       207

                                       ;; else take the max status
                                       :else
                                       (max status status-global))))
                  fuel*          (when meta (+ fuel-global (get res :fuel 0)))
                  response*      (if error?
                                   (assoc-in response [:errors alias] res)
                                   (assoc response alias (if _remove-meta?
                                                           (:result res)
                                                           res)))]
              (recur r status-global* fuel* response*)))))
      (catch* e e))))

(defn query
  "Generic query interface. Will determine if multi-query, standard query, block or history
  and dispatch appropriately.

  For now, sources is expected to be just a db. In the case of a block query, which requires
  a conn + ledger, those will be extracted from the db."
  [source flureeQL]
  (let [query-type (query-type flureeQL)]
    (case query-type
      :standard (query-async source flureeQL)
      :history (history source flureeQL)
      :multi (multi-query-async source flureeQL))))
