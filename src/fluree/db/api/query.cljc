(ns fluree.db.api.query
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.string :as str]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.time-travel :as time-travel]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.block :as query-block]
            [fluree.db.session :as session]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.permissions :as permissions]
            [fluree.db.auth :as auth]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]))

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
  [dbid]
  (re-find #"[a-z0-9]+/[a-z0-9]+" dbid))


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


(defn- format-block-resp-pretty
  [db curr-block cache fuel]
  (go-try (let [[asserted-subjects
                 retracted-subjects] (loop [[flake & r] (:flakes curr-block)
                                            asserted-subjects  {}
                                            retracted-subjects {}]
                                       (if-not flake
                                         [asserted-subjects retracted-subjects]
                                         (let [subject   (flake/s flake)
                                               asserted? (true? (flake/op flake))
                                               flake'    (if asserted? flake
                                                                       (flake/flip-flake flake))]
                                           (if asserted?
                                             (recur r (update asserted-subjects subject #(vec (conj % flake')))
                                                    retracted-subjects)
                                             (recur r asserted-subjects
                                                    (update retracted-subjects subject #(vec (conj % flake'))))))))
                retracted (loop [[subject & r] (vals retracted-subjects)
                                 acc []]
                            (if-not subject
                              acc
                              (recur r (conj acc (<? (fql/flakes->res db cache fuel 1000000 {:wildcard? true, :select {}} subject))))))
                asserted  (loop [[subject & r] (vals asserted-subjects)
                                 acc []]
                            (if-not subject
                              acc
                              (recur r (conj acc (<? (fql/flakes->res db cache fuel 1000000 {:wildcard? true, :select {}} subject))))))]
            {:block     (:block curr-block)
             :t         (:t curr-block)
             :retracted retracted
             :asserted  asserted})))


(defn- format-blocks-resp-pretty
  [db resp]
  (async/go-loop [fuel (volatile! 0)
                  cache (volatile! {})
                  curr-block (first resp)
                  rest-blocks (rest resp)
                  acc []]
    (let [curr-block' (<? (format-block-resp-pretty db curr-block cache fuel))
          acc'        (concat acc [curr-block'])]
      (if (first rest-blocks)
        (recur fuel cache (first rest-blocks) (rest rest-blocks) acc')
        acc'))))




(defn block-range
  "Returns a core async channel of blocks from start block (inclusive) to end if provided (exclusive).
  Each block is a separate map, containing keys :block, :t and :flakes.
  Channel is lazy, continue to take! values as needed."
  ([db start] (block-range db start nil nil))
  ([db start end] (block-range db start end nil))
  ([db start end opts]
   (query-block/block-range db start end opts)))


(defn block-query-async
  [conn ledger {:keys [opts] :as query}]
  (go-try
    (let [query-map     (dissoc query :opts)
          auth-id       (:auth opts)
          start #?(:clj (System/nanoTime) :cljs (util/current-time-millis))
          db            (<? (db conn ledger {:auth (when auth-id ["_auth/id" auth-id])}))
          [block-start block-end] (<? (resolve-block-range db query-map))
          result        (if (= '(:block) (keys (dissoc query-map :pretty-print :opts :prettyPrint)))
                          (<? (block-range db block-start block-end opts))
                          (throw (ex-info (str "Block query not properly formatted. It must only have a block key. Provided "
                                               (pr-str query-map))
                                          {:status 400
                                           :error  :db/invalid-query})))
          result'       (if (or (:prettyPrint query-map) (:pretty-print query-map))
                          (<? (format-blocks-resp-pretty db result))
                          result)]
      (if (:meta opts)
        {:status 200
         :result (if (sequential? result')
                   (doall result')
                   result')
         :fuel   100
         :time   (util/response-time-formatted start)}
        result'))))


(defn get-history-pattern
  [history]
  (let [subject (cond (util/subj-ident? history)
                      [history]

                      (sequential? history)
                      (if (empty? history)
                        (throw (ex-info (str "Please specify an subject for which to search history. Provided: " history)
                                        {:status 400
                                         :error  :db/invalid-query}))
                        history)

                      :else
                      (throw (ex-info (str "History query not properly formatted. Provided: " history)
                                      {:status 400
                                       :error  :db/invalid-query})))
        [s p o t] [(get subject 0) (get subject 1) (get subject 2) (get subject 3)]

        [pattern idx] (cond
                        (not (nil? s))
                        [subject :spot]

                        (and (nil? s) (not (nil? p)) (nil? o))
                        [[p s o t] :psot]

                        (and (nil? s) (not (nil? p)) (not (nil? o)))
                        [[p o s t] :post]

                        :else
                        (throw (ex-info (str "History query not properly formatted. Must include at least an subject or predicate to query. Provided: " history)
                                        {:status 400
                                         :error  :db/invalid-query})))]
    [pattern idx]))


(defn- auth-match
  [auth-set t-map flake]
  (let [[auth id] (get-in t-map [(flake/t flake) :auth])]
    (or (auth-set auth)
        (auth-set id))))


(defn- min-safe
  [& args]
  (->> (remove nil? args) (apply min)))


(defn- format-history-resp
  [db resp auth show-auth]
  (go-try
    (let [ts    (set (map #(flake/t %) resp))
          t-map (<? (async/go-loop [[t & r] ts
                                    acc {}]
                      (if t
                        (let [block (<? (time-travel/non-border-t-to-block db t))
                              acc*  (cond-> (assoc-in acc [t :block] block)
                                            (or auth show-auth) (assoc-in [t :auth]
                                                                          (<? (query-async
                                                                                (go-try db)
                                                                                {:selectOne ["?auth" "?id"],
                                                                                 :where     [[t, "_tx/auth", "?auth"],
                                                                                             ["?auth", "_auth/id", "?id"]]}))))]
                          (recur r acc*)) acc)))
          res   (loop [[flake & r] resp
                       acc {}]
                  (cond (and flake auth
                             (not (auth-match auth t-map flake)))
                        (recur r acc)

                        flake
                        (let [t   (flake/t flake)
                              {:keys [block auth]} (get t-map t)
                              acc (cond-> acc
                                          true (assoc-in [block :block] block)
                                          true (update-in [block :flakes] conj flake)
                                          true (update-in [block :t] min-safe t)
                                          show-auth (assoc-in [block :auth] auth))]
                          (recur r acc))

                        :else
                        acc))]
      (vals res))))

#?(:cljs
   (defn block-Flakes->vector
     "Convert flakes into vectors.
     Notes:
     Cannot use IPrintWithWriter override since calls to storage-handler
     download blocks using the #Flake format to support internal query
     handling."
    [blocks]
    (mapv (fn [block] (assoc block :flakes (mapv vec (:flakes block)))) blocks)))

(defn history-query-async
  [sources query-map]
  (go-try
    (let [{:keys [block history pretty-print prettyPrint show-auth showAuth auth opts]} query-map
          db     (<? sources)                               ;; only support 1 source currently

          [block-start block-end] (if block (<? (resolve-block-range db query-map)))
          result (if (contains? query-map :history)
                   (let [meta?    (:meta opts)
                         ;; From-t is the higher number, meaning it is the older time
                         ;; To-t is the lower number, meaning it is the newer time
                         ;; time-range is inclusive
                         from-t   (if (and block-start (not= 1 block-start))
                                    (dec (:t (<? (time-travel/as-of-block db (dec block-start))))) -1)
                         to-t     (if block-end
                                    (:t (<? (time-travel/as-of-block db block-end))) (:t db))
                         [pattern idx] (get-history-pattern history)
                         flakes   (<? (query-range/time-range db idx = pattern {:from-t from-t
                                                                                :to-t   to-t}))
                         auth-set (if auth (set auth) nil)
                         resp     (<? (format-history-resp db flakes auth-set (or showAuth show-auth)))
                         resp'    (if (or prettyPrint pretty-print)
                                    (<? (format-blocks-resp-pretty db resp))
                                    #?(:clj  resp
                                       :cljs (block-Flakes->vector resp)))]
                     (if meta? {:result resp'
                                :fuel   (count flakes)
                                :status 200}
                               resp'))
                   (throw (ex-info (str "History query not properly formatted. Provided "
                                        (pr-str query-map))
                                   {:status 400
                                    :error  :db/invalid-query})))] result)))


(defn query-async
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns core async channel containing result."
  [sources flureeQL]
  (go-try
    (let [{:keys [select selectOne selectDistinct selectReduced from where construct block prefixes opts]} flureeQL
          _             (when-not (->> [select selectOne selectDistinct selectReduced]
                                       (remove nil?) count (#(= 1 %)))
                          (throw (ex-info (str "Only one type of select-key (select, selectOne, selectDistinct, selectReduced) allowed. Provided: " (pr-str flureeQL))
                                          {:status 400
                                           :error  :db/invalid-query})))
          db            sources                             ;; only support 1 source currently
          db*           (if block (<? (time-travel/as-of-block (<? db) block)) (<? db))
          source-opts   (if prefixes
                          (get-sources (:conn db*) (:network db*) (:auth db*) prefixes)
                          {})
          meta?         (:meta opts)
          fuel          (when (or (:fuel opts) meta?) (volatile! 0)) ;; only measure fuel if fuel budget provided, or :meta true
          opts*         (assoc opts :sources source-opts
                                    :max-fuel (or (:fuel opts) 1000000)
                                    :fuel fuel)
          _             (when-not (and (or select selectOne selectDistinct selectReduced construct) (or from where))
                          (throw (ex-info (str "Invalid query.")
                                          {:status 400
                                           :error  :db/invalid-query})))
          start #?(:clj (System/nanoTime) :cljs (util/current-time-millis))
          result        (<? (fql/query db* (assoc flureeQL :opts opts*)))]
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
      :history (history-query-async source flureeQL)
      :block (let [conn   (:conn source)
                   ledger (keyword (:network source) (:dbid source))]
               (block-query-async conn ledger flureeQL))
      :multi (multi-query-async source flureeQL))))
