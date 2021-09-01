(ns fluree.db.api-js
  (:require [clojure.string :as str]
            [cljs.core.async :refer [go <!] :as async]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.operations :as ops]
            [fluree.db.query.block :as query-block]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.fql :as fql]
            [fluree.db.session :as session]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [fluree.db.connection-js :as conn-handler]))

;; supporting code for JS APIS (webworker, flureedb (both browser and nodejs)

;; ======================================
;;
;; Ledger Operations
;;
;; ======================================
; This function is not used by nodejs
; nodejs has its own version that incorporates permissions,
; smart functions, etc
(defn db
  "Returns a queryable database from the connection.
   No permissions applied - assumes root"
  [conn ledger & [opts]]
  (let [pc (async/promise-chan)]
    (async/go
      (try
        (let [{:keys [roles user auth block]} opts
              _       (conn-handler/check-connection conn opts)
              [network ledger-id] (session/resolve-ledger conn ledger)
              root-db (-> (<? (session/db conn ledger opts))
                          (assoc :conn conn :network network :dbid ledger-id))
              dbt     (if block
                        (<? (time-travel/as-of-block root-db block))
                        root-db)]
          (async/put! pc dbt))
        (catch :default e
          (log/error e)
          (async/put! pc e)
          (async/close! pc))))
    ;; return promise chan immediately
    pc))

(defn- db-ident?
  [source]
  (= (-> source (str/split #"/") count) 2))

(defn- isolate-ledger-id
  [dbid]
  (re-find #"[a-z0-9]+/[a-z0-9]+" dbid))

(defn- get-sources
  "Validates & returns the query sources.

  The db function, to evaluate prefixes/multiple sources, is passed
  as a parameter.  This allows for the node.js version to override the
  default [fluree.db.api-js.db] with its own function.
  "
  [conn network open-api auth prefixes db-fn]
  (reduce-kv (fn [acc key val]
               (when-not (re-matches #"[a-z]+" (util/keyword->str key))
                 (throw (ex-info (str "Source name must be only lowercase letters. Provided: " (util/keyword->str key))
                                 {:status 400
                                  :error  :db/invalid-query})))
               ;; Either open-api is true and the ledger is in the same network
               (cond (and (db-ident? val) open-api (= network (-> val (str/split #"/") first)))
                     (let [db-id  (isolate-ledger-id val)
                           opts   (if auth {:auth auth} {})
                           ledger (apply db-fn conn db-id opts)]
                       (assoc acc val ledger))

                     (and (db-ident? val) open-api)
                     (throw (ex-info "When attempting to query across multiple databases in different networks, you must be using a closed API."
                                     {:status 400
                                      :error  :db/invalid-query}))

                     ;; Or we're using a closed-api
                     (and (db-ident? val) auth)
                     (let [db-id  (isolate-ledger-id val)
                           ledger (apply db-fn conn db-id {:auth auth})]
                       (assoc acc val ledger))

                     :else
                     acc)) {} prefixes))


;; ======================================
;;
;; Core
;;
;; ======================================
(defn query-async
  "Execute an unsigned query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns a channel, where the results are eventually put.

  The db function, to evaluate prefixes/multiple sources, is passed
  as a parameter.  This allows for the node.js version to override the
  default [fluree.db.api-js.db] with its own function.
  "
  ([sources query-map] (query-async sources query-map db))
  ([sources query-map db-fn]
   (async/go
     (try
       (let [{:keys [select selectOne selectDistinct selectReduced from where construct block prefixes opts]} query-map
             db           (<? sources)                      ;; only support 1 source currently
             db*          (if block (<? (time-travel/as-of-block db block)) db)
             conn         (:conn db*)
             source-opts  (if prefixes
                            (get-sources conn (:network db*) (conn-handler/open-api? conn) (:auth db*) prefixes db-fn)
                            {})
             fuel         (volatile! 0)
             max-fuel     (or (:fuel opts) 1000000)
             meta?        (:meta opts)
             start-ms     (util/current-time-millis)
             opts         (assoc opts :sources source-opts
                                      :max-fuel max-fuel
                                      :fuel fuel)
             valid-query? (and (or select selectOne selectDistinct selectReduced construct) (or from where))]
         (if-not valid-query?
           {:message "Invalid query."
            :status  400
            :error   :db/invalid-query}
           (let [result (<? (fql/query db* (assoc query-map :opts opts)))
                 error? (instance? ExceptionInfo result)]
             (cond
               error?
               (let [err-data (ex-data result)]
                 {:status  (:status err-data)
                  :message (ex-message result)
                  :error   :db/invalid-query})

               meta?
               {:status 200
                :result (if (sequential? result)
                          (doall result) result)
                :fuel   @fuel
                :time   (util/response-time-formatted start-ms)
                :block  (:block db*)}

               :else
               result))))
       (catch :default e
         (log/error e)
         (assoc (ex-data e) :message (ex-message e)))))))


(defn monitor-tx
  "Monitors a database for a specific transaction id included in a block.

  Returns a core async channel that will eventually contain a response,
  or will close after the timeout has expired.

  Response may contain an exception, if the tx resulted in an exception."
  [conn ledger tid timeout-ms]
  (assert (int? timeout-ms) "monitor-tx requires timeout to be provided in milliseconds as an integer.")
  (let [session      (session/session conn ledger)
        key          (random-uuid)
        resp-chan    (async/chan)
        timeout-chan (async/timeout timeout-ms)]
    ;; when we get a result, just put the result on the pending resp-chan
    (session/monitor-tx session tid key #(async/put! resp-chan %))
    ;; if timeout returns first, un-register the callback function to clean up and close channel by returning nil
    (async/go
      (let [[res chan] (async/alts! [resp-chan timeout-chan])]
        (if (= timeout-chan chan)
          (do
            (session/monitor-tx-remove session tid key)
            {:txid    tid
             :status  408
             :message (str "Timeout of " timeout-ms " ms for reached without transaction being included in new block. Transaction is still being processed. To view transaction results, issue: {\"select\": [\"*\"], \"from\": [\"_tx/id\", \"" tid "\" ]}")})
          res)))))


(defn- tx->command
  "Helper function to fill out the parts of the transaction that are incomplete,
  producing a signed command.

  Optional opts is a map with the following keys. If not provided,
  defaults will be attempted.
  - auth        - The auth id for the auth record being used. The private key must
                  correspond to this auth record, or an authority of this auth record.
  - expire      - When this transaction should expire if not yet attempted.
                  Defaults to 5 minutes.
  - nonce       - Any long/64-bit integer value that will make this transaction unique.
                  By default epoch milliseconds is used.
  - deps        - Not yet implemented, list of dependent transactions.

  If successful, will return a map with four keys:
    - cmd  - a map with the command/transaction data as a JSON string
    - sig  - the signature of the above stringified map
    - id   - the ID for this unique request - in case you want to look it up later, sha3 of 'cmd'
    - db   - the ledger for this transaction"
  ([ledger txn private-key] (tx->command ledger txn private-key nil))
  ([ledger txn private-key opts]
   (when-not private-key
     (throw (ex-info "Private key not provided and no default present on connection"
                     {:status 400 :error :db/invalid-transaction})))
   (let [db-name     (if (sequential? ledger)
                       (str (first ledger) "/$" (second ledger))
                       ledger)
         {:keys [auth expire nonce deps]} opts
         _           (when deps (assert (sequential? deps)
                                        "Command/transaction 'deps', when provided, must be a sequential list/array."))
         key-auth-id (crypto/account-id-from-private private-key)
         [auth authority] (cond
                            (and auth (not= auth key-auth-id))
                            [auth key-auth-id]

                            auth
                            [auth nil]

                            :else
                            [key-auth-id nil])
         timestamp   (util/current-time-millis)
         nonce       (or nonce timestamp)
         expire      (or expire (+ timestamp 30000))        ;; 5 min default
         cmd         (try (-> {:type      :tx
                               :db        db-name
                               :tx        txn
                               :nonce     nonce
                               :auth      auth
                               :authority authority
                               :expire    expire
                               :deps      deps}
                              (util/without-nils)
                              (json/stringify))
                          (catch :default e _
                                            (throw (ex-info (str "Transaction contains data that cannot be serialized into JSON.")
                                                            {:status 400 :error :db/invalid-tx}))))
         sig         (crypto/sign-message cmd private-key)
         id          (crypto/sha3-256 cmd)]
     {:cmd cmd
      :sig sig
      :id  id
      :db  ledger})))


(defn transact-async
  ([conn ledger txn] (transact-async conn ledger txn nil))
  ([conn ledger txn opts]
   (async/go
     (try
       (let [{:keys [private-key txid-only timeout auth nonce deps expire jwt]
              :or   {timeout 60000
                     nonce   (util/current-time-millis)}} opts]
         (if private-key
           ;; private key, so generate command locally and submit signed command
           (let [command      (tx->command ledger txn private-key opts)
                 txid         (:id command)
                 persist-resp (ops/command-async conn command)
                 result       (if txid-only
                                persist-resp
                                (async/<! (monitor-tx conn ledger txid timeout)))]
             result)
           ;; no private key provided, request ledger to sign request
           (let [tx-map (util/without-nils
                          {:db     ledger
                           :tx     txn
                           :auth   auth
                           :nonce  nonce
                           :deps   deps
                           :expire expire
                           :jwt    jwt})]
             ;; will received txid once transaction is persisted, else an error
             (let [txid (async/<! (ops/transact-async conn tx-map))]
               (if txid-only
                 txid
                 ;; tx is persisted, monitor for txid
                 (let [tx-result (async/<! (monitor-tx conn ledger txid timeout))
                       error?    (instance? ExceptionInfo tx-result)]
                   (if error?
                     (let [err-data (ex-data tx-result)]
                       {:status  (:status err-data)
                        :message (ex-message tx-result)
                        :error   :db/invalid-tx})
                     tx-result)))))))
       (catch :default e
         (log/error e)
         (assoc (ex-data e) :message (ex-message e)))))))


;; ------------------------
;; Implementation - private
;; ------------------------
(defn- min-safe
  [& args]
  (->> (remove nil? args) (apply min)))

(defn- format-flake-groups-pretty
  [db cache fuel flakes]
  (async/go
    (loop [flake-group (first flakes)
           rest-flakes (rest flakes)
           acc         []]
      ; flakes->res should never be called as a test from format-flake-group-pretty, as this is for block and history
      (let [flake-res (<? (fql/flakes->res db cache fuel 1000000 {:wildcard? true, :select {}} flake-group))
            acc'      (concat acc [flake-res])]
        (if (first rest-flakes)
          (recur (first rest-flakes) (rest rest-flakes) acc')
          acc')))))

(defn- format-block-resp-pretty
  [db resp]
  (async/go
    (loop [fuel        (volatile! 0)
           cache       (volatile! {})
           curr-block  (first resp)
           rest-blocks (rest resp)
           acc         []]
      (let [flakes      (:flakes curr-block)
            asserted    (filter #(.-op %) flakes)
            asserted'   (if (not (empty? asserted))
                          (->> asserted
                               (group-by #(.-s %))
                               (vals)
                               (format-flake-groups-pretty db cache fuel)
                               (<?))
                          nil)
            retracted   (filter #(false? (.-op %)) flakes)
            retracted'  (if (not (empty? retracted))
                          (->> retracted
                               (map flake/flip-flake)
                               (group-by #(.-s %))
                               (vals)
                               (format-flake-groups-pretty db cache fuel)
                               (<?))
                          nil)
            flakes'     {:asserted asserted' :retracted retracted'}
            curr-block' (assoc curr-block :flakes flakes')
            acc'        (concat acc [curr-block'])]
        (if (first rest-blocks)
          (recur fuel cache (first rest-blocks) (rest rest-blocks) acc')
          acc')))))

(defn- format-history-resp
  [db resp]
  (go-try (let [ts    (-> (map #(.-t %) resp) set)
                t-map (<? (async/go
                            (loop [[t & r] ts
                                   acc {}]
                              (if t
                                (let [block (<? (time-travel/non-border-t-to-block db t))
                                      acc*  (assoc acc t block)]
                                  (recur r acc*)) acc))))
                resp  (-> (loop [[flake & r] resp
                                 acc {}]
                            (if flake
                              (let [t     (.-t flake)
                                    block (get t-map t)
                                    acc   (assoc-in acc [block :block] block)
                                    acc'  (update-in acc [block :flakes] conj flake)
                                    acc'' (update-in acc' [block :t] min-safe t)]
                                (recur r acc'')) acc)) vals)] resp)))


(defn- resolve-block-range
  "Returns an asynchronous channel that eventually contains the start and end block for a query
  or an error."
  [db query-map]
  (async/go
    (try
      (let [range     (if (sequential? (:block query-map))
                        (:block query-map)
                        [(:block query-map) (:block query-map)])
            [block-start block-end]
            (if (some string? range)                        ;; do we need to convert any times to block integers?
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
              [block-end block-start]                       ;; make sure smallest number comes first
              [block-start block-end])
            block-end (if (> block-end db-block)
                        db-block block-end)]
        [block-start block-end])
      (catch :default e
        (log/error e)
        (assoc (ex-data e) :message (ex-message e))))))

;; ======================================
;;
;; Queries - Block, History, Multi-
;;
;; ======================================
(defn block-query-async
  "Returns an asynchronous channel that eventually returns the results or an error.
  "
  ([db query-map] (block-query-async db query-map nil))
  ([db query-map opts]
   (async/go
     (try
       (let [start   (util/current-time-millis)
             {:keys [network db-id]} db
             range   (<? (resolve-block-range db query-map))
             _       (when
                       (and (map? range) (:error range))
                       (let [msg (or (:message range)
                                     (str "Unknown error attempting to resolve block range for ledger " network "/" db-id))]
                         (throw (ex-info msg range))))
             [block-start block-end] range
             result  (if (= '(:block) (keys (dissoc query-map :pretty-print)))
                       (<? (query-block/block-range db block-start block-end opts))
                       (throw (ex-info (str "Block query not properly formatted. It must only have a block key. Provided "
                                            (pr-str query-map))
                                       {:status 400
                                        :error  :db/invalid-query})))
             result' (if (:pretty-print query-map)
                       (<? (format-block-resp-pretty db result))
                       result)
             result* (if (:meta opts)
                       {:status 200
                        :result (if (sequential? result')
                                  (doall result')
                                  result')
                        :fuel   100
                        :time   (util/response-time-formatted start)}
                       result')]
         result*)
       (catch :default e
         (log/error e)
         (assoc (ex-data e) :message (ex-message e)))))))

(defn history-query-async
  ([sources query-map] (history-query-async sources query-map nil))
  ([sources query-map opts]
   (async/go
     (let [{:keys [block history prettyPrint]} query-map
           db     (<? sources)                              ;; only support 1 source currently
           [block-start block-end] (if block (<? (resolve-block-range db query-map)))
           result (let [meta?   (:meta opts)
                        ;; From-t is the higher number, meaning it is the older time
                        ;; To-t is the lower number, meaning it is the newer time
                        ;; time-range is inclusive
                        from-t  (if (and block-start (not= 1 block-start))
                                  (dec (:t (<? (time-travel/as-of-block db (dec block-start))))) -1)
                        to-t    (if block-end
                                  (:t (<? (time-travel/as-of-block db block-end))) (:t db))
                        subject (cond (util/subj-ident? history)
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

                        [subject idx] (cond
                                        (not (nil? s))
                                        [subject :spot]

                                        (and (nil? s) (not (nil? p)) (nil? o))
                                        [[p s o t] :psot]

                                        (and (nil? s) (not (nil? p)) (not (nil? o)))
                                        [[p o s t] :post]
                                        :else
                                        (throw (ex-info (str "History query not properly formatted. Must include at least an subject or predicate to query. Provided: " history)
                                                        {:status 400
                                                         :error  :db/invalid-query})))
                        flakes  (<? (query-range/time-range db idx = subject {:from-t from-t
                                                                              :to-t   to-t}))
                        resp    (<? (format-history-resp db flakes))
                        resp'   (if prettyPrint
                                  (<? (format-block-resp-pretty db resp))
                                  resp)]

                    (if meta? {:result resp'
                               :fuel   (count flakes)
                               :status 200}
                              resp'))]
       result))))

(defn multi-query-async
  "Returns an asynchronous channel that eventually contains the result or an error.

  The db function, required to evaluate prefixes/multiple sources, is passed
  as a parameter.  This allows for the node.js version to override the
  default [fluree.db.api-js.db] with its own function.
  "
  ([sources multi-query-map] (multi-query-async sources multi-query-map nil db))
  ([sources multi-query-map opts] (multi-query-async sources multi-query-map opts db))
  ([sources multi-query-map opts db-fn]
   (let [db               sources                           ;; only support 1 source for now
         block            (when-let [block (:block multi-query-map)]
                            (<? (time-travel/block-to-int-format (<? db) block)))
         meta?            (:meta opts)
         _                (when (and block (map? block))
                            (throw (ex-info (str "Block is a reserved keyword. Please choose another name for your query. " block)
                                            {:status 500
                                             :error  :db/unexpected-error})))
         _                (if (and block (coll? block))
                            (throw (ex-info (str "Query block must be a string or integer. Block: " block)
                                            {:status 500
                                             :error  :db/unexpected-error})))
         ;;   If block specified within individual queries, that is ignored. Only top-level block is accepted.
         multi-query-map' (dissoc multi-query-map :block)
         vals'            (map #(dissoc % :block) (vals multi-query-map'))
         keys'            (keys multi-query-map')]
     (async/go
       (let [responses  (<? (async/go
                              (loop [key          (first keys')
                                     query        (-> (first vals') (assoc :block block))
                                     rest-keys    (rest keys')
                                     rest-queries (rest vals')
                                     acc          {}]
                                (let [opts* (merge opts (:opts query))
                                      res   (<! (query-async db (assoc query :opts opts*) db-fn))
                                      acc'  (assoc acc key res)]
                                  (if (first rest-keys)
                                    (recur (first rest-keys)
                                           (first rest-queries)
                                           (rest rest-keys)
                                           (rest rest-queries)
                                           acc')
                                    acc')))))
             responses* (reduce-kv (fn [acc index response]
                                     (let [resp (or (:result response) response)]
                                       (if meta?
                                         (if (= 200 (:status response))
                                           (assoc-in acc [:result index] resp)
                                           (assoc-in acc [:errors index] (:error response)))
                                         (assoc acc index resp)))) {} responses)]
         (if meta?
           (let [statuses  (map :status (vals responses))
                 fuel      (apply + (map #(or (:fuel %) 0) (vals responses)))
                 status    (cond
                             (every? #(= (first statuses) %) statuses)
                             (first statuses)

                             (some #(= 200 %) statuses)
                             207

                             (some #(< 499 %) statuses)
                             500

                             :else
                             400)
                 block-res (filter #(= 200 (:status %)) (vals responses))
                 block     (:block (first block-res))
                 result*   (if block
                             (assoc responses* :status status :fuel fuel :block block)
                             (assoc responses* :status status :fuel fuel))]
             result*)
           responses))))))

;; ======================================
;;
;; Signed Queries
;;
;; ======================================
(defn- qry->command
  "Helper function to fill out the parts of the query that are incomplete,
  producing a signed command.

  Optional opts is a map with the following keys. If not provided,
  defaults will be attempted.
  - auth        - The auth id for the auth record being used. The private key must
                  correspond to this auth record, or an authority of this auth record.
  - expire      - When this transaction should expire if not yet attempted.
                  Defaults to 5 minutes.
  - nonce       - Any long/64-bit integer value that will make this transaction unique.
                  By default epoch milliseconds is used.
  - deps        - Not yet implemented, list of dependent transactions.

  If successful, will return a map with four keys:
    - cmd  - a map with the command/transaction data as a JSON string
    - sig  - the signature of the above stringified map
    - id   - the ID for this unique request - in case you want to look it up later, sha3 of 'cmd'
    - db   - the ledger for this transaction"
  ([ledger qry-map private-key] (qry->command ledger qry-map private-key nil))
  ([ledger qry-map private-key opts]
   (when-not private-key
     (throw (ex-info "Private key not provided and no default present on connection"
                     {:status 400 :error :db/invalid-signed-query})))
   (let [db-name     (if (sequential? ledger)
                       (str (first ledger) "/$" (second ledger))
                       ledger)
         {:keys [auth expire nonce action]} opts
         key-auth-id (crypto/account-id-from-private private-key)
         [auth authority] (cond
                            (and auth (not= auth key-auth-id))
                            [auth key-auth-id]

                            auth
                            [auth nil]

                            :else
                            [key-auth-id nil])
         action      (or action :query)
         timestamp   (util/current-time-millis)
         nonce       (or nonce timestamp)
         expire      (or expire (+ timestamp 30000))        ;; 5 min default
         cmd         (try (-> {:type      :signed-qry
                               :action    action
                               :db        db-name
                               :qry       qry-map
                               :nonce     nonce
                               :auth      auth
                               :authority authority
                               :expire    expire}
                              (util/without-nils)
                              (json/stringify-preserve-namespace))
                          (catch :default e
                            (throw (ex-info (str "Signed query contains data that cannot be serialized into JSON.")
                                            {:status 400 :error :db/invalid-signed-query}))))
         sig         (crypto/sign-message cmd private-key)
         id          (crypto/sha3-256 cmd)]
     {:cmd cmd
      :sig sig
      :id  id
      :db  ledger})))

(defn signed-query-async
  "Execute a signed query against a ledger.

  Returns an asynchronous channel that eventually contains the results."
  ([conn ledger query-map] (signed-query-async conn ledger query-map nil))
  ([conn ledger query-map opts]
   (async/go
     (try
       (let [private-key (:private-key opts)
             opts*       (if (nil? (:action opts))
                           (assoc-in opts [:action] :query)
                           opts)
             command     (qry->command ledger query-map private-key opts*)
             result      (<? (ops/command-async conn command))]
         result)
       (catch :default e
         (log/error e)
         (assoc (ex-data e) :message (ex-message e)))))))