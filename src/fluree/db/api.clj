(ns fluree.db.api
  (:gen-class)
  (:refer-clojure :exclude [range])
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.api.auth :as auth-api]
            [fluree.db.api.ledger :as ledger-api]
            [fluree.db.api.query :as query-api]
            [fluree.db.connection :as connection]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.graphdb :as graphdb]
            [fluree.db.operations :as ops]
            [fluree.db.query.block :as query-block]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.graphql-parser :as graphql]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.sparql-parser :as sparql]
            [fluree.db.query.sql :as sql]
            [fluree.db.session :as session]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? channel? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log])
  (:import java.util.UUID))

;; ======================================
;;
;; DB Operations
;;
;; ======================================

(declare db transact query)

;(defn pred-from-results
;  [db transaction-results pred-name]
;  "Gets first value of an predicate from transaction results."
;  (let [pred-id    (query db {:selectOne "?s" :where [["?s" "_predicate/name" pred-name]]})
;        pred-value (->> transaction-results
;                        :flakes (some #(when (= pred-id (:p %))
;                                         (:o %))))]
;    pred-value))




(defn sign
  "DEPRECATED: use fluree.db.api.auth/sign instead."
  [message private-key]
  (log/warn "sign DEPRECATED - use fluree.db.api.auth/sign instead")
  (auth-api/sign message private-key))


(defn public-key-from-private
  "DEPRECATED: use fluree.db.api.auth/public-key-from-private instead."
  [private-key]
  (log/warn "public-key-from-private DEPRECATED - use fluree.db.api.auth/public-key-from-private instead")
  (auth-api/public-key-from-private private-key))


(defn public-key
  "DEPRECATED: use fluree.db.api.auth/public-key instead."
  [message signature]
  (log/warn "public-key DEPRECATED - use fluree.db.api.auth/public-key instead")
  (auth-api/public-key message signature))


(defn new-private-key
  "DEPRECATED: use fluree.db.api.auth/new-private-key instead."
  []
  (log/warn "new-private-key DEPRECATED - use fluree.db.api.auth/new-private-key instead")
  (auth-api/new-private-key))


(defn set-default-key-async
  "DEPRECATED: use fluree.db.api.auth/set-default-key-async instead."
  ([conn private-key] (set-default-key-async conn nil nil private-key nil))
  ([conn network private-key] (set-default-key-async conn network nil private-key nil))
  ([conn network dbid private-key] (set-default-key-async conn network dbid private-key nil))
  ([conn network dbid private-key opts]
   (log/warn "set-default-key-async DEPRECATED - use fluree.db.api.auth/set-default-key-async instead")
   (auth-api/set-default-key-async conn network dbid private-key opts)))

(defn set-default-key
  "DEPRECATED: use fluree.db.api.auth/set-default-key instead."
  ([conn private-key] (set-default-key-async conn nil nil private-key nil))
  ([conn network private-key] (set-default-key-async conn network nil private-key nil))
  ([conn network dbid private-key] (set-default-key-async conn network dbid private-key nil))
  ([conn network dbid private-key opts]
   (log/warn "set-default-key DEPRECATED - use fluree.db.api.auth/set-default-key instead")
   (auth-api/set-default-key conn network dbid private-key opts)))

(defn account-id
  "INTERNAL USE ONLY

  Returns account id from either a public key or message and signature."
  ([public-key] (crypto/account-id-from-public public-key))
  ([message signature] (crypto/account-id-from-message message signature)))


(defn tx->command
  "Helper function to fill out the parts of the transaction that are incomplete,
  producing a signed command.

  Optional opts is a map with the following keys. If not provided,
  sensible defaults will be used.
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
         _           (when deps (assert (sequential? deps) "Command/transaction 'deps', when provided, must be a sequential list/array."))
         key-auth-id (crypto/account-id-from-private private-key)
         [auth authority] (cond
                            (and auth (not= auth key-auth-id))
                            [auth key-auth-id]

                            auth
                            [auth nil]

                            :else
                            [key-auth-id nil])
         timestamp   (System/currentTimeMillis)
         nonce       (or nonce timestamp)
         expire      (or expire (+ timestamp 30000)) ;; 5 min default
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
                          (catch Exception _
                            (throw (ex-info (str "Transaction contains data that cannot be serialized into JSON.")
                                            {:status 400 :error :db/invalid-tx}))))
         sig         (crypto/sign-message cmd private-key)
         id          (crypto/sha3-256 cmd)]
     {:cmd cmd
      :sig sig
      :id  id
      :db  ledger})))


(defn monitor-tx-async
  "Monitors a database for a specific transaction id included in a block.

  Returns a core async channel that will eventually contain a response,
  or will close after the timeout has expired.

  Response may contain an exception, if the tx resulted in an exception."
  [conn ledger tid timeout-ms]
  (assert (int? timeout-ms) "monitor-tx requires timeout to be provided in milliseconds as an integer.")
  (let [session      (session/session conn ledger)
        key          (UUID/randomUUID)
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

(defn submit-command-async
  "INTERNAL USE ONLY

  Submits a fully signed command to the connected ledger group.
  Commands have two required keys:
    - cmd  - a map with the transactional data as a JSON string
    - sig - the signature of the stringified tx map

  Command ids are the sha3 of the cmd, and can be used to reference command status
  or look them up (i.e. a transaction command id is the txid.)

  The stringified cmd contains a payload that is a map, a transaction example follows:

  {:type   tx             - command type is required on all commands
   :db     testnet/mydb   - db name, use testnet/$mydb to peg to a dbid
   :tx     [{...}, {...}] - transactional data
   :auth   ABC12345676    - only required if using an authority's signature, else inferred from signature
   :fuel   10000          - max fuel to spend, only required if enforcing fuel limits. tx will fail if auth doesn't have this much fuel avail. Will fail if all fuel is consumed. Unused fuel will not be debited.
   :nonce  1234           - nonce ensures uniqueness, making sure two identical transactions have different txids
   :expire 1547049123614  - don't even attempt this transaction after this moment in time
   :deps   []             - optional one or more txids that must execute successfully before this tx executes
                            if any of the txs in deps fail, this tx will fail
  }

  Attempting to cancel a transaction
  {:type   tx-cancel
   :txid   DSFGFHSDDF  - txid you wish to cancel
  }


  A new ledger command looks like:
  Note new ledgers are issued as a command, and auth/signature should have proper authority on ledger servers.
  {:type      new-db         - command type is required on all commands
   :db        testnet/mydb   - db name - as network/dbid
   :alias     testnet/mydb   - optional alias, will default to 'db' if not specified.
   :fork      testnet/forkdb - optional name of db to fork, if forking. Use testnet/$forkdb to peg to a dbid
   :forkBlock 42             - if forking a db, optionally provides a block to fork at, else will default to current block
   :auth      ABC12345676    - only required if using an authority's signature
   :fuel      10000          - max fuel to spend, only required if enforcing fuel limits. tx will fail if auth doesn't have this much fuel avail. Will fail if all fuel is consumed. Unused fuel will not be debited.
   :nonce     1234           - nonce ensures uniqueness, making sure two identical transactions have different txids
   :expire    1547049123614  - don't even attempt this transaction after this moment in time
  }

  Returns an async channel that will receive the result.
  "
  [conn command]
  ;; returns once persists, not upon transaction success
  (ops/command-async conn command))


(defn new-ledger-async
  "Attempts to create new ledger with the given ledger name (ex. `fluree/example` or `:fluree/example`).

  A successful result will kick off a process on the ledger server(s) to bootstrap.

  Returns a channel which will receive a command-id after the ledger has been successfully created.

  Ledger creation is handled asynchronously and may not be immediately available.

  Options include:
  - :alias       - Alias, if different than db-ident.
  - :root        - Root account id to bootstrap with (string). Defaults to connection default account id.
  - :doc         - Optional doc string about this db.
  - :fork        - If forking an existing db, ref to db (actual identity, not db-ident). Must exist in network db.
  - :forkBlock   - If fork is provided, optionally provide the block to fork at. Defaults to latest known.
  - :persistResp - Respond immediately once persisted with the dbid, don't wait for transaction to be finished
  "
  ([conn ledger] (new-ledger-async conn ledger nil))
  ([conn ledger opts]
   (try (let [invalid-ledger-name? (fn [ledger-id type]
                                     (when-not (re-matches #"^[a-z0-9-]{1,100}" ledger-id)
                                       (throw (ex-info (str "Invalid " type " id: " ledger-id ". Must match a-z0-9- and be no more than 100 characters long.")
                                                       {:status 400 :error :db/invalid-db}))))
              {:keys [alias auth doc fork forkBlock expire nonce private-key timeout
                      snapshot snapshotBlock copy copyBlock]
               :or   {timeout 60000}} opts
              [network ledger-id] (graphdb/validate-ledger-ident ledger)
              ledger-id            (if (str/starts-with? ledger-id "$")
                                     (subs ledger-id 1)
                                     ledger-id)
              _                    (invalid-ledger-name? ledger-id "ledger")
              _                    (invalid-ledger-name? network "network")
              [network-alias ledger-alias] (when alias
                                             (graphdb/validate-ledger-ident ledger))
              _                    (when alias (invalid-ledger-name? ledger-alias))
              alias*               (when alias (str network-alias "/" ledger-alias))
              timestamp            (System/currentTimeMillis)
              nonce                (or nonce timestamp)
              expire               (or expire (+ timestamp 30000)) ;; 5 min default
              cmd-data             {:type          :new-db
                                    :db            (str network "/" ledger-id)
                                    :alias         alias*
                                    :auth          auth
                                    :doc           doc
                                    :fork          fork
                                    :forkBlock     forkBlock
                                    :copy          copy
                                    :copyBlock     copyBlock
                                    :snapshot      snapshot
                                    :snapshotBlock snapshotBlock
                                    :nonce         nonce
                                    :expire        expire}]
          (if private-key
            (let [cmd          (-> cmd-data
                                   (util/without-nils)
                                   (json/stringify))
                  sig          (crypto/sign-message cmd private-key)
                  persisted-id (submit-command-async conn {:cmd cmd
                                                           :sig sig})]
              persisted-id) (ops/unsigned-command-async conn cmd-data)))
        (catch Exception e e))))


(defn new-ledger
  "Attempts to create new ledger with the given ledger name (ex. `fluree/example` or `:fluree/example`).

  Returns a promise of a command-id, if the ledger bootrapping process is successful.

  Options include:
  - :alias       - Alias, if different than db-ident.
  - :root        - Root account id to bootstrap with (string). Defaults to connection default account id.
  - :doc         - Optional doc string about this db.
  - :fork        - If forking an existing db, ref to db (actual identity, not db-ident). Must exist in network db.
  - :forkBlock   - If fork is provided, optionally provide the block to fork at. Defaults to latest known.
  - :persistResp - Respond immediately once persisted with the dbid, don't wait for transaction to be finished
  "
  ([conn ledger] (new-ledger conn ledger nil))
  ([conn ledger opts]
   (let [p (promise)]
     (async/go
       (let [res (new-ledger-async conn ledger opts)]
         (if (channel? res)
           (deliver p (async/<! res))
           (deliver p res)))) p)))

(defn delete-ledger-async
  "Completely deletes a ledger.
  Returns a channel that will receive a boolean indicating success or failure.

  A 200 status indicates the deletion has been successfully initiated.
  The full deletion happens in the background on the respective ledger.

  Query servers get notified when this process initiates, and ledger will be marked as
  being in a deletion state during the deletion process.

  Attempts to use a ledger in a deletion state will throw an exception."
  ([conn ledger] (delete-ledger-async conn ledger))
  ([conn ledger opts]
   (try (let [{:keys [nonce expire timeout private-key] :or {timeout 60000}} opts
              timestamp (System/currentTimeMillis)
              nonce     (or nonce timestamp)
              expire    (or expire (+ timestamp 30000))     ;; 5 min default
              cmd-data  {:type   :delete-db
                         :db     ledger
                         :nonce  nonce
                         :expire expire}]
          (if private-key
            (let [cmd          (-> cmd-data
                                   (util/without-nils)
                                   (json/stringify))
                  sig          (crypto/sign-message cmd private-key)
                  persisted-id (submit-command-async conn {:cmd cmd
                                                           :sig sig})]
              persisted-id)
            (ops/unsigned-command-async conn cmd-data)))
        (catch Exception e e))))

(defn delete-ledger
  "Completely deletes a ledger.
  Returns a future that will have a boolean indicating success or failure.

  A 200 status indicates the deletion has been successfully initiated.
  The full deletion happens in the background on the respective ledger.

  Query servers get notified when this process initiates, and ledger will be marked as
  being in a deletion state during the deletion process.

  Attempts to use a ledger in a deletion state will throw an exception."
  ([conn ledger] (delete-ledger conn ledger nil))
  ([conn ledger opts]
   (let [p (promise)]
     (async/go
       (let [res (delete-ledger-async conn ledger opts)]
         (if (channel? res)
           (deliver p (async/<! res))
           (deliver p res)))) p)))

(defn multi-txns-async
  "Submits multiple transactions to a ledger, one after the other. If a transaction fails
  subsequent transactions will still be attempted. Returns a channel with the
  results. See `transact` for details about opts."
  ([conn ledger txns]
   (multi-txns-async conn ledger txns nil))
  ([conn ledger txns opts]
   (let [{:keys [private-key txid-only timeout auth nonce deps expire]
          :or   {timeout 60000
                 nonce   (System/currentTimeMillis)}} opts]
     (if private-key
       ;; private key, so generate command locally and submit signed command
       (let [commands     (map #(tx->command ledger % private-key opts) txns)
             txids        (mapv :id commands)
             commands*    (map #(assoc % :multiTxs txids) commands)
             final-txid   (last txids)
             persist-resp (loop [[cmd & r] commands*]
                            (when cmd
                              (submit-command-async conn cmd)
                              (recur r)))
             result       (if txid-only
                            persist-resp
                            (monitor-tx-async conn ledger final-txid timeout))]
         result)
       ;; no private key provided, not allowed
       (throw (ex-info "You must provide a private key when submitting multiple transactions simultaneoulsy."
                       {:status 400
                        :error  :db/invalid-command}))))))

(comment
  (def conn (:conn user/system))
  (def ledger "fluree/test")
  (def pk "72914086db2716ea9de5d8b62f78363b411709bdbda9e1d91884ad981fb143a0")

  (def txns [[{:_id "_user" :username "1"}]
             [{:_id "_user" :username "2"}]
             [{:_id "_user" :username "3"}]])

  (async/<!! (multi-txns-async conn ledger txns {:private-key pk})))


(defn transact-async
  "Submits a transaction for a ledger and a transaction. Returns a core async channel
  that will eventually have either the result of the tx, the txid (if :txid-only option used), or
  an exception due to an invalid transaction or if the timeout occurs prior to a response.

  Will locally sign a final transaction command if a private key is provided via :private-key
  in the options, otherwise will submit the transaction to the connected ledger and request signature,
  provided the ledger group has a default private key available for signing.

  Options (opts) is a map with the following possible keys:
  - private-key - The private key to use for signing. If not present, a default
                  private key will attempt to be used from the connection, if available.
  - auth        - The auth id for the auth record being used. The private key must
                  correspond to this auth record, or an authority of this auth record.
  - expire      - When this transaction should expire if not yet attempted.
                  Defaults to 5 minutes.
  - nonce       - Any long/64-bit integer value that will make this transaction unique.
                  By default epoch milliseconds is used.
  - deps        - List of one or more txids that must be successfully processed before
                  this tx is processed. If any fail, this tx will fail. (not yet implemented)
  - txid-only   - Boolean (default of false). If true, will not wait for a response to the tx,
                  but instead return with the txid once it is successfully persisted by the
                  transactors. The txid can be used to look up/monitor the response at a later time.
  - timeout     - will respond with an exception if timeout reached before response available."
  ([conn ledger txn] (transact-async conn ledger txn nil))
  ([conn ledger txn opts]
   (let [{:keys [private-key txid-only timeout auth nonce deps expire]
          :or   {timeout 60000
                 nonce   (System/currentTimeMillis)}} opts]
     (if private-key
       ;; private key, so generate command locally and submit signed command
       (let [command      (tx->command ledger txn private-key opts)
             txid         (:id command)
             persist-resp (submit-command-async conn command)
             result       (if txid-only
                            persist-resp
                            (monitor-tx-async conn ledger txid timeout))]
         result)
       ;; no private key provided, request ledger to sign request
       (let [tx-map (util/without-nils
                      {:db     ledger
                       :tx     txn
                       :auth   auth
                       :nonce  nonce
                       :deps   deps
                       :expire expire})]
         (go-try
           ;; will received txid once transaction is persisted, else an error
           (let [txid (<? (ops/transact-async conn tx-map))]
             (if txid-only
               txid
               ;; tx is persisted, monitor for txid
               (let [tx-result (<? (monitor-tx-async conn ledger txid timeout))]
                 tx-result)))))))))


(defn transact
  "Like transact-async, but returns a promise."
  ([conn ledger txn] (transact conn ledger txn nil))
  ([conn ledger txn opts]
   (let [p (promise)]
     (async/go
       (deliver p (async/<! (transact-async conn ledger txn opts))))
     p)))


(defn collection-id
  "Returns promise containing id of a collection, given a collection name.
  Returns nil if collection doesn't exist."
  [db collection]
  (let [p (promise)]
    (async/go
      (try
        (deliver p (dbproto/-c-prop (<? db) :partition collection))
        (catch Exception e
          (deliver p e))))
    p))


(defn predicate-id
  "Returns promise containing predicate id given a predicate name, or predicate id.
  If predicate doesn't exist, returns nil."
  [db predicate]
  (let [p (promise)]
    (async/go
      (try
        (deliver p (dbproto/-p-prop (<? db) :id predicate))
        (catch Exception e
          (deliver p e))))
    p))

(defn predicate-name
  "Returns promise containing predicate name given predicate id."
  [db predicate-name]
  (let [p (promise)]
    (async/go
      (try
        (deliver p (dbproto/-p-prop (<? db) :name predicate-name))
        (catch Exception e
          (deliver p e))))
    p))


(defn subid-async
  "Like subid, but returns a core async promise channel instead of a promise."
  [db ident]
  (let [pc (async/promise-chan)]
    (async/go
      (try
        (async/put! pc (<? (dbproto/-subid (<? db) ident false)))
        (catch Exception e
          (async/put! pc e))))
    pc))


(defn subid
  "Returns promise containing subject id given a subject identity, or subject id.
  If subject doesn't exist, returns nil."
  [db ident]
  (let [p (promise)]
    (async/go
      (try
        (deliver p (<? (dbproto/-subid (<? db) ident false)))
        (catch Exception e
          (deliver p e))))
    p))


(defn search-async
  "Performs a search for matching flakes, returns a core async promise channel."
  [db flake-parts]
  (let [pc (async/promise-chan)]
    (async/go
      (try
        (async/put! pc (dbproto/-search (<? db) flake-parts))
        (catch Exception e
          (async/put! pc e))))
    pc))


(defn search
  "Returns a promise containing search results of flake parts (fparts)."
  [db flake-parts]
  (let [p (promise)]
    (async/go
      (try
        (deliver p (<? (dbproto/-search (<? db) flake-parts)))
        (catch Exception e
          (deliver p e))))
    p))


(defn forward-time-travel
  "Returns a core async chan with a new db based on the provided db, including the provided flakes.
  Flakes can contain one or more 't's, but should be sequential and start after the current
  't' of the provided db. (i.e. if db-t is -14, flakes 't' should be -15, -16, etc.).
  Remember 't' is negative and thus should be in descending order.

  A forward-time-travel db can be further forward-time-traveled.

  A forward-time travel DB is held in memory, and is not shared across servers. Ensure you
  have adequate memory to hold the flakes you generate and add. If access is provided via
  an external API, do any desired size restrictions or controls within your API endpoint.

  Remember schema operations done via forward-time-travel should be done in a 't' prior to
  the flakes that end up requiring the schema change."
  [db flakes]
  (graphdb/forward-time-travel db nil flakes))


(defn forward-time-travel-db?
  "Returns true if provided db is a forward-time-travel db."
  [db]
  (graphdb/forward-time-travel-db? db))



;; ======================================
;;
;; Querying
;;
;; ======================================


(defn block-range
  "Returns a core async channel of blocks from start block (inclusive) to end if provided (exclusive).
  Each block is a separate map, containing keys :block, :t and :flakes.
  Channel is lazy, continue to take! values as needed."
  ([db start] (block-range db start nil nil))
  ([db start end] (block-range db start end nil))
  ([db start end opts]
   (query-block/block-range db start end opts)))

(defn block-range-with-txn-async
  "Returns a core async channel of blocks from start block (inclusive) to end if provided (exclusive).
   Each block is a separate map, containing keys :block, :t, :flakes and :txn"
  [conn ledger block-map]
  (async/go
    (let [[network ledger-id] (session/resolve-ledger conn ledger)
          {:keys [start end opts]} block-map
          auth-id   (:auth opts)
          db-chan   (->
                      (<? (db conn ledger {:auth (when auth-id ["_auth/id" auth-id])}))
                      (assoc :conn conn :network network :dbid ledger-id))
          db-blocks (<? (query-block/block-range db-chan start end opts))
          result    (query-range/block-with-tx-data db-blocks)]
      result)))

(defn query-async
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns core async channel containing result."
  [sources query-map]
  (query-api/query-async sources query-map))


(defn query
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns promise with result."
  [sources query-map]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (query-api/query-async sources query-map))))
    p))

(defn query-with-async
  "INTERNAL USE ONLY
  "
  [sources param]
  (go-try
    (let [{:keys [query flakes]} param
          flakes' (map flake/parts->Flake flakes)
          db      (<? sources)
          db-with (dbproto/-forward-time-travel db flakes')
          res     (<? (query-async db-with (assoc-in query [:opts :meta] true)))]
      res)))


(defn format-block-resp-pretty
  "INTERNAL USE ONLY"
  [db curr-block cache fuel]
  (go-try (let [[asserted-subjects
                 retracted-subjects] (loop [[flake & r] (:flakes curr-block)
                                            asserted-subjects  {}
                                            retracted-subjects {}]
                 (if-not flake
                   [asserted-subjects retracted-subjects]
                   (let [subject   (.-s flake)
                         asserted? (true? (.-op flake))
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


(defn format-blocks-resp-pretty
  "INTERNAL USE ONLY"
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

(defn min-safe
  "INTERNAL USE ONLY"
  [& args]
  (->> (remove nil? args) (apply min)))

(defn auth-match
  "INTERNAL USE ONLY"
  [auth-set t-map flake]
  (let [[auth id] (get-in t-map [(.-t flake) :auth])]
    (or (auth-set auth)
        (auth-set id))))

(defn format-history-resp
  "INTERNAL USE ONLY"
  [db resp auth show-auth]
  (go-try (let [ts    (-> (map #(.-t %) resp) set)
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
                resp  (-> (loop [[flake & r] resp
                                 acc {}]
                            (cond (and flake auth
                                       (not (auth-match auth t-map flake)))
                                  (recur r acc)

                                  flake
                                  (let [t   (.-t flake)
                                        {:keys [block auth]} (get t-map t)
                                        acc (cond-> acc
                                                    true (assoc-in [block :block] block)
                                                    true (update-in [block :flakes] conj flake)
                                                    true (update-in [block :t] min-safe t)
                                                    show-auth (assoc-in [block :auth] auth))]
                                    (recur r acc))

                                  :else
                                  acc)) vals)] resp)))

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
            [block-end block-start] ; make sure smallest number comes first
            [block-start block-end])
          block-end (if (> block-end db-block)
                      db-block block-end)]
      [block-start block-end])))

(defn block-query-async
  "Given a map with a `:block` with a block number value, return a channel that will receive the raw flakes contained in that block.

  Can also specify `:prettyPrint` `true` in the query map to receive the flakes as a map with predicate names."
  [conn ledger query-map]
  (query-api/block-query-async conn ledger query-map))

(defn block-query
  "Given a map with a `:block` with a block number value, return a promise of the raw flakes contained in that block.

  Can also specify `:prettyPrint` `true` in the query map to receive the flakes as a map with predicate names."
  [conn ledger query-map]
  (let [p (promise)]
    (async/go
      (let [res (block-query-async conn ledger query-map)]
        (if (channel? res)
          (deliver p (async/<! res))
          (deliver p res)))) p))

(defn get-history-pattern
  "INTERNAL USE ONLY"
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

(defn history-query-async
  "Given a map with a `:history` key that has a subject ident or id, return a channel that will receive the history of that subject.

  Can also specify `:prettyPrint` `true` in the query map to receive the history as a map with predicate names instead of raw flakes."
  [sources query-map]
  (query-api/history-query-async sources query-map))

(defn history-query
  "Given a map with a `:history` key that has a subject ident or id, return a promise of the history of that subject.

  Can also specify `:prettyPrint` `true` in the query map to receive the history as a map with predicate names instead of raw flakes."
  [sources query-map]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (query-api/history-query-async sources query-map))))
    p))


(defn multi-query-async
  "Like query, but takes a map of multiple queries - where map keys are any user-provided aliases
  and values are queries as if sent directly to 'query'.

  If not {:meta true}, any responses with errors will not be returned.

  If {:meta true} ->
  The response :result contains all provided queries, with keys being the same user-provided aliases
  and values being the query results only.

  Queries with any non-200 response will not be included in the :result, but will be in a separate
  :errors key containing the error(s) using their respective aliases.

  If all queries have the identical error status, the overall response status will be that status.
  If some queries are 200 status but others have errors, the overall response will be a 207 (WebDAV's multi-status response)
  If all queries have error responses, but mixed, the overall response will be 400 unless there is also a 5xx
  status code, in which case it will be 500.

  Queries leverage multi-threading.

  All queries are for the same block. :block can be included on the main map. Whether or not a block
  is specified in the main map, any of the individual queries specify a block, it will be ignored.

  Returns a channel that will receive the result."
  [sources multi-query-map]
  (query-api/multi-query-async sources multi-query-map))


(defn multi-query
  "Like query, but takes a map of multiple queries - where map keys are any user-provided aliases
  and values are queries as if sent directly to 'query'.

  If not {:meta true}, any responses with errors will not be returned.

  If {:meta true} ->
  The response :result contains all provided queries, with keys being the same user-provided aliases
  and values being the query results only.

  Queries with any non-200 response will not be included in the :result, but will be in a separate
  :errors key containing the error(s) using their respective aliases.

  If all queries have the identical error status, the overall response status will be that status.
  If some queries are 200 status but others have errors, the overall response will be a 207 (WebDAV's multi-status response)
  If all queries have error responses, but mixed, the overall response will be 400 unless there is also a 5xx
  status code, in which case it will be 500.

  Queries leverage multi-threading.

  All queries are for the same block. :block can be included on the main map. Whether or not a block
  is specified in the main map, any of the individual queries specify a block, it will be ignored.

  Returns a promise of the result."
  [sources multi-query-map]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (multi-query-async sources multi-query-map))))
    p))

(defn graphql-async
  "Execute a graphql query against the specified database.
  Query must come in as a map with keys:
  - query - required graphql string query
  - variables - optional substitution variables supplied with query
  - opts - optional map of options (same as flureeQL :opts map), however not all options would be relevant for graphql"
  [conn db-name query-map]
  (go-try
    (let [{gql-query :query vars :variables opts :opts} query-map
          auth-id          (:auth-id opts)
          db-ch            (db conn db-name {:auth (when auth-id ["_auth/id" auth-id])})
          db               (<? db-ch)
          parsed-gql-query (<? (graphql/parse-graphql-to-flureeql db gql-query vars opts))]
      (if (util/exception? parsed-gql-query)
        parsed-gql-query
        (cond
          ;; __schema and __type queries are fully resolved in the graphql ns, can return from there
          (#{:__schema :__type} (:type parsed-gql-query))
          (if (:meta opts)
            (dissoc parsed-gql-query :type)
            (:result parsed-gql-query))

          (= :history (:type parsed-gql-query))
          (<? (history-query-async db-ch (-> parsed-gql-query
                                             (dissoc :type)
                                             (assoc :opts opts))))

          (= :block (:type parsed-gql-query))
          (<? (block-query-async conn db-name (-> parsed-gql-query
                                                  (dissoc :type)
                                                  (assoc :opts opts))))

          (:tx parsed-gql-query)
          (<? (transact-async conn db-name (:tx parsed-gql-query) opts))

          :else
          (<? (multi-query-async db-ch (assoc parsed-gql-query :opts opts))))))))

(defn graphql
  "Execute a graphql query against the specified database.
  Query must come in as a map with keys:
  - query - required graphql string query
  - variables - optional substitution variables supplied with query
  - opts - optional map of options (same as flureeQL :opts map), however not all options would be relevant for graphql"
  [conn db-name query-map]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (graphql-async conn db-name query-map))))
    p))

(defn sql-async
  "Execute an SQL query against a specified database"
  ([db sql-str]
   (sql-async db sql-str {}))
  ([db sql-str opts]
   (-> sql-str
       sql/parse
       (update :opts merge opts)
       (as-> q (query-async db q)))))

(defn sql
  "Execute an SQL query against a specified database. Returns the a promise of the query results."
  ([db sql-str]
   (sql db sql-str {}))
  ([db sql-str opts]
   (let [p (promise)]
    (async/go
      (deliver p (async/<! (sql-async db sql-str opts))))
    p)))

(defn sparql-async
  "Exceute a sparql query against a specified database. Returns a core async channel,
  which will recieve the query result."
  ([db sparql-str]
   (sparql-async db sparql-str nil))
  ([db sparql-str opts]
   (->> (sparql/sparql-to-ad-hoc sparql-str)
        (#(query-async db (assoc % :opts (merge (:opts %) opts)))))))

(defn sparql
  "Execute an sparql query against a specified database. Returns a promise of the query results."
  ([db sparql-str]
   (sparql db sparql-str {}))
  ([db sparql-str opts]
   (let [p (promise)]
    (async/go
      (deliver p (async/<! (sparql-async db sparql-str opts))))
    p)))

#_(defn index
  "INTERNAL USE ONLY

  Returns a raw collection of flakes from the specified index as a lazy sequence.

  Optionally specify a start and/or stop point to restrict the collection to a range
  along with an operator of <, <=, >, >=. If you wish to restrict to a specific
  subject, predicate, etc. the = operator can also be used, which is equivalent to the same
  parts being specified with a >= and <= operators.

  The start and stop point should be specified as a vector of the relevant part(s) of the
  specified index. i.e. if using the :spo index, the parts are [s p o], an :pos index would
  be [p o s]. If only some parts, i.e. [s] are provided, the other parts are assumed to
  be the lowermost or uppermost bounds of the remaining parts depending on if it is the
  start or stop respectively. Keep in mind subjects sort descending.

  Entities can be specified as an _id long integer, any unique identity (pred / obj two-tuple),
  or a collection name.

  Predicates can b
  "
  [db index start stop]

  nil)





(defn collection-flakes
  "INTERNAL USE ONLY"
  [db collection]
  (query-range/collection db collection))



(defn flakes
  "Returns a lazy sequence of raw flakes from the blockchain history from
  start block/transaction (inclusive) to end block/transaction (exclusive).

  A nil start defaults to the genesis block. A nil end includes the last block of the known database.
  A positive integer for either start/end indicates a block number, a negative integer indicates a
  transaction number.

  Results can potentially include the entire database depending on your filtering criteria,
  so be sure to only 'pull' items as you need them.

  The optional map of filter criteria has the following keyed options:

  :subject    - Limit results to only include history of this subject, specified as either an _id or identity.
               Note the results are no longer lazy when using this option.
  :predicate - Limit results to only include history for this predicate. Must be used in conjunction with subject.
               If there is a need to get history of all subjects for a specific predicate, see 'range-history'.
  :limit     - Limit results to this quantity of matching flakes
  :offset    - Begin results after this number of matching flakes (for paging - use in conjunction with limit)
  :chunk     - Results are fetched in chunks. Optionally specify the size of a chunk if optimization is needed."
  ([conn] (flakes conn nil nil {}))
  ([conn start] (flakes conn start nil {}))
  ([conn start end] (flakes conn start end {}))
  ([conn start end {:keys [subject predicate limit offset chunk]}]))






(defn range
  "Returns a lazy sequence of raw flakes for the database and specified index. Results can be
  limited by including one or two match clauses, in addition to options including a limit and offset.

  Match predicates can be one of: =, >, >=, <, <= as either a string or symbol. If two matches are
  provided, one must be a > or >=, and the other a < or <=. An = predicate is only allowed with a single
  match statement, and acts the same providing two match statements: >= match <= match]

  Matches are a vector containing the flake components desired for a match, in the order dictated
  by the specified index. For example, if an spot index is used, the order is [s, p, o]. If an post
  index is used, the order is [p, o, s]. The match vector can be a one, two, or three-tuple depending
  on the specificity desired for the match. For example, for an spot index where the match should include
  all predicates from subject 42, the match clause could be either [42], [42, nil] or [42, nil, nil].
  Unlike the 'flakes' function, nil values in a match are only valid if no non-nil value follows.

   Opts may be provided. opts is a map containing any of the optional keys:

   :limit  - Limit results to this quantity
   :history  - Return the history of this range
   :from-block - Only applicable for history, return history up until a certain block
   :chunk  - Results are fetched in chunks. Optionally specify the size of a chunk if optimization is needed.
   :test  - Running as a test. Makes sure, i.e. index ranges are not cached."
  [& args]
  (apply query-range/index-range args))


;; ======================================
;;
;; Connection, db
;;
;; ======================================

(defn db
  "Returns a queryable database from the connection for the specified ledger."
  ([conn ledger]
   (session/db conn ledger nil))
  ([conn ledger opts]
   (ledger-api/db conn ledger opts)))

(defn get-db-at-block
  "Returns a channel with queryable database value from the given block number."
  [conn ledger block every-n-sec]
  (let [db-chan (async/promise-chan)]
    (go-try (loop [n 1]
              (let [root-db  (<? (session/db conn ledger nil))
                    db-block (:block root-db)
                    done?    (>= db-block block)]
                (if done?
                  (async/put! db-chan root-db)
                  (do (<? (async/timeout (* every-n-sec 1000)))
                      (recur (inc n)))))))
    db-chan))


(defn resolve-ledger
  "INTERNAL USE ONLY

  Resolves a ledger identity in the form of 'network/ledger-or-alias' and returns a
  two-tuple of [network ledger].

  An alias lookup is always performed first, and if an alias doesn't exist it is assumed
  the provided name is a ledger id.

  If you are providing a ledger id, and wish to skip an alias lookup, a prefix of '$'
  can be used for the name portion of the db-ident.

  i.e.
  - testnet/testledger - Look for ledger with an alias or id of testledger on network testnet.
  - testnet/$testledger - look for a ledger with id testledger on network testnet (skip alias lookup)."
  [conn ledger]
  (session/resolve-ledger conn ledger))


(defn connect
  "Connect to a ledger server using URL address. If using a ledger group, multiple addresses can be
   supplied, separated by commas.

  Optional opts is a map with the following keys. If not provided,
  sensible defaults will be used.
    :tx-private-key - Will be used to sign every command passed through this connection,
                      as long as it is not overidden on the transaction.
    :memory         - How much memory to limit the object LRU cache to. "
  [ledger-servers & [opts]]
  (connection/connect ledger-servers opts))

(defn close
  "Closes connection."
  [conn]
  (connection/close! conn))


;(defn session
;  "Create a session to a specific database. The session can be used to get a
;  queryable database, submit a transaction, etc.
;
;  Utilize full database name: network-name/db-name.
;
;  A session object is returned if successful.  If the supplied connection is not
;  participating in that network, or the connection itself is not currently active,
;  an exception will be thrown.
;
;  While there is activity, the current version of the database will stay in-sync
;  allowing for immediate access of the latest data. If idle for a period of time,
;  the session will close, but automatically re-connect when next used."
;  [connection db-name & [opts]]
;  (session/session connection db-name opts)
;  )

(defn listen
  "Listens to all events of a given ledger. Supply a ledger identity,
  any key, and a two-argument function that will be called with each event.

  The key is any arbitrary key, and is only used to close the listener via close-listener,
  otherwise it is opaque to the listener.

  The callback function's first argument is the event header/metadata and the second argument
  is the event data itself."
  [conn ledger key fn]
  (let [[network ledger-id] (resolve-ledger conn ledger)]
    (connection/add-listener conn network ledger-id key fn)))


(defn close-listener
  "Closes a listener. See `listen` for details."
  [conn ledger key]
  (let [[network ledger-id] (session/resolve-ledger conn ledger)]
    (connection/remove-listener conn network ledger-id key)))

(defn block-event->map
  "INTERNAL USE ONLY

  Takes block event data from (listen...) and adds an :added and
  :retracted key containing maps of data organized by subject
  and containing full predicate names."
  [conn ledger block-event]
  (let [db     (async/<!! (db conn ledger))
        {add true retract false} (group-by #(nth % 4) (:flakes block-event))
        to-map (fn [flakes]
                 (let [by-subj (group-by first flakes)]
                   (reduce-kv (fn [acc sid flakes]
                                (conj acc
                                      (reduce (fn [m flake]
                                                (let [p-schema (get-in db [:schema :pred (second flake)])
                                                      v        (nth flake 2)]
                                                  (if (:multi p-schema)
                                                    (update m (:name p-schema) conj v)
                                                    (assoc m (:name p-schema) v))))
                                              {"_id" sid} flakes)))
                              [] by-subj)))]
    (assoc block-event :added (to-map add)
           :retracted (to-map retract))))

(defn session
  "Returns actual session object for a given ledger."
  [conn ledger]
  (session/session conn ledger))

(defn ledger-info-async
  "Returns core async promise channel with ledger's status as a map, including index, indexes, block, and status.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (ops/ledger-stats-async conn ledger))


(defn ledger-info
  "Returns promise with ledger's status as a map, including index, indexes, block, and status.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (ops/ledger-info-async conn ledger))))
    p))

(defn ledger-list-async
  "Returns a list of ledgers the connected server is currently serving.
  Returns core async promise channel with response."
  [conn]
  (ops/ledgers-async conn))


(defn ledger-list
  "Returns promise with a list of ledgers the connected server is currently serving."
  [conn]
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (ops/ledgers-async conn))))
    p))


(defn ledger-stats-async
  "DEPRECATED: use `ledger-info-async` instead.

  Returns core async promise channel with ledger info, including db size and # of flakes.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (log/warn "ledger-stats-async DEPRECATED: use ledger-info-async instead.")
  (ops/ledger-stats-async conn ledger))


(defn ledger-stats
  "DEPRECATED: use `ledger-info` instead.

  Returns promise with ledger info, including db size and # of flakes.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (log/warn "ledger-stats DEPRECATED: use ledger-info instead.")
  (let [p (promise)]
    (async/go
      (deliver p (async/<! (ops/ledger-stats-async conn ledger))))
    p))


(defn wait-for-ledger-ready-async
  "Returns core async channel that will return true when ledger is in ready status
   or if timeout in ms supplied expires, will close the channel (returning nil)."
  [conn ledger timeout]
  (let [timeout-at (+ (System/currentTimeMillis) timeout)]
    (async/go-loop []
      (let [db-status (-> (async/<! (ledger-info-async conn ledger)) :status)]
        (if (= "ready" db-status)
          true
          (when (<= (System/currentTimeMillis) timeout-at)
            (async/<! (async/timeout 100))
            (recur)))))))


(defn wait-for-ledger-ready
  "Will block until ledger is in ready status and return true, or
  if supplied timeout in milliseconds expires, will return nil.
  Defaults timeout to 30000 if two-arity call is used."
  ([conn ledger] (wait-for-ledger-ready conn ledger 30000))
  ([conn ledger timeout]
   (async/<!! (wait-for-ledger-ready-async conn ledger timeout))))


(defn ledger-ready?-async
  "Returns core async channel that will be true or false if ledger is in a 'ready' status."
  [conn ledger]
  (async/go
    (-> (async/<! (ledger-info-async conn ledger))
        :status
        (= "ready"))))


(defn ledger-ready?
  "Returns true or false if ledger is in a 'ready' status."
  [conn ledger]
  (async/<!! (ledger-ready?-async conn ledger)))


(defn latest-block
  "Returns latest block (positive integer) for a local ledger. Will bring the ledger locally if not
  already local."
  [conn ledger]
  (let [p (promise)]
    (async/go
      (let [latest-db (async/<! (db conn ledger))
            block     (:block latest-db)]
        (deliver p block)))
    p))


(defn latest-t
  "Returns latest t (negative integer) for a local ledger. Will bring the ledger locally if not
  already local."
  [conn ledger]
  (let [p (promise)]
    (async/go
      (let [latest-db (async/<! (db conn ledger))
            t         (:t latest-db)]
        (deliver p t)))
    p))


(defn to-t
  "Given a db and any time value (block, ISO-8601 time/duration, or t)
  will return the underlying ledger's t value as of that time value."
  [db block-or-t-or-time]
  (let [p (promise)]
    (async/go
      (->> (ledger-api/to-t db block-or-t-or-time)
           async/<!
           (deliver p)))
    p))
