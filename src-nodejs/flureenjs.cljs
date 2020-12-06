(ns flureenjs
  (:require-macros [cljs.tools.reader.reader-types]
                   [flureenjs :refer [analyzer-state]])

  (:require [clojure.string :as str]
            [cljs.core.async :refer [go <!] :as async]
            [alphabase.core :as alphabase]
            [fluree.crypto :as crypto]
            [fluree.db.api.ledger :as ledger]
            [fluree.db.auth :as db-auth]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.dbfunctions.fns :as fns] ;:refer [?sid ?pid ?s ?p ?o ?pO ?auth_id ?user_id]
            [fluree.db.graphdb :as graphdb]
            [fluree.db.query.http-signatures :as http-signatures]
            [fluree.db.operations :as ops]
            [fluree.db.permissions :as permissions]
            [fluree.db.query.block :as query-block]
            [fluree.db.query.graphql-parser :as graphql]
            [fluree.db.query.range :as query-range]
            [fluree.db.query.sparql-parser :as sparql-parser]
            [fluree.db.query.sql :as sql]
            [fluree.db.session :as session]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [go-try <? into? channel?]]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [goog.string.format]
            [cljs.nodejs :as node-js]                       ;; NodeJS support

    ; shared clojurescript code
            [fluree.db.connection-js :as conn-handler]
            [fluree.db.api-js :as fdb-js]

    ;; self-hosted clojurescript
            [cljs.js]
            [cljs.analyzer]
            [cljs.env]
            [cljs.compiler]))

(node-js/enable-util-print!)


;;-------------------------------------------------------------------------------------------------
;; ----------------------------
;; -- Implement *eval*       --
;; ----------------------------
;; https://stackoverflow.com/questions/47177243/clojure-dynamic-binding-read-string-and-eval-unable-to-resolve-symbol
(defn init-state [state]
  (assoc-in state [:cljs.analyzer/namespaces 'fluree.db.dbfunctions.fns]
            (analyzer-state 'fluree.db.dbfunctions.fns)))

(def nj-state (cljs.js/empty-state init-state))

(let [st nj-state]
  (set! *eval*
        (fn [form]
          (let [result   (atom {:result nil})
                form-str (if (string? form)
                           form
                           (str form))
                ;_ (log/warn {:form-to-evaluate form-str})
                name     "rtm"
                opts     {:context :expr
                          :eval    cljs.js/js-eval
                          :ns      (cljs.core/find-ns cljs.analyzer/*cljs-ns*)
                          ;:verbose true
                          :target  :nodejs}
                cb       (fn [res]
                           (if (:error res)
                             (swap! result assoc :result (:error res))
                             (swap! result assoc :result (:value res))))]
            (do
              (cljs.js/eval-str st form-str name opts cb)
              ;(log/warn {:result-atom @result})
              (-> @result :result))))))
;;-------------------------------------------------------------------------------------------------


;; define your app data so that it doesn't get over-written on reload
(defonce app-state (atom {:product "Fluree NodeJs Library"
                          :version "v0.17.0"}))

(println (:product @app-state) (:version @app-state))


(declare db-instance)

;; ======================================
;;
;; Support logging at different levels
;;
;; ======================================
(log/set-level! :warning)                                   ;; default to log only warnings or errors
;(def ^:export logging-levels log/levels)

(defn ^:export set-logging
  "Configure logging for Fluree processes.  Supported options:
  1. level [Values: severe, warning, info, config, fine, finer, finest]
  "
  [opts]
  (let [opts' (js->clj opts :keywordize-keys true)
        {:keys [level]} opts']
    (log/set-level! (keyword level))))


;; ======================================
;;
;; Network Operations
;;
;; ======================================

(defn ^:export connect
  "Connect to a ledger server using URL address. If using a ledger group, multiple addresses can be
   supplied, separated by commas."
  [servers-string & [opts]]
  (-> opts
      (js->clj :keywordize-keys true)
      (as-> clj-opts (conn-handler/connect servers-string clj-opts))))


(defn ^:export connect-p
  "Connect to a ledger server using URL address. If using a ledger group, multiple addresses can be
   supplied, separated by commas.

   Returns a promise that eventually contains the connection object."
  [servers-string & [opts]]
  (-> opts
      (js->clj :keywordize-keys true)
      (as-> clj-opts (conn-handler/connect-p servers-string clj-opts))))


(defn ^:export close
  "Closes a connection"
  [conn]
  (conn-handler/close conn))


;; ======================================
;;
;; Login Operations
;;
;; ======================================
(defn ^:export password-generate
  "Attempts to generate a new user auth record account."
  ([conn ledger password user] (password-generate conn ledger password user nil))
  ([conn ledger password user opts]
   (-> opts
       (js->clj :keywordize-keys true)
       (assoc :user user)
       (as-> data (conn-handler/password-generate conn ledger password data)))))


(defn ^:export password-login
  "Returns a JWT token if successful.
  Must supply ledger, password and either user or auth identifier.
  Expire is optional
  - connection - connection object to server
  - ledger     - ledger identifier
  - password   - plain-text password
  - user       - _user/username (TODO: should allow any _user ident in the future)
  - auth       - _auth/id (TODO: should allow any _auth ident in the future)
  - expire     - requested time to expire in milliseconds"
  ([conn ledger password user] (conn-handler/password-login conn ledger password user))
  ([conn ledger password user auth expire] (conn-handler/password-login conn ledger password user auth expire)))


(defn ^:export renew-token
  "Renews a JWT token if successful.
  Returns a promise that eventually contains the token or an exception"
  ([conn jwt] (conn-handler/renew-token conn jwt nil))
  ([conn jwt expire] (conn-handler/renew-token conn jwt expire)))


;; ======================================
;;
;; Listeners
;;
;; ======================================

(defn ^:export listen
  "Listens to all events of a given ledger. Supply a ledger identity,
  any key, and a two-argument function that will be called with each event.

  The key is any arbitrary key, and is only used to close the listener via close-listener,
  otherwise it is opaque to the listener.

  The callback function's first argument is the event header/metadata and the second argument
  is the event data itself."
  [conn ledger key callback]
  (conn-handler/listen conn ledger key callback))


(defn ^:export close-listener
  "Closes a listener."
  [conn ledger key]
  (conn-handler/close-listener conn ledger key))


(defn ^:export listeners
  "Return a list of listeners currently registered for each ledger along with their respective keys."
  [conn]
  (conn-handler/listeners conn))


;; ======================================
;;
;; Ledger/DB Operations
;;
;; ======================================


(defn ^:export db
  "Returns a queryable database from the connection."
  [conn ledger & [opts]]
  (-> opts
      (js->clj :keywordize-keys true)
      (as-> clj-opts (db-instance conn ledger clj-opts))))


(defn ^:private db-instance
  "Returns a queryable database from the connection."
  ([conn ledger] (db-instance conn ledger {}))
  ([conn ledger opts]
   (let [pc (async/promise-chan)]
     (async/go
       (try
         (let [{:keys [auth jwt]} opts
               _             (conn-handler/check-connection conn opts)
               [network ledger-id] (session/resolve-ledger conn ledger)
               auth'        (or auth (if jwt
                                       ["_auth/id" (-> (conn-handler/validate-token conn jwt)
                                                       :sub)]))
               perm-db       (-> (<? (ledger/db conn ledger (assoc opts :auth auth')))
                                 (assoc :conn conn :network network :dbid ledger-id))]
           (async/put! pc perm-db))
         (catch :default e
           (log/error e)
           (async/put! pc e)
           (async/close! pc))))
     pc)))


(defn ^:export db-p
  "Returns a queryable database from the connection."
  [conn ledger & [opts]]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> opts
              (js->clj :keywordize-keys true)
              (as-> clj-opts (db-instance conn ledger clj-opts))
              resolve)
          (catch :default e
            (log/error e)
            (reject e)))))))

(defn ^:export db-schema
  "Returns db's schema map."
  [db]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (<? db)
              :schema
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export delete-ledger
  "Completely deletes a ledger.
  Returns a promise that will have a response with a corresponding status of success.

  A 200 status indicates the deletion has been successfully initiated.
  The full deletion happens in the background on the respective ledger.

  Query servers get notified when this process initiates, and ledger will be marked as
  being in a deletion state during the deletion process.

  Attempts to use a ledger in a deletion state will throw an exception."
  ([conn ledger] (delete-ledger conn ledger nil))
  ([conn ledger opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [opts      (when-not (nil? opts) (js->clj opts :keywordize-keys true))
                 _         (conn-handler/check-connection conn opts)
                 {:keys [nonce expire timeout private-key] :or {timeout 60000}} opts
                 timestamp (util/current-time-millis)
                 nonce     (or nonce timestamp)
                 expire    (or expire (+ timestamp 30000))  ;; 5 min default
                 cmd-data  {:type   :delete-db
                            :db     ledger
                            :nonce  nonce
                            :expire expire}
                 cmd       (when private-key
                             (-> cmd-data
                                 (util/without-nils)
                                 (json/stringify)))
                 sig       (when private-key
                             (crypto/sign-message cmd private-key))
                 result    (if private-key
                             (<? (ops/command-async conn {:cmd cmd :sig sig}))
                             (<? (ops/unsigned-command-async conn cmd-data)))
                 result*   {:status 200
                            :result result}]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export ledger-stats
  "Returns promise with ledger's  with ledger's stats, including db size and # of flakes.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (<? (ops/ledger-stats-async conn ledger))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export new-ledger
  "Attempts to create new ledger.

   A successful result will kick off a process on the ledger server(s) to bootstrap.

   Once successful, will return with a command-id.

   Ledger creation is handled asynchronously and may not be immediately available.

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
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [opts      (when-not (nil? opts) (js->clj opts :keywordize-keys true))
                 _         (conn-handler/check-connection conn opts)
                 {:keys [alias auth doc fork forkBlock expire nonce private-key timeout
                         snapshot snapshotBlock copy copyBlock]
                  :or   {timeout 60000}} opts
                 [network ledger-id] (graphdb/validate-ledger-ident ledger)
                 ledger-id (if (str/starts-with? ledger-id "$")
                             (subs ledger-id 1)
                             ledger-id)
                 _         (graphdb/validate-ledger-name ledger-id "ledger")
                 _         (graphdb/validate-ledger-name network "network")
                 [network-alias ledger-alias]
                 (when alias (graphdb/validate-ledger-ident ledger))
                 _         (when alias (graphdb/validate-ledger-name ledger-alias "alias"))
                 alias*    (when alias (str network-alias "/" ledger-alias)) ;
                 timestamp (util/current-time-millis)
                 nonce     (or nonce timestamp)
                 expire    (or expire (+ timestamp 30000))  ;; 5 min default
                 cmd-data  {:type          :new-db
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
                            :expire        expire}
                 cmd       (when private-key
                             (-> cmd-data
                                 (util/without-nils)
                                 (json/stringify)))
                 sig       (when private-key
                             (crypto/sign-message cmd private-key))
                 result    (if private-key
                             (<? (ops/command-async conn {:cmd cmd :sig sig}))
                             (<? (ops/unsigned-command-async conn cmd-data)))
                 result*   {:status 200
                            :result result}]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject (clj->js (assoc (ex-data e) :message (ex-message e)))))))))))

;; ======================================
;;
;; Transactions
;;
;; ======================================
(defn ^:export monitor-tx
  "Monitors a database for a specific transaction id included in a block.

  Returns a promise that will eventually contain a response or an exception
  if the timeout period has expired.

  Response may contain an exception, if the tx resulted in an exception."
  [conn ledger txid timeout-ms]
  (assert (int? timeout-ms) "monitor requires timeout to be provided in milliseconds as an integer.")
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (let [_      (conn-handler/check-connection conn)
                result (<! (fdb-js/monitor-tx conn ledger txid timeout-ms))]
            (resolve (clj->js result)))
          (catch :default e
            (log/error e)
            (reject (clj->js e))))))))


(defn ^:export transact
  "Submits a transaction for a ledger and a transaction. Returns a promise
   that will eventually have the result of the tx, the txid (if :txid-only option used), or
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
  ([conn ledger txn] (transact conn ledger txn nil))
  ([conn ledger txn opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [opts      (when-not (nil? opts) (js->clj opts :keywordize-keys true))
                 _         (conn-handler/check-connection conn opts)
                 txn*      (js->clj txn :keywordize-keys true)
                 tx-result (<! (fdb-js/transact-async conn ledger txn* opts))]
             (resolve (clj->js tx-result)))
           (catch :default e
             (log/error e)
             (reject e))))))))


;; ======================================
;;
;; Queries
;;
;; ======================================
(defn ^:export block-range-with-txn
  "Returns a Promise that will eventually contain transaction information for blocks from
   start block (inclusive) to end if provided (exclusive). Each block is a separate map,
   containing keys :block :tx"
  ([conn ledger block-map] (block-range-with-txn conn ledger block-map nil))
  ([conn ledger block-map opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [clj-opts  (-> opts
                              (js->clj :keywordize-keys true))
                 block-map (js->clj block-map :keywordize-keys true)
                 {:keys [start end]} block-map
                 db-chan   (async/<! (db-instance conn ledger clj-opts))
                 db-blocks (<? (query-block/block-range db-chan start end clj-opts))
                 result    (query-range/block-with-tx-data db-blocks)]
             (resolve (clj->js result)))
           (catch :default e
             (log/error e)
             (reject e))))))))

(defn ^:export block-query
  ([conn ledger query-map] (block-query conn ledger query-map nil))
  ([conn ledger query-map opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [query-map*  (js->clj query-map :keywordize-keys true)
                 clj-opts    (-> opts
                                 (js->clj :keywordize-keys true))
                 _           (conn-handler/check-connection conn clj-opts)
                 auth-id     (or (:auth clj-opts) (:auth-id clj-opts))
                 jwt         (:jwt clj-opts)
                 private-key (:private-key clj-opts)
                 db          (when (nil? private-key)
                               (<? (db-instance conn ledger {:auth   (when auth-id ["_auth/id" auth-id])
                                                             :jwt    jwt})))
                 result*     (if (nil? private-key)
                               (<? (fdb-js/block-query-async db query-map* clj-opts))
                               (<? (fdb-js/signed-query-async conn ledger query-map* (assoc-in clj-opts [:action] :block))))]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export graphql
  "Execute a graphql query against the specified database."
  ([conn db-name param] (graphql conn db-name param {}))
  ([conn db-name param opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [param*       (-> param
                                  (json/parse)
                                  (js->clj :keywordize-keys true))
                 clj-opts     (-> opts
                                  (js->clj :keywordize-keys true))
                 auth-id      (:auth-id clj-opts)
                 {gql-query :query vars :variables} param*
                 db-ch        (db-instance conn db-name {:auth (when auth-id ["_auth/id" auth-id])})
                 db           (<? db-ch)
                 parsed-query (<? (graphql/parse-graphql-to-flureeql db gql-query vars clj-opts))
                 result       (if (util/exception? parsed-query)
                                parsed-query
                                (cond
                                  ;; __schema and __type queries are fully resolved in the graphql ns, can return from there
                                  (#{:__schema :__type} (:type parsed-query))
                                  (if (:meta clj-opts)
                                    (dissoc parsed-query :type)
                                    (:result parsed-query))

                                  (= :history (:type parsed-query))
                                  (<? (fdb-js/history-query-async db (dissoc parsed-query :type) clj-opts))

                                  (= :block (:type parsed-query))
                                  (<? (fdb-js/block-query-async db (dissoc parsed-query :type) clj-opts))

                                  (:tx parsed-query)
                                  (<? (fdb-js/transact-async conn db-name (:tx parsed-query) clj-opts))

                                  :else
                                  (<? (fdb-js/multi-query-async db-ch parsed-query clj-opts))))]
             (resolve (clj->js result)))
           (catch :default e
             (log/error e)
             (reject (clj->js e)))))))))


(defn ^:export history-query
  ([sources query-map] (history-query sources query-map nil))
  ([sources query-map opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [query-map* (js->clj query-map :keywordize-keys true)
                 clj-opts   (-> opts
                                (js->clj :keywordize-keys true))
                 result     (<? (fdb-js/history-query-async sources query-map* clj-opts))]
             (resolve (clj->js result)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export multi-query
  ([sources multi-query-map] (multi-query sources multi-query-map nil))
  ([sources multi-query-map opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [query-map* (js->clj multi-query-map :keywordize-keys true)
                 clj-opts   (-> opts
                                (js->clj :keywordize-keys true))
                 result*    (<? (fdb-js/multi-query-async sources query-map* clj-opts db-instance))]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export q
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.

  Returns promise containing results."
  [sources query-map]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (let [query-map (js->clj query-map :keywordize-keys true)
                result    (<! (fdb-js/query-async sources query-map db-instance))]
            (resolve (clj->js result)))
          (catch :default e
            (log/error e)
            (reject (clj->js e))))))))



(defn ^:export sparql
  "Exceute a sparql query against a specified database"
  ([db sparql-str] (sparql db sparql-str nil))
  ([db sparql-str opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [sparql-str   (json/parse sparql-str)
                 query-parsed (sparql-parser/sparql-to-ad-hoc sparql-str)
                 opts*        (merge (:opts query-parsed)
                                     (when opts (js->clj opts :keywordize-keys true)))
                 result       (<? (fdb-js/query-async db (assoc query-parsed :opts opts*) db-instance))]
             (resolve (clj->js result)))
           (catch :default e
             (log/error e)
             (reject (clj->js e)))))))))



(defn ^:export sql
  "Exceute a sql query against a specified database"
  ([db sql-str] (sql db sql-str {}))
  ([db sql-str opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [clj-opts (js->clj opts :keywordize-keys true)]
             (-> sql-str
                 json/parse
                 sql/parse
                 (update :opts merge clj-opts)
                 (as-> q (fdb-js/query-async db q db-instance))
                 <?
                 clj->js
                 resolve))
           (catch :default e
             (log/error e)
             (reject (clj->js e)))))))))


(defn ^:export http-signature
  "Takes an http request and creates an http signature using a private key"
  [req-method url request private-key auth]
  (http-signatures/sign-request req-method url request private-key auth))
