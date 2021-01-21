(ns flureenjs
  (:require-macros [cljs.tools.reader.reader-types]
                   [flureenjs :refer [analyzer-state]])

  (:require [clojure.string :as str]
            [cljs.core.async :refer [go <!] :as async]
            [alphabase.core :as alphabase]
            [fluree.crypto :as crypto]
            [fluree.db.api.query :as query]
            [fluree.db.api.ledger :as ledger]
            [fluree.db.auth :as db-auth]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.dbfunctions.fns :as fns]
            [fluree.db.flake :refer [Flake] :as flake]
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
                          :version "v1.0.0-rc5"}))

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
;; Auth-related
;;
;; ======================================
(defn ^:export account-id
  "Returns account id from either a public key or message and signature."
  ([public-key] (crypto/account-id-from-public public-key))
  ([message signature] (crypto/account-id-from-message message signature)))


(defn ^:export http-signature
  "Takes an http request and creates an http signature using a private key"
  ([req-method url request private-key] (http-signature req-method url request private-key nil))
  ([req-method url request private-key auth]
   (-> request
       js->clj
       (as-> request (http-signatures/sign-request req-method url request private-key auth)))))


(defn ^:export public-key-from-private
  "Returns a public key given a private key."
  [private-key] (crypto/pub-key-from-private private-key))


(defn ^:export public-key
  "Returns a public key from a message and a signature."
  [message signature] (crypto/pub-key-from-message message signature))


;(defn ^:export new-private-key
;  "Generates a new private key, returned in a map along with
;  the public key and account id. Return keys are :public,
;  :private, and :id."
;  []
;  (let [kp      (crypto/generate-key-pair)
;        account (crypto/account-id-from-private (:private kp))]
;    (assoc kp :id account)))


(defn ^:export sign
  "Returns a signature for a message given provided private key."
  [message private-key]
  (crypto/sign-message message private-key))


(defn ^:export set-default-key
  "Sets a new default private key for the entire tx-group, network or db level.
  This will only succeed if signed by the default private key for the tx-group,
  or if setting for a dbid, either the tx-group or network.

  It will overwrite any existing default private key.

  It will respond with true or false.

  Returns promise that eventually contains the results. "
  ([conn private-key] (set-default-key conn nil nil private-key nil))
  ([conn network private-key] (set-default-key conn network nil private-key nil))
  ([conn network dbid private-key] (set-default-key conn network dbid private-key nil))
  ([conn network dbid private-key opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [{:keys [nonce expire signing-key]} (js->clj opts :keywordize-keys true)
                 timestamp (util/current-time-millis)
                 nonce     (or nonce timestamp)
                 expire    (or expire (+ timestamp 30000))          ;; 5 min default
                 cmd-map   {:type        :default-key
                            :network     network
                            :dbid        dbid
                            :private-key private-key
                            :nonce       nonce
                            :expire      expire}
                 cmd       (when signing-key
                             (-> cmd-map
                                 (util/without-nils)
                                 (json/stringify)))
                 sig       (when signing-key
                             (crypto/sign-message cmd signing-key))]
             (-> (if signing-key
                   (ops/command-async conn {:cmd cmd :sig sig})
                   (ops/unsigned-command-async conn cmd-map))
                 <!
                 clj->js
                 resolve))
           (catch :default e
             (log/error e)
             (reject e))))))))




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
  "Attempts to generate a new user auth record account.

  Returns a promise that eventually contains the token or an exception."
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
  - expire     - requested time to expire in milliseconds

  Returns a promise that eventually contains the token or an exception."
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
(defn block-event->map
  "Takes block event data from (listen...) and adds an :added and
  :retracted key containing maps of data organized by subject
  and containing full predicate names."
  [conn ledger block-event]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (let [db     (<? (db-instance conn ledger))
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
            (resolve (assoc block-event :added (to-map add)
                               :retracted (to-map retract))))
          (catch :default e
            (log/error e)
            (reject e)))))))


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


(defn ^:export collection-id
  "Returns promise containing collection id given a collection name.
  If collection doesn't exist, returns nil."
  [db collection]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> collection
              (js->clj :keywordize-keys true)
              (as-> clj-collection (dbproto/-c-prop (<? db) :id clj-collection))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export predicate-id
  "Returns promise containing predicate id given a predicate.
  If predicate doesn't exist, returns nil."
  [db predicate]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> predicate
              (js->clj :keywordize-keys true)
              (as-> clj-predicate (dbproto/-p-prop (<? db) :id clj-predicate))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export predicate-name
  "Returns promise containing predicate name given a predicate.
  If predicate doesn't exist, returns nil."
  [db predicate]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> predicate
              (js->clj :keywordize-keys true)
              (as-> clj-predicate (dbproto/-p-prop (<? db) :name clj-predicate))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export subid
  "Returns promise containing subject id given a subject
  identity or a subject id.
  If subject doesn't exist, returns nil."
  [db ident]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> ident
              (js->clj :keywordize-keys true)
              (as-> clj-ident (<? (dbproto/-subid (<? db) clj-ident false)))
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


(defn ^:export ledger-info
  "Returns promise with ledger's status as a map, including index, indexes, block, and status.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (ops/ledger-info-async conn ledger)
              <?
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export ledger-list
  "Returns promise with a list of ledgers the connected server is currently serving."
  [conn]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (ops/ledgers-async conn)
              <?
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))

(defn ^:export ledger-stats
  "Returns promise with ledger's  with ledger's stats, including db size and # of flakes.
  If ledger doesn't exist, will return an empty map."
  [conn ledger]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (ops/ledger-stats-async conn ledger)
              <?
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


(defn ^:export resolve-ledger
  "Resolves a ledger identity in the form of 'network/ledger-or-alias' and returns a
  tuple of either [network ledger alias] or [network ledger].

  An alias lookup is always performed first, and if an alias doesn't exist it is assumed
  the provided name is a ledger id.

  If you are providing a ledger id, and wish to skip an alias lookup, a prefix of '$'
  can be used for the name portion of the db-ident.

  i.e.
  - testnet/testledger - Look for ledger with an alias or id of testledger on network testnet.
  - testnet/$testledger - look for a ledger with id testledger on network testnet (skip alias lookup)."
  [conn ledger]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (session/resolve-ledger conn ledger)
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export session
  "Returns actual session object for a given ledger."
  [conn ledger]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (session/session conn ledger)
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))




;; ======================================
;;
;; Search/Time-travel
;;
;; ======================================
(defn- ^:private block-Flakes->vector
  [blocks]
  (loop [[block & r] blocks
         acc []]
    (if block
      (let [flakes (map flake/Flake->parts (:flakes block))]
        (recur r (into acc [(assoc block :flakes flakes)])))
      acc)))


(defn ^:export search
  "Returns a promise containing search results of flake parts (flake-parts)."
  [db flake-parts]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> flake-parts
              js->clj
              (as-> fp (dbproto/-search (<? db) fp))
              <?
              (as-> flakes (map flake/Flake->parts flakes))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export forward-time-travel
  "Returns a promise containing a new db based on the provided db,
   including the provided flakes. Flakes can contain one or more 't's,
   but should be sequential and start after the current 't' of the provided
   db. (i.e. if db-t is -14, flakes 't' should be -15, -16, etc.). Remember
   't' is negative and thus should be in descending order.

   A forward-time-travel db can be further forward-time-traveled.

   A forward-time travel DB is held in memory, and is not shared across servers.
   Ensure you have adequate memory to hold the flakes you generate and add. If
   access is provided via an external API, do any desired size restrictions or
   controls within your API endpoint.

   Remember schema operations done via forward-time-travel should be done in a
   't' prior to the flakes that end up requiring the schema change."
  [db flakes]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> flakes
              js->clj
              (as-> flakes' (map flake/parts->Flake flakes'))
              (as-> flakes' (graphdb/forward-time-travel (<? db) nil flakes'))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export is-forward-time-travel-db
  "Returns true if provided db is a forward-time-travel db."
  [db]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (if (channel? db)
                (<? db)
                db)
              graphdb/forward-time-travel-db?
              clj->js
              resolve)
          (catch :default e
            (log/error e)
            (reject e)))))))


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


(defn tx->command
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
     (throw (ex-info "Private key not provided"
                     {:status 400 :error :db/invalid-transaction})))
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [db-name     (if (sequential? ledger)
                               (str (first ledger) "/$" (second ledger))
                               ledger)
                 {:keys [auth expire nonce deps]} opts
                 _           (when deps
                               (assert (sequential? deps) "Command/transaction 'deps', when provided, must be a sequential list/array."))
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
                                  (catch :default e
                                    (do
                                      (log/error e)
                                      (throw (ex-info (str "Transaction contains data that cannot be serialized into JSON.")
                                                      {:status 400 :error :db/invalid-tx})))))
                 sig         (crypto/sign-message cmd private-key)
                 id          (crypto/sha3-256 cmd)]
             (resolve {:cmd cmd  :sig sig  :id id  :db ledger}))
           (catch :default e
             (log/error e)
             (reject (clj->js e)))))))))


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
                 clj-opts    (merge (:opts query-map*)
                                    (when opts (js->clj opts :keywordize-keys true)))
                 _           (conn-handler/check-connection conn clj-opts)
                 auth-id     (or (:auth clj-opts)
                                 (:auth-id clj-opts)
                                 (some->> (:jwt clj-opts)
                                          (conn-handler/validate-token conn)
                                          :auth))
                 result*     (<? (query/block-query-async
                                   conn ledger
                                   (update query-map* :opts merge (merge clj-opts (util/without-nils {:auth auth-id})))))]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export block-range
  "Returns a promise containing blocks from start (inclusive)
   to end if provided (exclusive).

   Each block is a separate map, containing keys :block, :t and :flakes."
  ([db start] (block-range db start nil nil))
  ([db start end] (block-range db start end nil))
  ([db start end opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (-> opts
               (js->clj :keywordize-keys true)
               (as-> clj-opts (query-block/block-range (<? db) start end clj-opts))
               <?
               block-Flakes->vector
               clj->js
               (resolve))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export collection-flakes
  "Returns spot index range for only the requested collection."
  [db collection]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (query-range/collection (<? db) collection)
              <?
              (as-> flakes (map flake/Flake->parts flakes))
              clj->js
              (resolve))
          (catch :default e
            (log/error e)
            (reject e)))))))


(defn ^:export graphql
  "Execute a graphql query against the specified database."
  ([conn ledger param] (graphql conn ledger param {}))
  ([conn ledger param opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [param*       (-> param
                                  (json/parse)
                                  (js->clj :keywordize-keys true))
                 clj-opts     (merge (:opts param*)
                                     (when opts (js->clj opts :keywordize-keys true)))
                 {gql-query :query vars :variables} param*
                 db-ch        (db-instance conn ledger clj-opts)
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
                                  (<? (query/history-query-async db (-> parsed-query
                                                                        (dissoc :type)
                                                                        (assoc  :opts clj-opts))))

                                  (= :block (:type parsed-query))
                                  (<? (query/block-query-async conn ledger (-> parsed-query
                                                                               (dissoc :type)
                                                                               (assoc  :opts clj-opts))))

                                  (:tx parsed-query)
                                  (<? (fdb-js/transact-async conn ledger (:tx parsed-query) clj-opts))

                                  :else
                                  (<? (query/multi-query-async db-ch (-> parsed-query
                                                                         (dissoc :type)
                                                                         (assoc  :opts clj-opts))))))]
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
                 clj-opts   (merge (:opts query-map*)
                              (-> opts
                                (js->clj :keywordize-keys true)))
                 result     (<? (query/history-query-async sources (merge query-map* {:opts clj-opts})))]
             (resolve (clj->js result)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export multi-query
  ([source multi-query-map] (multi-query source multi-query-map nil))
  ([source multi-query-map opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [query-map* (js->clj multi-query-map :keywordize-keys true)
                 clj-opts   (merge (:opts query-map*)
                                   (-> opts
                                       (js->clj :keywordize-keys true)))
                 result*    (<? (query/multi-query-async source (merge query-map* {:opts clj-opts})))]
             (resolve (clj->js result*)))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn ^:export query
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.

  Returns promise that eventually contains the results or
  an exception."
  [source query-map]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (-> (js->clj query-map :keywordize-keys true)
               (as-> qm (query/query source qm))
               <?
               clj->js
               resolve)
          (catch :default e
            (log/error e)
            (reject (clj->js e))))))))


(defn ^:export query-with
  "Execute a query against a database source, with the
  given flakes applied.

  Returns promise that eventually contains the results or
  an exception."
  [db param]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (let [{:keys [query flakes]} (js->clj param :keywordize-keys true)
                flakes' (map flake/parts->Flake flakes)
                db-with (dbproto/-forward-time-travel (<? db) flakes')]
            (-> (query/query db-with query)
                <?
                clj->js
                resolve))
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
                 result       (<? (query/query-async db (assoc query-parsed :opts opts*)))]
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
                 (as-> q (query/query-async db q))
                 <?
                 clj->js
                 resolve))
           (catch :default e
             (log/error e)
             (reject (clj->js e)))))))))
