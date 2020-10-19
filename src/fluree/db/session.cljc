(ns fluree.db.session
  (:require [fluree.db.graphdb :as graphdb]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.string :as str]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.log :as log]
            [fluree.db.operations :as ops]
            [fluree.db.flake :as flake]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.schema :as schema])
  #?(:clj
     (:import (fluree.db.flake Flake))))

(declare db current-db session)

(defrecord DbSession [conn network dbid db-name update-chan transact-chan state schema-cache blank-db close id])


;;; ----------------------------------------
;;;
;;; Session cache operations
;;;
;;; ----------------------------------------

(defn- cache-factory
  []
  {})

(def ^:private session-cache (atom (cache-factory)))


(comment

  (-> @session-cache)

  (reset! session-cache {})

  (-> @session-cache
      (first)
      (val)
      :state
      deref
      :db/db
      (async/poll!)))

(defn- cache!
  "Only replaces cache if an existing conn is not already present.
  Returns the cached connection."
  [session]
  (let [{:keys [network dbid]} session
        cache-key [network dbid]]
    (swap! session-cache (fn [c]
                           (if (get c cache-key)
                             c
                             (assoc c cache-key session))))
    (get @session-cache cache-key)))

(defn- from-cache
  "Retrieves session from cache."
  [network dbid]
  (get @session-cache [network dbid]))

(defn remove-cache!
  "Removes a specific session from cache."
  [network dbid]
  (swap! session-cache dissoc [network dbid]))

(defn reset-cache!
  "Clears entire session cache. Should not be used under normal circumstances as sessions will not be properly closed."
  []
  (reset! session-cache (cache-factory)))

(defn- full-load-existing-db
  [session]
  (let [pc (async/promise-chan)]
    (async/go
      (try*
        (let [blank-db        (:blank-db session)
              {:keys [conn network dbid]} session
              db-info         (<? (ops/ledger-info-async conn [network dbid]))
              _               (when (not= :ready (keyword (:status db-info)))
                                (throw (ex-info (if (empty? db-info)
                                                  (str "Ledger " network "/" dbid
                                                       " is not found on this ledger group.")
                                                  (str "Ledger " network "/" dbid
                                                       " is not currently available. Status is: "
                                                       (:status db-info) "."))
                                                {:status 400
                                                 :error  :db/unavailable})))
              last-indexed-db (<? (storage/reify-db conn network dbid blank-db (:index db-info)))
              latest-block    (:block db-info)
              db              (when last-indexed-db
                                (loop [db         last-indexed-db
                                       next-block (-> last-indexed-db :block inc)]
                                  (if (> next-block latest-block)
                                    db
                                    (let [block-data (<? (storage/read-block conn network dbid next-block))]
                                      (if block-data
                                        (let [{:keys [flakes block t]} block-data
                                              db* (<? (dbproto/-with db block flakes))]
                                          (recur db* (inc next-block)))
                                        (throw (ex-info (str "Error reading block " next-block " for db: " network "/" dbid ".")
                                                        {:status 500 :error :db/unexpected-error})))))))
              db*             (assoc db :schema (<? (schema/schema-map db)))
              db**            (assoc db* :settings (<? (schema/setting-map db*)))]
          (async/put! pc db**))
        (catch* e
                (async/put! pc e))))
    pc))

(defn cas-db!
  "Performs a compare and set! to update db, but only does so if
  existing db promise-chan is the same as old-db-ch.

  Returns true if successful, false if it did not replace."
  [session old-db-ch new-db-ch]
  (let [new-state (swap! (:state session)
                         (fn [state]
                           (if (= old-db-ch (:db/db state))
                             (assoc state :db/db new-db-ch)
                             state)))]
    (= new-db-ch (:db/db new-state))))


(defn clear-db!
  "Clears db from cache, forcing a new full load next time db is requested."
  [session]
  (swap! (:state session) assoc :db/db nil))


(defn reload-db!
  "Clears any current db that is cached and forces a db reload."
  [session]
  (swap! (:state session) assoc :db/db (full-load-existing-db session)))


(defn indexing?
  "Returns block currently being indexed (truthy), or nil (falsey) if not currently indexing."
  [session]
  (:db/indexing @(:state session)))


(defn indexed
  "Returns block of last indexed block, but only for indexing done by this server."
  [session]
  (:db/indexed @(:state session)))


(defn acquire-indexing-lock!
  "Attempts to acquire indexing lock, and if successful returns true, else false."
  [session block]
  (swap! (:state session)
         (fn [s]
           (cond-> s
                   (nil? (:db/indexing s)) (assoc :db/indexing block))))
  ;; if we got the lock, indexing value will be same as block (true)
  (= block (indexing? session)))

(defn release-indexing-lock!
  "Releases indexing lock, and updates the last indexed value on the connection with provided block number."
  [session block]
  (swap! (:state session) assoc :db/indexing nil :db/indexed block))


(def alias->id-cache (atom #?(:clj  (cache/fifo-cache-factory {:threshold 100})
                              :cljs (cache/lru-cache-factory {:threshold 100}))))

(defn ledger-alias->id
  "Returns ledger id from alias."
  [conn network alias]
  (or (get-in @alias->id-cache [network alias])
      (let [
            ;; TODO - temporarily turned off alias
            dbid alias]
        (swap! alias->id-cache assoc-in [network alias] dbid)
        dbid)))


(defn resolve-ledger
  "Resolves a ledger identity in the form of 'network/alias' and returns a
  two-tuple of [network ledger-id].

  An alias lookup is always performed first, and if an alias doesn't exist it is assumed
  the provided name is a ledger-id.

  If you are providing a ledger-id, and wish to skip an alias lookup, a prefix of '$'
  can be used for the name portion of the ledger.

  i.e.
  - testnet/testledger - Look for ledger with an alias testledger on network testnet.
  - testnet/$testledger - look for a ledger with id testledger on network testnet (skip alias lookup).
  - [testnet testledger] - already in form of [network ledger-id]"
  [conn ledger]
  (if (sequential? ledger)
    ledger
    (let [ledger      (keyword ledger)
          network     (namespace ledger)
          maybe-alias (name ledger)

          _           (when-not (and network maybe-alias) (throw (ex-info (str "Invalid ledger identity: " (pr-str ledger))
                                                                          {:status 400 :error :db/invalid-db})))]
      (if (str/starts-with? maybe-alias "$")
        [network (subs maybe-alias 1)]
        [network (ledger-alias->id conn network maybe-alias) maybe-alias]))))


;; note all process-ledger-update operations must return a go-channel
(defmulti process-ledger-update (fn [_ event-type _] event-type))


(defmethod process-ledger-update :block
  [session event-type {:keys [block t flakes] :as data}]
  (go-try
    (let [current-db-ch (current-db session)
          current-db    (<? current-db-ch)
          current-block (:block current-db)]
      (cond
        ;; no-op
        ;; TODO - we can avoid logging here if we are the transactor
        (<= block current-block)
        (log/info (str (:network session) "/$" (:dbid session) ": Received block " block ", but DB is already more current. No-op."))

        ;; next block is correct, update cached db
        (= block (+ 1 current-block))
        (do
          (log/trace (str (:network session) "/$" (:dbid session) ": Received block " block ", DB at that block, update cached db with flakes."))
          (let [flakes* (map #(if (instance? Flake %) % (flake/parts->Flake %)) flakes)
                new-db  (dbproto/-with current-db block flakes*)]
            (cas-db! session current-db-ch new-db)))

        ;; missing blocks, reload entire db
        :else
        (do
          (log/info (str "Missing block(s): " (:network session) "/$" (:dbid session) ". Received block " block
                         ", but latest local block is: " current-block ". Forcing a db reload."))
          (reload-db! session))))))


(defmethod process-ledger-update :new-index
  [session header block]
  (async/go
    ;; reindex, reload at next request
    (clear-db! session)
    (log/debug (str "Database " (:network session) "/$" (:dbid session) " re-indexed as of block: " block "."))
    true))


(defn closed?
  [session]
  (:closed? @(:state session)))


(defn close
  "Properly shuts down a session.
  Returns true if shut down, false if it was already shut down.

  Calling with a session will shut down session, calling with
  two arity network + dbid will see if a session is in cache and
  then perform the shutdown on the cached session, else will return
  false."
  ([session]
   (if (closed? session)
     false
     (let [{:keys [conn update-chan transact-chan state network dbid id]} session
           closed? (closed? session)]
       (if closed?
         (do
           (remove-cache! network dbid)
           false)
         (do
           (swap! state assoc :closed? true)
           ;; remove updates callback from connection
           ((:remove-listener conn) network dbid id)
           (async/close! update-chan)
           (when transact-chan
             (async/close! transact-chan))
           (remove-cache! network dbid)
           (when (fn? (:close session))
             ((:close session)))
           true)))))
  ([network dbid]
   (if-let [session (from-cache network dbid)]
     (close session)
     false)))


(defn- process-ledger-updates
  "Creates loop that takes new blocks / index commands and processes them in order
  ensuring the consistency of the database."
  [conn network ledger-id update-chan]
  (async/go-loop []
    (let [msg     (<? update-chan)
          session (from-cache network ledger-id)]
      (cond
        (nil? msg)                                          ;; channel closed, likely connection closed. If it wasn't force close just in case.
        (log/info (str "Channel closed for session updates for: " network "/" ledger-id "."))

        (nil? session)                                      ;; unlikely to happen... if channel was closed previous condition would trigger
        (log/warn (str "Ledger update received for session that is no longer open: " network "/" ledger-id ". Message: " (pr-str (first msg))))

        :else
        (do
          (try*
            (let [[event-type event-data] msg]
              (log/trace (str "[process-ledger-updates[" network "/$" ledger-id "]: ") (util/trunc (pr-str msg) 200))
              (<? (process-ledger-update session event-type event-data)))
            (catch* e
                    (log/error e "Exception processing ledger updates for message: " msg)))
          (recur))))))

(defn- session-factory
  "Creates a connection without first checking if db exists. Only useful if reloading
  and replacing an existing DB."
  [{:keys [conn network dbid db-name db state close transactor? id]}]
  (let [schema-cache  (atom {})
        update-chan   (async/chan)
        transact-chan (when transactor? (async/chan))       ;; transactors only
        state         (atom (merge
                              state
                              {:req/sync      {}            ;; holds map of block -> [update-chans ...] to pass DB to once block is fully updated
                               :req/count     0             ;; count of db requests on this connection
                               :req/last      nil           ;; epoch millis of last db request on this connection
                               :db/pending-tx {}            ;; map of pending transaction ids to a callback that we will monitor for
                               :db/db         (when db
                                                (assoc db :schema-cache schema-cache)) ;; current cached DB - make sure we use the latest (new) schema cache in it
                               :db/indexing   nil           ;; a flag holding the block (a truthy value) we are currently in process of indexing.
                               :closed?       false}))
        session       (map->DbSession {:conn          conn
                                       :network       network
                                       :dbid          dbid
                                       :db-name       db-name
                                       :update-chan   update-chan
                                       :transact-chan transact-chan
                                       :state         state
                                       :schema-cache  schema-cache
                                       :blank-db      nil
                                       :close         close
                                       :id            id})
        current-db-fn (fn [] (current-db session))          ;; allows any 'db' to update itself to the latest db
        blank-db      (graphdb/blank-db conn network dbid schema-cache current-db-fn)]
    (assoc session :blank-db blank-db)))


(defn block-response->tx-response
  "Blocks can have multiple transactions. If we are monitoring a single transaction
  we take a full block response and return a map with just that transaction's details.
  If the entire block details are desired, the block can be used to retrieve them.

  If throw? is true, throws an exception if non-2xx response."
  [block-result txid]
  (let [{:keys [block hash instant txns flakes]} block-result
        tx-result (or (get txns (keyword txid)) (get txns txid))
        _         (when-not tx-result
                    (throw (ex-info (str "Unexpected error, unable to get tx results for txid " txid
                                         " out of block " block ".")
                                    {:status 500 :error :db/unexpected-error :block (pr-str block-result)})))
        {:keys [t status]} tx-result
        t-filter  (if (instance? Flake (first flakes))
                    #(= t (.-t ^Flake %))
                    #(= t (nth % 3)))
        response  (-> tx-result
                      (assoc :block block
                             :hash hash
                             :instant instant
                             :flakes (filter t-filter flakes)))]
    (if (< (:status response) 300)
      response
      (let [error-msg (some #(when (= const/$_tx:error (second %)) (nth % 2)) (:flakes response))
            ;; error message strings look like: "400 db/invalid-tx The transaction item _id ...."
            [_ status error message] (when error-msg (re-find #"^([0-9]+) ([^ ]+) (.+)" error-msg))]
        (ex-info (or message "Unknown error.")
                 {:status (:status response)
                  :error  (keyword error)
                  :meta   response})))))



(defn- create-and-cache-session
  "Creates new session and caches it.

  Will tolerate race conditions, and if this call successfully created the session
  it will attach an extra key, :new? true, to the session.

  If another process created the session first, will return the other process' session."
  [opts]
  (let [_        (log/trace "Create and cache session. Opt keys: " (keys opts))
        id       (util/random-uuid)
        session  (session-factory (assoc opts :id id))
        session* (cache! session)
        new?     (= id (:id session*))]
    (if new?
      (assoc session* :new? true)
      session*)))


;; TO-DO check for expired jwt when specified
(defn session
  "Returns connection to the given ledger, and ensures it is cached.

  If 'state' is provided, it will get merged into the connection's state.
  Use namespaced keys, so as to not to create a conflict with system state keys.

  Options supported:
  - connect? - attempts to create a streaming connection to db - will fail if db does not exist
             - ensure is 'false' if trying to create a new db that does not yet exist.
  - state    - initial state map to use
  - auth     - the auth
  - jwt      - jwt from password login
  "
  ([conn ledger] (session conn ledger {}))
  ([conn ledger {:keys [state connect? auth jwt]}]
   (let [[network ledger-id ledger-alias] (resolve-ledger conn ledger)
         connect?    (if (false? connect?) false true)
         transactor? (:transactor? conn)
         opts        (util/without-nils {:auth auth :jwt jwt})]
     (or (from-cache network ledger-id)
         (let [session (create-and-cache-session {:network     network
                                                  :dbid        ledger-id
                                                  :db-name     nil
                                                  :auth        auth
                                                  :jwt         jwt
                                                  :conn        conn
                                                  :state       (or state {})
                                                  :transactor? transactor?})
               new?    (true? (:new? session))]
           (when new?

             (when connect?
               ;; send a subscription request to this database. This is idempotent in the
               ;; unlikely case of multiple sessions simultaneously being created (of which only one will 'win').
               (ops/subscribe session opts)

               ;; register a callback fn for this session to listen for updates and push onto update chan
               ((:add-listener conn) network ledger-id (:id session)
                (fn [event-type event-data]
                  (async/put! (:update-chan session) [event-type event-data])
                  ;; check if we are waiting for any responses for any transactions in here
                  (when (= :block event-type)
                    (when-let [tx-callbacks (not-empty (get @(:state session) :db/pending-tx))]
                      (let [tids (-> event-data :txns keys)]
                        (doseq [tid tids]
                          (when-let [keyed-callbacks (get tx-callbacks (util/keyword->str tid))]
                            ;; remove callbacks from state
                            (swap! (:state session) update :db/pending-tx dissoc (util/keyword->str tid))
                            (let [tx-response (block-response->tx-response event-data tid)]
                              (doseq [[k f] keyed-callbacks]
                                (try*
                                  (f tx-response)
                                  (catch* e
                                          (log/error e (str "Error processing transaction callback for tid: " tid ".")))))))))))))

               ;; launch a go-loop to monitor the update-chan and process updates
               (process-ledger-updates conn network ledger-id (:update-chan session)))

             ;; launch channel for incoming updates
             ;; Currently, (as of 7/12) the only use for transact-chan is to close the session after db creation
             (when transactor?
               (let [transact-handler (:transact-handler conn)]
                 (async/go-loop []
                   (let [req (async/<! (:transact-chan session))]
                     (if (nil? req)
                       (log/info (str "Transactor session closing for db: " network "/$" ledger-id "[" ledger-alias "]"))
                       ;; do some initial validation, then send to handler for synchronous processing
                       (do (transact-handler conn req)
                           (recur))))))))

           session)))))


(defn current-db
  "Gets the latest db from the central DB atom if available, or loads it from scratch.
  DB is returned as a core async promise channel."
  [session]
  (swap! (:state session) #(assoc % :req/last (util/current-time-millis)
                                    :req/count (inc (:req/count %))))
  (let [db (:db/db @(:state session))]
    (if (nil? db)
      (do
        (swap! (:schema-cache session) empty)               ;; always clear schema cache on new load
        (swap! (:state session) #(assoc % :db/db (full-load-existing-db session)))
        (:db/db @(:state session)))
      db)))


(defn blank-db
  "Creates a session and returns a blank db."
  [conn ledger]
  (let [session (session conn ledger {:connect? false})]
    (:blank-db session)))


(defn db
  "Returns core async channel containing current db"
  [conn ledger opts]
  (let [session (session conn ledger opts)]
    (current-db session)))


(defn close-all-sessions
  "Useful for a shutdown process. Closes all sessions for a given connection-id.
  If no connection given, closes all sessions."
  ([] (close-all-sessions nil))
  ([conn-id]
   (let [sessions (cond->> (vals @session-cache)
                           conn-id (filter #(= conn-id (get-in % [:conn :id]))))]
     (doseq [session sessions]
       (close session)))))


(defn monitor-tx
  "Adds a callback function to call when we see a completed transaction in a block
  for the given tid.

  Key is any arbitrary key provided that allows the callback to be unregistered later
  with monitor-tx-remove. It must be unique."
  [session tid key f]
  (swap! (:state session) update-in [:db/pending-tx tid key]
         (fn [x]
           (when x (throw (ex-info "Key provided to monitor-tx must be unique."
                                   {:status 400 :error :db/invalid-request})))
           f)))


(defn monitor-tx-remove
  "Removes callback from supplied transaction id and key.

  Will return true if callback successfully removed, else false if
  callback didn't exist."
  [session tid key]
  (if (get-in @(:state session) [:db/pending-tx tid key])
    (do
      (swap! (:state session) update :db/pending-tx
             (fn [pending-txs]
               ;; if 'key' is only pending tx callback, removes txid entirely from pending-tx state
               (let [updated (update pending-txs tid dissoc key)]
                 (if (empty? (get updated tid))
                   (dissoc updated tid)
                   updated))))
      true)
    false))