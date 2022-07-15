(ns fluree.db.session
  (:require [fluree.db.graphdb :as graphdb]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async :refer [<! >! chan go go-loop]]
               :cljs [cljs.core.async :as async :refer [<! chan] :refer-macros [go go-loop]])
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [clojure.string :as str]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.log :as log]
            [fluree.db.operations :as ops]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.schema :as schema]
            [fluree.db.conn-events :as conn-events])
  #?(:clj (:import (fluree.db.flake Flake))))

#?(:clj (set! *warn-on-reflection* true))

(declare db session)

(defrecord DbSession [conn network ledger-id db-name update-chan transact-chan state
                      schema-cache blank-db close id])


;;; ----------------------------------------
;;;
;;; Session cache operations
;;;
;;; ----------------------------------------

(defn- cache-factory
  []
  {})

(def ^:private session-cache (atom (cache-factory)))

(defn- cache!
  "Only replaces cache if an existing conn is not already present.
  Returns the cached connection."
  [session]
  (let [{:keys [network ledger-id]} session
        cache-key [network ledger-id]]
    (swap! session-cache (fn [c]
                           (if (get c cache-key)
                             c
                             (assoc c cache-key session))))
    (get @session-cache cache-key)))

(defn- from-cache
  "Retrieves session from cache."
  [network ledger-id]
  (get @session-cache [network ledger-id]))

(defn remove-cache!
  "Removes a specific session from cache."
  [network ledger-id]
  (swap! session-cache dissoc [network ledger-id]))

(defn reset-cache!
  "Clears entire session cache. Should not be used under normal circumstances as sessions will not be properly closed."
  []
  (reset! session-cache (cache-factory)))

(defn ready?
  [db-info]
  (= :ready
     (-> db-info :status keyword)))

(defn load-ledger-info
  [conn network ledger-id]
  (go-try
   (let [ledger-info (<? (ops/ledger-info-async conn [network ledger-id]))]
     (if (empty? ledger-info)
       (throw (ex-info (str "Ledger " network "/" ledger-id
                            " is not found on this ledger group.")
                       {:status 400
                        :error  :db/unavailable}))
       (if-not (ready? ledger-info)
         (throw (ex-info (str "Ledger " network "/" ledger-id
                              " is not currently available. Status is: "
                              (:status ledger-info) ".")
                         {:status 400
                          :error  :db/unavailable}))
         ledger-info)))))

(defn load-current-db
  [conn {:keys [network ledger-id] :as blank-db}]
  (go-try
   (let [{:keys [index], latest-block :block, :as ledger-info}
         (<? (load-ledger-info conn network ledger-id))]
     (when-let [indexed-db (<? (storage/reify-db conn network ledger-id blank-db index))]
       (loop [db         indexed-db
              next-block (-> indexed-db :block inc)]
         (if (> next-block latest-block)
           (let [schema-ch   (schema/schema-map db)
                 settings-ch (schema/setting-map db)]
             (swap! (:schema-cache db) empty)
             (assoc db :schema (<? schema-ch), :settings (<? settings-ch)))
           (if-let [{:keys [flakes block t]}
                    (<? (storage/read-block conn network ledger-id next-block))]
             (recur (<? (dbproto/-with db block flakes))
                    (inc next-block))
             (throw (ex-info (str "Error reading block " next-block " for ledger: "
                                  network "/" ledger-id ".")
                             {:status 500, :error :db/unexpected-error})))))))))


(defn cas-db!
  "Perform a compare and set operation to update the db stored in the session
  argument's state atom. Update the cache to `new-db-ch`, but only if the
  previously stored db channel is the same as the `old-db-ch`. Returns a boolean
  indicating whether the cache was updated."
  [{:keys [state]} old-db-ch new-db-ch]
  (-> state
      (swap! (fn [{:db/keys [current] :as s}]
               (if (= current old-db-ch)
                 (assoc s :db/current new-db-ch)
                 s)))
      :db/current
      (= new-db-ch)))


(defn clear-db!
  "Clears db channel from session state, forcing a new full load next time db
  channel is requested."
  [{:keys [state]}]
  (swap! state assoc :db/current nil))


(defn reload-db!
  "Clears any cached database channels and forces an immediate reload. Returns a
  channel that will contain the newly loaded database"
  [{:keys [conn blank-db state]}]
  (let [db-ch (async/promise-chan)]
    (swap! state assoc :db/current db-ch)
    (go
      (try*
        (let [latest-db (<? (load-current-db conn blank-db))]
          (>! db-ch latest-db))
        (catch* e
                (swap! state assoc :db/current nil)
                (log/error e "Error reloading db")
                (async/put! db-ch e))))
    db-ch))


(defn current-db
  "Gets the channel containing the current database from the session's state. If
  no database channel is cached then the current database is loaded form storage
  and a new channel containing it is cached. Returns the cached channel that
  will contain the current database"
  ([{:keys [blank-db] :as session}]
   (current-db session blank-db))
  ([{:keys [conn state] :as session} blank-db]
   (swap! state (fn [s]
                  (-> s
                      (assoc :req/last (util/current-time-millis))
                      (update :req/count inc))))
   (or (:db/current @state)
       (let [cur-ch   (async/promise-chan)
             state-ch (-> state
                          (swap! (fn [s]
                                   (if-not (:db/current s)
                                     (assoc s :db/current cur-ch)
                                     s)))
                          :db/current)]
         (if (= cur-ch state-ch)
           (do (go
                 (try*
                  (let [latest-db (<? (load-current-db conn blank-db))]
                    (>! cur-ch latest-db))
                  (catch* e
                          (swap! state assoc :db/current nil)
                          (log/error e "Error loading current db")
                          (async/put! cur-ch e))))
               cur-ch)
           state-ch)))))

(defn indexing-promise-ch
  "Returns block currently being indexed (truthy), or nil (falsey) if not currently indexing."
  [session]
  (:db/indexing @(:state session)))


(defn indexed
  "Returns block of last indexed block, but only for indexing done by this server."
  [session]
  (:db/indexed @(:state session)))


(defn acquire-indexing-lock!
  "Attempts to acquire indexing lock. Returns two-tuple of [lock? promise-chan]
  where lock? indicates if the lock was successful, and promise-chan is whatever
  promise-chan is registered for indexing."
  [session pc]
  (let [swap-res (swap! (:state session)
                        (fn [s]
                          (if (nil? (:db/indexing s))
                            (assoc s :db/indexing pc)
                            s)))
        res-pc   (:db/indexing swap-res)
        lock?    (= pc res-pc)]
    ;; return two-tuple of if lock was acquired and whatever promise channel is registered.
    [lock? res-pc]))


(defn release-indexing-lock!
  "Releases indexing lock, and updates the last indexed value on the connection with provided block number."
  [session block]
  (swap! (:state session)
         (fn [s]
           (assoc s :db/indexing nil :db/indexed block))))


(def alias->id-cache (atom #?(:clj  (cache/fifo-cache-factory {:threshold 100})
                              :cljs (cache/lru-cache-factory {:threshold 100}))))

(defn ledger-alias->id
  "Returns ledger id from alias."
  [network alias]
  (or (get-in @alias->id-cache [network alias])
      (let [
            ;; TODO - temporarily turned off alias
            ledger-id alias]
        (swap! alias->id-cache assoc-in [network alias] ledger-id)
        ledger-id)))


(defn resolve-ledger
  "Resolves a ledger identity in the form of 'network/alias' and returns a
  two-tuple of [network ledger-id].

  i.e.
  - testnet/testledger - Look for ledger named testledger on network testnet.
  - [testnet testledger] - already in form of [network ledger-id]

  The two-arity version of this exists for backwards compatibility. It doesn't do anything with the
  conn arg so there is also a single-arity version that just takes the ledger name."
  ([ledger] (resolve-ledger nil ledger))
  ([_conn ledger]
   (if (sequential? ledger)
     ledger
     (let [ledger      (keyword ledger)
           network     (namespace ledger)
           ledger-id   (name ledger)

           _           (when-not (and network ledger-id)
                         (throw (ex-info (str "Invalid ledger identity: " (pr-str ledger))
                                         {:status 400 :error :db/invalid-db})))]
         [network ledger-id]))))


;; note all process-ledger-update operations must return a go-channel
(defmulti process-ledger-update (fn [_ event-type _] event-type))

(defmethod process-ledger-update :local-ledger-update
  [_ _ _]
  ;; no-op, local event to trigger any connection listeners (i.e. syncTo or other user (fdb/listen ...) fns)
  ;; see :block update/event type below where this event gets originated from
  (go
    ::no-op))

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
       (log/info (str (:network session) "/" (:ledger-id session)
                      ": Received block: " block
                      ", but DB is already more current at block: " current-block
                      ". No-op."))

       ;; next block is correct, update cached db
       (= block (+ 1 current-block))
       (do
         (log/trace (str (:network session) "/$" (:ledger-id session)
                         ": Received block " block
                         ", DB at that block, update cached db with flakes."))
         (let [new-db-ch (->> flakes
                              (map (fn [f]
                                     (if (instance? Flake f)
                                       f
                                       (flake/parts->Flake f))))
                              (dbproto/-with current-db block))]
           ;; update-local-db, returns true if successful
           (when (cas-db! session current-db-ch new-db-ch)
             ;; place a local notification of updated db on *connection* sub-chan, which is what
             ;; receives all events from ledger server - this allows any (fdb/listen...) listeners to listen
             ;; for a :local-ledger-update event. :block events from ledger server will trigger listeners
             ;; but if they rely on the block having updated the local ledger first they will instead want
             ;; to filter for :local-ledger-update event instead of :block events (i.e. syncTo requires this)
             (conn-events/process-event (:conn session) :local-ledger-update [(:network session) (:ledger-id session)] data))))

       ;; missing blocks, reload entire db
       :else
       (do
         (log/info (str "Missing block(s): " (:network session) "/" (:ledger-id session) ". Received block " block
                        ", but latest local block is: " current-block ". Forcing a db reload."))
         (reload-db! session))))))


(defmethod process-ledger-update :new-index
  [session header block]
  (go
    ;; reindex, reload at next request
    (clear-db! session)
    (log/debug (str "Ledger " (:network session) "/" (:ledger-id session) " re-indexed as of block: " block "."))
    true))


(defn closed?
  [session]
  (:closed? @(:state session)))


(defn close
  "Properly shuts down a session.
  Returns true if shut down, false if it was already shut down.

  Calling with a session will shut down session, calling with
  two arity network + ledger-id will see if a session is in cache and
  then perform the shutdown on the cached session, else will return
  false."
  ([{:keys [conn update-chan transact-chan state network ledger-id id] :as session}]
   (if (closed? session)
     (do
       (remove-cache! network ledger-id)
       false)
     (do
       (swap! state assoc :closed? true)
       ((:remove-listener conn) network ledger-id id)
       (async/close! update-chan)
       (when transact-chan
         (async/close! transact-chan))
       (remove-cache! network ledger-id)
       (when (fn? (:close session))
         ((:close session)))
       true)))
  ([network ledger-id]
   (if-let [session (from-cache network ledger-id)]
     (close session)
     false)))


(defn- process-ledger-updates
  "Creates loop that takes new blocks / index commands and processes them in order
  ensuring the consistency of the database."
  [conn network ledger-id update-chan]
  (go-loop []
    (let [msg     (<? update-chan)
          session (from-cache network ledger-id)]
      (cond
        (nil? msg) ;; channel closed, likely connection closed. If it wasn't force close just in case.
        (log/info "Channel closed for session updates for:" (str network "/" ledger-id))

        (nil? session) ;; unlikely to happen... if channel was closed previous condition would trigger
        (log/warn "Ledger update received for session that is no longer open:" (str network "/" ledger-id)
                  "Message: " (first msg))

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
  "Creates a connection without first checking if one already exists. Only useful
  if reloading and replacing an existing session."
  [{:keys [conn network ledger-id db-name db state close transactor? id]}]
  (let [schema-cache  (atom {})
        cur-db        (when db
                        (assoc db :schema-cache schema-cache))
        state         (atom (merge state
                                   {:req/sync      {}            ;; holds map of block -> [update-chans ...] to pass DB to once block is fully updated
                                    :req/count     0             ;; count of db requests on this connection
                                    :req/last      nil           ;; epoch millis of last db request on this connection
                                    :db/current    cur-db        ;; current cached DB - make sure we use the latest (new) schema cache in it
                                    :db/pending-tx {}            ;; map of pending transaction ids to a callback that we will monitor for
                                    :db/indexing   nil           ;; a flag holding the block (a truthy value) we are currently in process of indexing.
                                    :closed?       false}))
        session       (map->DbSession {:conn          conn
                                       :network       network
                                       :ledger-id     ledger-id
                                       :db-name       db-name
                                       :update-chan   (chan)
                                       :transact-chan (when transactor?
                                                        (chan))
                                       :state         state
                                       :schema-cache  schema-cache
                                       :blank-db      nil
                                       :close         close
                                       :id            id})
        current-db-fn (partial current-db session) ;; allows any 'db' to update itself to the latest db
        blank-db      (graphdb/blank-db conn network ledger-id schema-cache current-db-fn)]
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
                    #(= t (flake/t %))
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
  (log/trace "Create and cache session. Opt keys: " (keys opts))
  (let [id       (keyword "session" (-> (random-uuid) str (subs 0 7)))
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
                                                  :ledger-id   ledger-id
                                                  :db-name     nil
                                                  :auth        auth
                                                  :jwt         jwt
                                                  :conn        conn
                                                  :state       (or state {})
                                                  :transactor? transactor?})
               new?    (true? (:new? session))]
           (when new?

             (when connect?
               ;; send a subscription request to this database.
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
                 (go-loop []
                   (let [req (async/<! (:transact-chan session))]
                     (if (nil? req)
                       (log/info "Transactor session closing for db:" (str network "/" ledger-id "[" ledger-alias "]"))
                       ;; do some initial validation, then send to handler for synchronous processing
                       (do (transact-handler conn req)
                           (recur))))))))

           session)))))

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
