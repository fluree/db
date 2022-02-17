(ns fluree.db.connection
  (:require [clojure.string :as str]
            #?(:clj [environ.core :as environ])
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.index :as index]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.session :as session]
            #?(:clj [fluree.crypto :as crypto])
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.xhttp :as xhttp]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.query.http-signatures :as http-signatures]
            #?(:clj [fluree.db.serde.avro :refer [avro-serde]])
            [fluree.db.conn-events :as conn-events]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.storage.core :as storage]))

#?(:clj (set! *warn-on-reflection* true))

;; socket connections are keyed by connection-id and contain :socket - ws, :id - socket-id :health - status of health checks.
(def server-connections-atom (atom {}))

(def server-regex #"^(?:((?:https?):)//)([^:/\s#]+)(?::(\d*))?")


(defn- acquire-healthy-server
  "Tries all servers in parallel, the first healthy response will be used for the connection
  (additional server healthy writes will be no-ops after first)."
  [conn-id servers promise-chan]
  ;; kick off server comms in parallel
  (doseq [server servers]
    (let [healthcheck-uri (str server "/fdb/health")
          resp-chan       (xhttp/post-json healthcheck-uri {} {:request-timeout 5000})]
      (async/go
        (let [resp (async/<! resp-chan)]
          (if (util/exception? resp)
            (log/warn "Server contact error: " (ex-message resp) (ex-data resp))
            (async/put! promise-chan server))))))

  ;; trigger a timeout and clear pending channel if no healthy responses
  (async/go
    (let [healthy-server (async/alt! promise-chan ::server-found
                                     (async/timeout 60000) ::timeout)]
      (when (= ::timeout healthy-server)
        ;; remove lock, so next attempt tries a new server
        (swap! server-connections-atom update conn-id dissoc :server)
        ;; respond with error
        (async/put! promise-chan (ex-info (str "Unable to find healthy server before timeout.")
                                          {:status 500 :error :db/connection-error}))))))


(defn get-healthy-server
  "Returns a core async channel that will contain first healthy as it appears.

  Use with a timeout to consume, as no healthy servers may be avail."
  [conn-id servers]
  (let [lock-id      (util/random-uuid)
        new-state    (swap! server-connections-atom update-in [conn-id :server]
                            (fn [x]
                              (if x
                                x
                                {:lock-id lock-id
                                 :chan    (async/promise-chan)})))
        have-lock?   (= lock-id (get-in new-state [conn-id :server :lock-id]))
        promise-chan (get-in new-state [conn-id :server :chan])]
    (when have-lock?
      (acquire-healthy-server conn-id servers promise-chan))
    promise-chan))


(defn establish-socket
  [conn-id sub-chan pub-chan servers]
  (go-try
    (let [lock-id    (util/random-uuid)
          state      (swap! server-connections-atom update-in [conn-id :ws]
                            (fn [x]
                              (if x
                                x
                                {:lock-id lock-id
                                 :socket  (async/promise-chan)})))
          have-lock? (= lock-id (get-in state [conn-id :ws :lock-id]))
          resp-chan  (get-in state [conn-id :ws :socket])]
      (when have-lock?
        (let [healthy-server (async/<! (get-healthy-server conn-id servers))
              ws-url         (-> (str/replace healthy-server "http" "ws")
                                 (str "/fdb/ws"))
              timeout        60000
              close-fn       (fn []
                               (swap! server-connections-atom dissoc conn-id)
                               (session/close-all-sessions conn-id))]
          (if (util/exception? healthy-server)
            (do
              ;; remove web socket promise channel, it could be tried again
              (swap! server-connections-atom update conn-id dissoc :ws)
              ;; return error, so anything waiting on socket can do what it needs
              (async/put! resp-chan healthy-server))
            (xhttp/try-socket ws-url sub-chan pub-chan resp-chan timeout close-fn))))
      resp-chan)))


;; all ledger messages are fire and forget

;; we do need to establish an upstream connection from a ledger to us, so we can propogate
;; blocks, flushes, etc.

(defrecord Connection [id servers state req-chan sub-chan pub-chan group
                       storage-read storage-write storage-exists storage-rename
                       object-cache parallelism serializer default-network
                       transactor? publish transact-handler tx-private-key
                       tx-key-id meta add-listener remove-listener close]

  storage/Store
  (read [_ k]
    (storage-read k))
  (write [_ k data]
    (storage-write k data))
  (exists? [_ k]
    (storage-exists k))
  (rename [_ old-key new-key]
    (storage-rename old-key new-key))

  index/Resolver
  (resolve
    [conn {:keys [id leaf tempid] :as node}]
    (if (= :empty id)
      (storage/resolve-empty-leaf node)
      (object-cache
       [id tempid]
       (fn [_]
         (storage/resolve-index-node conn node
                                     (fn []
                                       (object-cache [id tempid] nil)))))))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [{:keys [storage-type] :as conn} network dbid lang]
                     (when-let [path (-> conn :meta :file-storage-path)]
                       (full-text/disk-index path network dbid lang)))]))

(defn- normalize-servers
  "Split servers in a string into a vector.

  Randomizies order, ensures uniqueness."
  [servers transactor?]
  (let [servers* (if (string? servers)
                   (str/split servers #",")
                   servers)]
    (when (and (empty? servers) (not transactor?))
      (throw (ex-info "At least one server must be supplied for connection."
                      {:status 400 :error :db/invalid-connection})))
    ;; following randomizes order
    (when (not-empty servers*)
      (loop [[server & r] servers*
             https? nil
             result #{}]
        (when-not (string? server)
          (throw (ex-info (str "Invalid server provided for connection, must be a string: " (pr-str server))
                          {:status 400 :error :db/invalid-connection})))
        (let [server    (str/replace server #".+@" "")      ;; remove any server name/token for now
              server*   (cond
                          ;; properly formatted
                          (re-matches #"^https?://.+" server)
                          server

                          (str/includes? server "//")
                          (throw (ex-info (str "Only http:// and https:// protocols currently supported for connection servers. Provided:" server)
                                          {:status 400 :error :db/invalid-connection}))

                          ;; default to http
                          :else (str "http://" server))
              ;; add port 8090 as a default
              server*   (if (re-matches #".+:[0-9]+" server*)
                          server*
                          (str server* ":8090"))
              is-https? (str/starts-with? server "https://")
              result*   (conj result server*)]
          (when-not (re-matches server-regex server*)
            (throw (ex-info (str "Invalid connection server, provide url and port only. Optionally specify http:// or https://. Provided: " server)
                            {:status 400 :error :db/invalid-connection})))
          (when (and https? (not= is-https? https?))
            (throw (ex-info (str "Connection servers must all be http or https, not a mix.")
                            {:status 400 :error :db/invalid-connection})))
          (if (empty? r)
            (shuffle result*)
            (recur r is-https? result*)))))))


(defn- closed?
  "Returns true if connection has been closed."
  [conn]
  (:close? @(:state conn)))


(defn- close-websocket
  "Closes websocket on connection if exists."
  [conn-id]
  (let [existing-socket (some-> (get-in server-connections-atom [conn-id :ws :socket])
                                (async/poll!))]
    (swap! server-connections-atom dissoc conn-id)
    (if existing-socket
      (xhttp/close-websocket existing-socket)
      false)))


(defn get-socket
  "Gets websocket from connection, or establishes one if not already done.

  Returns a core async promise channel. Check for exceptions."
  [conn]
  (go-try
    (or (get-in @server-connections-atom [(:id conn) :ws :socket])
        ;; attempt to connect
        (<? (establish-socket (:id conn) (:sub-chan conn) (:pub-chan conn) (:servers conn))))))


(defn get-server
  "returns promise channel, check for errors"
  [conn-id servers]
  (or (get-in @server-connections-atom [conn-id :server :chan])
      ;; attempt to connect
      (get-healthy-server conn-id servers)))


(defn default-publish-fn
  "Publishes message to the websocket associated with the connection."
  [conn message]
  (let [pub-chan  (:pub-chan conn)
        resp-chan (async/promise-chan)
        msg       (try* (json/stringify message)
                        (catch* e
                                (log/error "Unable to publish message on websocket. Error encoding JSON message: " message)
                                (async/put! resp-chan (ex-info (str "Error encoding JSON message: " message) {}))
                                nil))]
    (when msg
      (async/put! pub-chan [msg resp-chan]))
    resp-chan))

(defn msg-producer
  "Shuffles outgoing messages to the web socket in order."
  [{:keys [state req-chan publish]
    :as   conn}]
  (async/go-loop [i 0]
    (when-let [msg (async/<! req-chan)]
      (try*
       (let [_ (log/trace "Outgoing message to websocket: " msg)
             [operation data resp-chan opts] msg
             {:keys [req-id timeout] :or {req-id  (str (util/random-uuid))
                                          timeout 60000}} opts]
         (when resp-chan
           (swap! state assoc-in [:pending-req req-id] resp-chan)
           (async/go
             (let [[resp c] (async/alts! [resp-chan (async/timeout timeout)])]
               ;; clear request from state
               (swap! state update :pending-req #(dissoc % req-id))
               ;; return result
               (if (= c resp-chan)
                 resp
                 ;; if timeout channel comes back first, respond with timeout error
                 (ex-info (str "Request " req-id " timed out.")
                          {:status 408
                           :error  :db/timeout})))))
         (let [publisher  (or publish default-publish-fn)
               published? (async/<! (publisher conn [operation req-id data]))]
           (when-not (true? published?)
             (cond
               (util/exception? published?)
               (log/error published? "Error processing message in producer.")

               (nil? published?)
               (log/error "Error processing message in producer. Socket closed.")

               :else
               (log/error "Error processing message in producer. Socket closed. Published result" published?)))))
       (catch* e
               (let [[_ _ resp-chan] (when (sequential? msg) msg)]
                 (if (and resp-chan (channel? resp-chan))
                   (async/put! resp-chan e)
                   (log/error e (str "Error processing ledger request, no valid return channel: " (pr-str msg)))))))
      (recur (inc i)))))


(defn ping-transactor
  [conn]
  (let [req-chan (:req-chan conn)]
    (async/put! req-chan [:ping true])))


(defn msg-consumer
  "Takes messages from peer/ledger and processes them."
  [conn]
  (let [;; if we haven't received a message in at least this long, ping ledger.
        ;; after two pings, if still no response close connection (so connection closes before the 3rd ping, so 3x this time.)
        ping-transactor-after 2500
        {:keys [sub-chan]} conn]
    (async/go-loop [no-response-pings 0]
      (let [timeout (async/timeout ping-transactor-after)
            [msg c] (async/alts! [sub-chan timeout])]
        (cond
          ;; timeout, ping and wait
          (= c timeout)
          (if (= 2 no-response-pings)
            ;; assume connection dropped, close!
            (do
              (log/warn "Connection has gone stale. Perhaps network conditions are poor. Disconnecting socket.")
              (let [cb (:keep-alive-fn conn)]
                (cond

                  (nil? cb)
                  (log/trace "No keep-alive callback is registered")

                  (fn? cb)
                  (cb)

                  (string? cb)
                  #?(:cljs
                     ;; try javascript eval
                     (eval cb)
                     :clj
                     (log/warn "Unsupported clojure callback registered" {:keep-alive-fn cb}))

                  :else
                  (log/warn "Unsupported callback registered" {:keep-alive-fn cb})))
              (close-websocket (:id conn))
              (session/close-all-sessions (:id conn)))
            (do
              (ping-transactor conn)
              (recur (inc no-response-pings))))

          (nil? msg)
          (log/info "Connection closed.")

          (util/exception? msg)
          (do
            (log/error msg)
            (recur 0))

          :else
          (do
            (log/trace "Received message:" (pr-str (json/parse msg)))
            (conn-events/process-events conn (json/parse msg))
            (recur 0)))))))


(defn- default-storage-read
  "Default storage read function - uses ledger storage and issues http(s) requests."
  ([conn-id servers] (default-storage-read conn-id servers nil))
  ([conn-id servers opts]
   (let [{:keys [private jwt]} opts]
     (fn [k]
       (go-try
         (let [jwt' #?(:clj jwt
                       :cljs (or jwt
                                 (get-in @server-connections-atom [conn-id :token])))
               path         (str/replace k "_" "/")
               address      (async/<! (get-server conn-id servers))
               url          (str address "/fdb/storage/" path)
               headers      (cond-> {"Accept" #?(:clj  "avro/binary"
                                                 :cljs "application/json")}
                                    jwt' (assoc "Authorization" (str "Bearer " jwt')))
               headers*     (if private
                              (-> (http-signatures/sign-request "get" url {:headers headers} private)
                                  :headers)
                              headers)
               res          (<? (xhttp/get url {:request-timeout 5000
                                                :headers         headers*
                                                :output-format   #?(:clj  :binary
                                                                    :cljs :json)}))]

           res))))))


(defn- default-object-cache-fn
  "Default object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (if (nil? value-fn)
      (swap! cache-atom cache/evict k)
      (if-let [v (get @cache-atom k)]
        (do (swap! cache-atom cache/hit k)
            v)
        (let [v (value-fn k)]
          (swap! cache-atom cache/miss k v)
          v)))))


(defn- default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))


(defn- from-environment
  "Gets a specific key from the environment, returns nil if doesn't exist."
  [key]
  #?(:clj  (get environ/env key)
     :cljs nil))


(defn listeners
  "Returns list of listeners"
  [conn]
  (-> @(:state conn)
      (:listeners)))


(defn- add-listener*
  "Internal call to add-listener that uses the state atom directly."
  [conn-state network dbid key fn]
  (when-not (fn? fn)
    (throw (ex-info "add-listener fn paramer not a function."
                    {:status 400 :error :db/invalid-listener})))
  (when (nil? key)
    (throw (ex-info "add-listener key must not be nil."
                    {:status 400 :error :db/invalid-listener})))
  (swap! conn-state update-in
         [:listeners [network dbid] key]
         #(if %
            (throw (ex-info (str "add-listener key already in use: " (pr-str key))
                            {:status 400 :error :db/invalid-listener}))
            fn))
  true)


(defn- remove-listener*
  "Internal call to remove-listener that uses the state atom directly."
  [conn-state network dbid key]
  (if (get-in @conn-state [:listeners [network dbid] key])
    (do
      (swap! conn-state update-in [:listeners [network dbid]] dissoc key)
      true)
    false))


(defn add-listener
  "Registers a new listener function, fn,  on connection.

  Each listener must have an associated key, which is used to remove the listener
  when needed but is otherwise opaque to the function. Each key must be unique for the
  given network + dbid."
  [conn network dbid key fn]
  ;; load db to make sure ledger events subscription initiated
  (let [ledger (str network "/" dbid)
        db     (session/db conn ledger nil)]
    ;; check that db exists, else throw
    #?(:clj (when (util/exception? (async/<!! db))
              (throw (async/<!! db))))
    (add-listener* (:state conn) network dbid key fn)))


(defn remove-listener
  "Removes listener on given network + dbid for the provided key.

  The key is the same provided for add-listener when registering.

  Will return true if a function exists for that key and it was removed."
  [conn network dbid key]
  (remove-listener* (:state conn) network dbid key))


(defn add-token
  "Adds token to connection information so it is available to submit storage read requests.

  Returns true if successful, false otherwise."
  [conn token]
  (let [conn-id (:id conn)]
    (try*
      (swap! server-connections-atom update-in [conn-id :token] #(or % token))
      true
      (catch* e
        false))))

(defn- generate-connection
  "Generates connection object."
  [servers opts]
  (let [state-atom         (atom {:close?       false
                                  ;; map of transactors and the latest 'health' request results
                                  :health       {}
                                  ;; which of the transactors we are connected to
                                  :connected-to nil
                                  ;; web socket connection to ledger
                                  :socket       nil
                                  ;; web socket id
                                  :socket-id    nil
                                  ;; map of pending request ids to async response channels
                                  :pending-req  {}
                                  ;; map of listener functions registered. key is two-tuple of [network dbid],
                                  ;; value is vector of single-argument callback functions that will receive [header data]
                                  :listeners    {}})
        {:keys [storage-read storage-exists storage-write storage-rename storage-delete storage-list
                parallelism req-chan sub-chan pub-chan default-network group
                object-cache close-fn serializer
                tx-private-key private-key-file memory
                transactor? transact-handler publish meta memory?
                private keep-alive-fn]
         :or   {memory           1000000                    ;; default 1MB memory
                parallelism      4
                req-chan         (async/chan)
                sub-chan         (async/chan)
                pub-chan         (async/chan)
                memory?          false
                storage-write    (fn [k v] (throw (ex-info (str "Storage write was not implemented on connection, but was called to store key: " k) {})))
                serializer       #?(:clj  (avro-serde)
                                    :cljs (json-serde))
                transactor?      false
                private-key-file "default-private-key.txt"}} opts
        memory-object-size (quot memory 100000)             ;; avg 100kb per cache object
        _                  (when (< memory-object-size 10)
                             (throw (ex-info (str "Must allocate at least 1MB of memory for Fluree. You've allocated: " memory " bytes.") {:status 400 :error :db/invalid-configuration})))
        default-cache-atom (atom (default-object-cache-factory memory-object-size))
        object-cache-fn    (or object-cache
                               (default-object-cache-fn default-cache-atom))
        conn-id            (str (util/random-uuid))
        close              (fn []
                             (async/close! req-chan)
                             (async/close! sub-chan)
                             (async/close! pub-chan)
                             (close-websocket conn-id)
                             (swap! state-atom assoc :close? true)
                             ;; NOTE - when we allow permissions back in CLJS (browser), remove conditional below
                             #?(:clj  (dbfunctions/clear-db-fn-cache)
                                :cljs (when (identical? "nodejs" cljs.core/*target*)
                                        (dbfunctions/clear-db-fn-cache)))
                             (session/close-all-sessions conn-id)
                             (reset! default-cache-atom (default-object-cache-factory memory-object-size))
                             ;; user-supplied close function
                             (when (fn? close-fn) (close-fn))
                             (log/info "connection closed"))
        servers*           (normalize-servers servers transactor?)
        storage-read*      (or storage-read (default-storage-read conn-id servers* opts))
        storage-exists*    (or storage-exists storage-read (default-storage-read conn-id servers* opts))
        _                  (when-not (fn? storage-read*)
                             (throw (ex-info (str "Connection's storage-read must be a function. Provided: " (pr-str storage-read))
                                             {:status 500 :error :db/unexpected-error})))
        _                  (when-not (fn? storage-exists*)
                             (throw (ex-info (str "Connection's storage-exists must be a function. Provided: " (pr-str storage-exists))
                                             {:status 500 :error :db/unexpected-error})))
        _                  (when (and storage-write (not (fn? storage-write)))
                             (throw (ex-info (str "Connection's storage-write, if provided, must be a function. Provided: " (pr-str storage-write))
                                             {:status 500 :error :db/unexpected-error})))
        settings           {:meta             meta
                            ;; supplied static metadata, used mostly by ledger to add additional info
                            :id               conn-id
                            :servers          servers*
                            :state            state-atom
                            :req-chan         req-chan
                            :sub-chan         sub-chan
                            :pub-chan         pub-chan
                            :close            close
                            :group            group
                            :storage-list     storage-list
                            :storage-read     storage-read*
                            :storage-exists   storage-exists*
                            :storage-write    storage-write
                            :storage-rename   storage-rename
                            :storage-delete   storage-delete
                            :object-cache     object-cache-fn
                            :parallelism      parallelism
                            :serializer       serializer
                            :default-network  default-network
                            :transact-handler transact-handler ;; only used for transactors
                            :transactor?      transactor?
                            :memory           memory?
                            :publish          publish       ;; publish function for transactors
                            :tx-private-key   tx-private-key
                            :tx-key-id        (when tx-private-key
                                                #?(:clj  (crypto/account-id-from-private tx-private-key)
                                                   :cljs nil))
                            :keep-alive-fn    (when (or (fn? keep-alive-fn) (string? keep-alive-fn))
                                                keep-alive-fn)
                            :add-listener     (partial add-listener* state-atom)
                            :remove-listener  (partial remove-listener* state-atom)}]
    (map->Connection settings)))

(defn close!
  "Closes connection, returns true if close successful, false if already closed."
  [conn]
  (if (closed? conn)
    false
    (do
      ;; execute close function
      ((:close conn))
      true)))


(defn connect
  "Creates a connection to a ledger group server.
  Provide servers in either a sequence or as a string that is comma-separated."
  [servers & [opts]]
  (let [conn        (generate-connection servers opts)
        transactor? (:transactor? opts)]
    (when-not transactor?
      (async/go
        (let [socket (async/<! (get-socket conn))]
          (if (or (nil? socket)
                  (util/exception? socket))
            (do
              (log/error socket "Cannot establish connection to a healthy server, disconnecting.")
              (async/close! conn))
            ;; kick off consumer
            (msg-consumer conn)))))

    (msg-producer conn)

    conn))
