(ns fluree.db.operations
  (:require [fluree.db.messages.command :as cmd]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])))

#?(:clj (set! *warn-on-reflection* true))

(defn- conn-closed?
  "Returns true if connection has been closed."
  [conn]
  (:close? @(:state conn)))

(defn- formulate-op-header
  "Creates the operation header."
  ([operation ledger] (formulate-op-header operation ledger 60000))
  ([operation ledger timeout]
   (let [req-id (str (random-uuid))
         header {:ledger    ledger
                 :op        operation
                 :req-id    req-id
                 :timestamp (util/current-time-millis)
                 ;; 30 second timeout for now
                 :timeout   timeout}]
     header)))

(defn send-operation
  "Sends operation off, returns core async promise channel that will have eventual response
  or timeout response.

  Records the request-id which is stored in an atom along with the response channel.

  Incoming ledger notifications will look at request id and see if
  there is a matching request-id in the record with its corresponding
  async channel, and if so it will populate that channel with the response.

  We use the timeout to create a timeout channel, so if no transactions with
  the corresponding request id are 'seen' in that timeframe, a timeout response
  is given."
  ([conn operation data] (send-operation conn operation data nil))
  ([conn operation data opts]
   (let [pc (async/promise-chan)]
     (async/go
       (try*
         (if (conn-closed? conn)
           (throw (ex-info "Connection was closed." {:status 400 :error :db/invalid-connection}))
           (do
             (async/put! (:req-chan conn) [operation data pc opts])
             (log/trace "Put operation on connection req-chan: " [operation data pc opts])))
         (catch* e
           (async/put! pc e))))
     pc)))


(defn delete-ledger-async
  "A quick async response to confirm as initiated, but deletion happens in background.

  A final 'completed' message will be sent in the future, if anyone cares to hear it."
  ([session] (delete-ledger-async session nil))
  ([session {:keys [snapshot?] :as opts}]
   (let [{:keys [ledger-id network conn]} session]
     (send-operation conn :delete-ledger {:network   network
                                          :ledger-id ledger-id
                                          :snapshot? snapshot?}))))


(defn garbage-collect-async
  "A quick async response to confirm as initiated, but garbage collection happens in background.

  A final 'completed' message will be sent in the future, if anyone cares to hear it."
  [session & [{:keys [toBlock toTime]} :as opts]]
  (async/go
    (let [{:keys [network ledger-id conn]} session]
      (send-operation conn :garbage {:network   network
                                     :ledger-id ledger-id
                                     :toBlock   toBlock
                                     :toTime    toTime}))))


(defn snapshot-ledger-async
  "A quick async response to confirm, but archival happens in background.

  A final 'completed' message will be sent in the future, if anyone cares to hear it."
  [session opts]
  (let [{:keys [network ledger-id conn]} session]
    (send-operation conn :snapshot-ledger [network ledger-id])))


(defn command-async
  "Submits a command to connected tx-group server"
  [conn cmd]
  (send-operation conn :cmd cmd))

(defn signed-command-async
  ([conn command signing-key]
   (signed-command-async conn command signing-key {}))
  ([conn command signing-key opts]
   (let [signed-command (-> command
                            cmd/validate
                            (cmd/sign signing-key opts))]
     (send-operation :cmd signed-command))))


(defn unsigned-command-async
  "A response will not be returned until transaction is completed.

  An option of {:wait false} can be provided which will return 'true' when the transaction
  has been successfully persisted by the transactors, or throw an exception.

  By default transact will monitor completed blocks and only return once it sees
  the successful transaction completed, or it will return with an error."
  [conn command]
  (send-operation conn :unsigned-cmd (cmd/validate command)))


(defn ledger-info-async
  "Returns information about a ledger in a map, or empty map if ledger doesn't exist."
  [conn ledger]
  (send-operation conn :ledger-info ledger))

(defn ledger-status-async
  "Returns information about a ledger in a map, or empty map if ledger doesn't exist."
  [conn ledger]
  (:status (send-operation conn :ledger-info ledger)))

(defn ledger-stats-async
  "Returns stats about a ledger in a map, or empty map if ledger doesn't exist."
  [conn ledger]
  (send-operation conn :ledger-stats ledger))

(defn ledgers-async
  "Returns a list of ledgers in two-tuples of [network ledger-id]."
  [conn]
  (send-operation conn :ledger-list nil))


(defn transact-async
  "A response will not be returned until transaction is completed.

  An option of {:wait false} can be provided which will return 'true' when the transaction
  has been successfully persisted by the transactors, or throw an exception.

  By default transact will monitor completed blocks and only return once it sees
  the successful transaction completed, or it will return with an error."
  [conn tx-map]
  (log/trace "Sending transaction async: " tx-map)
  (send-operation conn :tx tx-map))


(defn subscribe
  "Starts a subscription to the session's ledger.

  Supported options:
  - auth    - auth, if specified, takes precedence
  - jwt     - valid jwt
  "
  ([session] (subscribe session nil))
  ([session opts]
   (let [{:keys [auth jwt]} opts
         conn        (:conn session)
         auth-or-jwt (or auth jwt)
         _           (log/trace "Subscribe to: " (:network session)
                                (:ledger-id session) auth-or-jwt)]
     (send-operation conn :subscribe [[(:network session) (:ledger-id session)]
                                      auth-or-jwt]))))


(defn unsubscribe
  "Unsubscribes from the session's ledger."
  [session]
  (let [conn (:conn session)]
    (send-operation conn :unsubscribe
                    [(:network session) (:ledger-id session)])))
