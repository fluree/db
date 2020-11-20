(ns fluree.db.api.ledger
  (:require [fluree.db.session :as session]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.connection :as conn]
            [fluree.db.permissions :as permissions]
            [fluree.db.auth :as auth]
            [fluree.db.time-travel :as time-travel]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))


(defn root-db
  "Returns a queryable database from the connection for the specified ledger."
  ([conn ledger]
   (session/db conn ledger nil))
  ([conn ledger opts]
   (if-let [block (:block opts)]
     (let [pc (async/promise-chan)]
       (async/go
         (async/put! pc (try*
                          (-> (<? (session/db conn ledger nil))
                              (time-travel/as-of-block block)
                              (<?))
                          (catch* e e)))
         (async/close! pc))
       pc)
     ;; session/db returns a promise channel
     (session/db conn ledger nil))))

(defn- add-db-auth-sid
  "Resolves auth subject id from any identity value. Will
  throw an exception if it is unable to resolve to an established identity.
  Auth 0 is a special case. It:
  - Is short-hand to keeping 'local' root-level permissions. 'local' permissions
    may already be a restricted permissioned set of data, it simply is not further restricted.
  - Does not attempt to resolve to a subject ID (as it would error as '0' will not exist in the db)"
  [db auth]
  (go-try
    (cond
      (not auth)
      db

      (= 0 auth)
      (assoc db :auth 0)

      :else
      (let [auth-sid (<? (dbproto/-subid db auth))]
        (if auth-sid
          (assoc db :auth auth-sid)
          (throw (ex-info (str "Auth id: " auth " unknown.")
                          {:status 401
                           :error  :db/invalid-auth})))))))


(defn- add-db-permissions
  "Adds permissions to db. Permissions can either be explicitly stated with roles
  or it can be derived from an auth-id.
  This assumes the :auth on the db, if it was provided, is already resolved
  via add-db-auth-sid function."
  [db auth roles]
  (go-try
    (let [auth-sid    (cond
                        (nil? auth) nil
                        (= 0 auth) 0
                        auth (or (<? (dbproto/-subid db auth))
                                 (throw (ex-info (str "Auth id: " auth " unknown.")
                                                 {:status 401
                                                  :error  :db/invalid-auth}))))
          roles'      (cond
                        roles roles
                        auth-sid (<? (auth/roles db auth-sid))
                        :else nil)
          permissions (when roles'
                        (<? (permissions/permission-map db roles' :query)))]
      (assoc db :auth auth-sid
                :roles roles'
                :permissions permissions))))


(defn- syncTo-wait
  "Executes wait listener for updated db that is at least at block syncTo.
  Received listen-id which is used as key for connection listener, allowing
  anything with that key to cancel the listener.

  resp-port is the async port on which any successful response will be placed."
  [db syncTo listen-id resp-port]
  (let [{:keys [conn network dbid]} db
        newer-block? (fn [block] (>= block syncTo))
        event-fn     (fn [evt data]
                       (log/warn "NEW EVENT:" (pr-str evt) (pr-str data))
                       (when (and (= :local-ledger-update evt) (newer-block? (:block data)))
                         (conn/remove-listener conn network dbid listen-id)
                         (async/go                          ;; note: avoided async/pipe as I don't believe promise-chan from session/db technically 'closes'
                           (async/put! resp-port (async/<! (session/db conn (str network "/" dbid) nil))))))]

    ;; listener will monitor all blocks, add updated db to resp-port once > syncTo, and close listener
    (conn/add-listener conn network dbid listen-id event-fn)
    ;; a preemptive check if newer version of db already exists (db passed in could be old)
    (async/go
      (let [latest-db (async/<! (session/db conn (str network "/" dbid) nil))] ;; possible while setting up listener the block has come through, check again
        (when (newer-block? (:block latest-db))
          (async/put! resp-port latest-db))))))


(defn- syncTo-db
  [db syncTo syncTimeout]
  (assert (pos-int? syncTo) (str "syncTo must be a block number (positive integer), provided: " syncTo))
  (let [pc        (async/promise-chan)                      ;; final response channel - has db or timeout error
        {:keys [conn network dbid]} db
        listen-id (util/random-uuid)
        timeout   (if (pos-int? syncTimeout)
                    (min syncTimeout 120000)                ;; max 2 minutes
                    60000)                                  ;; 1 minute default
        res-port  (async/chan)]
    (if (>= (:block db) syncTo)
      ;; already current
      (async/put! pc db)
      ;; not current enough, need to monitor/wait
      (do
        ;; launch listener for new blocks, will put updated db on res-port when available
        (syncTo-wait db syncTo listen-id res-port)
        ;; listener or timeout will respond first, respond with error or updated db
        (async/go
          (let [timeout-ch (async/timeout timeout)
                updated-db (async/alt! timeout-ch :timeout
                                       res-port ([db] db))]
            (conn/remove-listener conn network dbid listen-id) ;; close listener to clean up
            (if (= :timeout updated-db)
              (async/put! pc (ex-info (str "Timeout waiting for block: " syncTo)
                                      {:status 400 :error :db/timeout}))
              (async/put! pc updated-db))))))
    pc))


(defn db
  "Returns a queryable database from the connection for the specified ledger."
  ([conn ledger]
   (root-db conn ledger nil))
  ([conn ledger {:keys [roles auth block syncTo syncTimeout] :as opts}]
   (let [pc (async/promise-chan)]
     (async/go
       (try*
         (let [dbx (cond-> (<? (session/db conn ledger nil))
                           syncTo (-> (syncTo-db syncTo syncTimeout) <?)
                           block (-> (time-travel/as-of-block block) <?)
                           roles (-> (add-db-permissions auth roles) <?) ;; should only ever have roles -or- auth
                           auth (-> (add-db-permissions auth roles) <?) ;; if both, auth overrides roles
                           )]
           (async/put! pc dbx))
         (catch* e
           (async/put! pc e)
           (async/close! pc))))
     ;; return promise chan immediately
     pc)))
