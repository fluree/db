(ns fluree.db.connection-js
  (:require [cljs.core.async :as async]
            [alphabase.core :as alphabase]
            [fluree.db.connection :as connection]
            [fluree.db.operations :as ops]
            [fluree.db.session :as session]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.token-auth :as token-auth]))


(defn dbaas?
  "Returns open-api? setting from connection object"
  [conn]
  (if (true? (-> (:flureedb-settings conn) deref :dbaas?)) true false))

(defn open-api?
  "Returns open-api? setting from connection object"
  [conn]
  (-> (:flureedb-settings conn) deref :open-api?))


;; ======================================
;;
;; Token validation
;;
;; ======================================

(defn validate-token
  "Verifies that the jwt token has not expired.
  Only returns when token is valid.  Otherwise, an exception is thrown."
  [conn jwt]
  (let [secret (-> (:flureedb-settings conn) deref :jwt-secret
                   (alphabase/base-to-byte-array :hex))]
    (token-auth/verify-jwt secret jwt)))


;; ======================================
;;
;; Connection
;;
;; ======================================
(defn authenticate
  "Authenticate with Fluree On-Demand"
  ([conn account user password] (authenticate conn account user password nil))
  ([conn account user password expireSeconds] (authenticate conn account user password expireSeconds nil))
  ([conn account user password expireSeconds syncTo]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [user*  (cond
                          (number? user)
                          user

                          (string? user)
                          ["_user/username" user]

                          :else
                          user)
                 data   {:account       account
                         :user          user*
                         :password      password
                         :expireSeconds expireSeconds
                         :syncTo        syncTo}
                 result (async/<! (ops/send-operation conn :authenticate data))
                 token  (:token result)]
             (swap! (:token conn) (constantly token))       ; attach token to connection
             (connection/add-token conn token)              ; attach token to conn info for default storage reads
             (resolve result))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn connect
  "Connect to a ledger server using URL address. If using a ledger group, multiple addresses can be
   supplied, separated by commas."
  ([servers-string] (connect servers-string nil))
  ([servers-string opts]
   (let [conn (-> (connection/connect servers-string opts)
                  (assoc-in [:flureedb-settings] (atom {}))
                  (assoc-in [:token] (atom {})))
         pc   (async/promise-chan)
         _    (async/go
                (async/put! (:req-chan conn) [:settings nil pc nil])
                (async/take! pc
                             (fn [x]
                               (when-not (nil? x)
                                 (doseq [k (keys x)]
                                   (swap! (:flureedb-settings conn) assoc-in [k] (get x k nil)))))))]
     conn)))


(defn connect-p
  "Connect to a ledger server using URL address. If using a ledger group, multiple addresses can be
   supplied, separated by commas.

   Returns a promise that eventually contains the connection object."
  ([servers-string] (connect-p servers-string nil))
  ([servers-string opts]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [conn (-> (connection/connect servers-string opts)
                          (assoc-in [:flureedb-settings] (atom {}))
                          (assoc-in [:token] (atom {})))
                 pc   (async/promise-chan)]
             (do
               (async/put! (:req-chan conn) [:settings nil pc nil])
               (async/take! pc
                            (fn [x]
                              (do
                                (when-not (nil? x)
                                  (doseq [k (keys x)]
                                    (swap! (:flureedb-settings conn) assoc-in [k] (get x k nil))))
                                (resolve conn))))))
           (catch :default e
             (log/error e)
             (reject e))))))))


(defn close
  "Closes a connection"
  [conn]
  (connection/close! conn))




(defn check-connection
  "Returns true when the connection is properly configured to access the server.
  If jwt tokens are used; will also verify that the token has not expired.
  Otherwise, throws an error."
  ([conn] (check-connection conn nil))
  ([conn opts]
   (let [open-api? (-> (:flureedb-settings conn) deref :open-api?)
         token     (:jwt opts)
         has-auth? (or (:auth opts) (:auth-id opts) token)
         _         (when (and (not open-api?) (not has-auth?))
                     (throw (ex-info "To access the server, either open-api must be true or a valid auth must be available."
                                     {:status 401
                                      :error  :db/invalid-request})))
         _         (when token
                     (validate-token conn token))]
     true)))


;; ======================================
;;
;; Listeners
;;
;; ======================================

(defn listen
  "Listens to all events of a given ledger. Supply a ledger identity,
  any key, and a two-argument function that will be called with each event.

  The key is any arbitrary key, and is only used to close the listener via close-listener,
  otherwise it is opaque to the listener.

  The callback function's first argument is the event header/metadata and the second argument
  is the event data itself."
  [conn ledger key callback]
  (let [[network ledger-id] (session/resolve-ledger conn ledger)
        cb* (fn [header data]
              (callback (clj->js header) (clj->js data)))]
    (connection/add-listener conn network ledger-id key cb*)))


(defn close-listener
  "Closes a listener."
  [conn ledger key]
  (let [[network ledger-id] (session/resolve-ledger conn ledger)]
    (connection/remove-listener conn network ledger-id key)))


(defn listeners
  "Return a list of listeners currently registered for each ledger along with their respective keys."
  [conn]
  (-> (connection/listeners conn)
      (clj->js)))


;; ======================================
;;
;; Password Auth
;;
;; ======================================
(defn ^:private password-enabled?
  "Returns true when the ledger server/group supports password authentication"
  [conn]
  (-> (:flureedb-settings conn) deref :password-enabled?))

(defn password-generate
  "Generates a password auth record for an existing role, or a user.
  The user may exist, or if createUser? is true, the user is created.

  Returns a promise that eventually contains the token or an exception"
  [conn ledger password map-data]
  (js/Promise.
    (fn [resolve reject]
      (async/go
        (try
          (let [_    (when-not (password-enabled? conn)
                       (throw (ex-info "Password authentication is not enabled."
                                       {:status 401
                                        :error  :db/no-password-auth})))

                _    (when-not ledger
                       (throw (ex-info "A ledger must be supplied."
                                       {:status 400
                                        :error  :db/invalid-request})))

                _    (when-not password
                       (throw (ex-info "A password must be supplied."
                                       {:status 400
                                        :error  :db/invalid-request})))

                data (-> map-data
                         (assoc :ledger ledger :password password)
                         (util/without-nils))
                pc   (async/promise-chan)]
            (do
              (async/put! (:req-chan conn) [:pw-generate data pc nil])
              (async/take! pc
                           (fn [result]
                             (cond

                               (when (nil? result)
                                 (throw (ex-info "The password auth could not be generated."
                                                 {:status 400
                                                  :error  :db/invalid-request})))

                               (when (:error result)
                                 (reject (clj->js result)))

                               (instance? ExceptionInfo result)
                               (let [err-data (ex-data result)
                                     err-resp {:status  (or (:status err-data) 400)
                                               :message (ex-message result)
                                               :error   (or (:error err-data) :db/invalid-request)}]
                                 (reject (clj->js err-resp)))

                               :else
                               (resolve (clj->js result)))))))
          (catch :default e
            (reject (clj->js e))))))))

(defn password-login
  "Returns a JWT token if successful.
  Must supply ledger, password and either user or auth identifier.
  Expire is optional
  - ledger   - ledger identifier
  - password - plain-text password
  - user     - _user/username (TODO: should allow any _user ident in the future)
  - auth     - _auth/id (TODO: should allow any _auth ident in the future)
  - expire   - requested time to expire in milliseconds"
  ([conn ledger password user] (password-login conn ledger password user nil nil))
  ([conn ledger password user auth expire]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [
                 _    (when-not (password-enabled? conn)
                        (throw (ex-info "Password authentication is not enabled."
                                        {:status 401
                                         :error  :db/no-password-auth})))
                 _    (when-not ledger
                        (throw (ex-info "A ledger must be supplied in the provided JSON."
                                        {:status 400
                                         :error  :db/invalid-request})))
                 _    (when-not password
                        (throw (ex-info "A password must be supplied in the provided JSON."
                                        {:status 400
                                         :error  :db/invalid-request})))
                 _    (when-not (or user auth)
                        (throw (ex-info "A user identity or auth identity must be supplied."
                                        {:status 400
                                         :error  :db/invalid-request})))
                 data (-> {:ledger ledger :password password :user user :auth auth :expire expire}
                          (util/without-nils))
                 pc   (async/promise-chan)]
             (do
               (async/put! (:req-chan conn) [:pw-login data pc nil])
               (async/take! pc
                            (fn [result]
                              (cond

                                (when (nil? result)
                                  (throw (ex-info "A token could not be generated for the identity and password combination."
                                                  {:status 400
                                                   :error  :db/invalid-request})))

                                (when (:error result)
                                  (reject (clj->js result)))

                                (instance? ExceptionInfo result)
                                (let [err-data (ex-data result)
                                      err-resp {:status  (or (:status err-data) 400)
                                                :message (ex-message result)
                                                :error   (or (:error err-data) :db/invalid-request)}]
                                  (reject (clj->js err-resp)))

                                :else
                                (do
                                  (connection/add-token conn result)
                                  (resolve (clj->js result))))))))
           (catch :default e
             (reject (clj->js e)))))))))

(defn renew-token
  "Renews a JWT token if successful.

  Returns a promise that eventually contains the token or an exception"
  ([conn jwt] (renew-token conn jwt nil))
  ([conn jwt expire]
   (js/Promise.
     (fn [resolve reject]
       (async/go
         (try
           (let [_    (when-not (password-enabled? conn)
                        (throw (ex-info "Password authentication is not enabled."
                                        {:status 401
                                         :error  :db/no-password-auth})))
                 data (-> {:jwt jwt :expire expire}
                          (util/without-nils))
                 pc   (async/promise-chan)]
             (do
               (async/put! (:req-chan conn) [:pw-renew data pc nil])
               (async/take! pc
                            (fn [result]
                              (cond

                                (when (nil? result)
                                  (throw (ex-info "The token could not be renewed."
                                                  {:status 400
                                                   :error  :db/invalid-request})))

                                (when (:error result)
                                  (reject (clj->js result)))

                                :else
                                (do
                                  (connection/add-token conn result)
                                  (resolve (clj->js result))))))))
           (catch :default e
             (reject (clj->js e)))))))))