(ns fluree.sdk.webworker
  (:require [cljs.core.async :as async]
            [fluree.db.query.api :as q]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [goog.object]))

(def ^:private conn-register (atom {}))

(defn- postMessage
  [message]
  (.postMessage js/self (clj->js message)))

(defn- conn-id->db
  [conn-id time]
  (let [{:keys [conn ledger]} (get @conn-register conn-id)]
    :TODO #_(l/root-db conn ledger {:block time})))

(defn- obj->clj
  "Parses a (nested) JavaScript object into a Clojure map"
  [obj]
  (if (goog.isObject obj)
    (reduce (fn [result key]
              (let [v (goog.object/get obj key)]
                (if (= "function" (goog/typeOf v))
                  result
                  (assoc result (keyword key) (obj->clj v)))))
            {} (goog.object/getKeys obj))
    obj))

(defmulti ^:private worker-action (fn [conn-id event & _] event))

(defmethod worker-action :setState
  [conn-id _ id state-update]

  (postMessage {:conn  conn-id
                :event "setState"
                :ref   id
                :data  (clj->js state-update)}))

(defmethod worker-action :connStatus
  [conn-id _ id response]
  (postMessage {:conn  conn-id
                :event "connStatus"
                :ref   id
                :data  (clj->js response)}))

(defmethod worker-action :connClosed
  [conn-id _ ref response]
  (postMessage {:conn  conn-id
                :event "connClosed"
                :ref   ref
                :data  response}))

(defmethod worker-action :connReset
  [conn-id _ ref response]
  (postMessage {:conn  conn-id
                :event "connReset"
                :ref   ref
                :data  response}))

(defmethod worker-action :setTransact
  [conn-id _ ref response]
  (postMessage {:conn  conn-id
                :event "setTransact"
                :ref   ref
                :data  (clj->js response)}))

;(defmethod worker-action :connLogout
;  [conn _ ref]
;  (let [conn-id  (:id conn)
;        settings (-> (get-in @conn-register [conn-id :settings]))]
;    ;; first close connection
;    (close conn-id)
;    ;; then use original settings to establish a new connection, but first clear out
;    ;; any credentials
;    (aset settings "anonymous" true)
;    (aset settings "token" nil)
;    (connect settings)
;    (postMessage {:conn  conn-id
;                  :event "connLogout"
;                  :ref   ref
;                  :data  true}))
;  )

(defmethod worker-action :connInit
  [_ _]
  ;; let server know we're alive
  (postMessage {:conn  0
                :event "connInit"}))

(defn- error-return-map
  "Send an error map back to the worker invoker"
  [error]
  (let [default-error {:message (or (ex-message error) "Unknown Error.")
                       :status  500
                       :error   :db/unexpected-error}
        e-map         (merge default-error (ex-data error))]
    {:error  e-map
     :status "error"}))

(defn- process-query
  "Process a query for a specific component id and return result with a :setState call."
  [conn-id id conn-opts]
  (let [flureeql (get-in @conn-register [conn-id :queries id])

        jwt      (get-in @conn-register [conn-id :jwt])
        opts'    (-> (merge conn-opts (:opts flureeql))
                     (assoc :jwt jwt))]
    (when flureeql
      (async/go
        (let [db  (conn-id->db conn-id (:forceTime opts'))
              res (async/<! (q/query db (assoc flureeql :opts opts')
                                     {:format :fql}))
              ret (if (util/exception? res)
                    (error-return-map res)
                    {:result res :status "loaded"})]
          (worker-action conn-id :setState id ret))))))

(defn- process-all-queries
  "Re-execute every registered query."
  [conn-id]
  (let [{:keys [queries opts]} (get @conn-register conn-id)
        query-ids (keys queries)]
    (doseq [id query-ids]
      (process-query conn-id id opts))))

(defn- ledger-listener
  [conn ledger conn-id]
  :TODO
  #_(let [[network ledger-id] (session/resolve-ledger conn ledger)
          cb (fn [header data]
               (async/go
                 (async/<! (async/timeout 100))
                 (process-all-queries conn-id)))]
      (connection/add-listener conn network ledger-id conn-id cb)))

(defn- remove-conn-listener
  [conn conn-id ledger]
  :TODO
  #_(let [[network ledger-id] (session/resolve-ledger conn ledger)]
      (connection/remove-listener conn network ledger-id conn-id)))

(defn- register-connection
  "Registers new connection with all of its items."
  [conn config queries]
  (let [{:keys [servers ledger id log compact private]} config]
    (swap! conn-register assoc id {:conn    conn
                                   :config  config
                                   :servers servers
                                   :ledger  ledger
                                   :private private
                                   :queries (or queries {})
                                   :log     log
                                   :opts    {:compact compact} ;; default query options
                                   :closed  false})))

(defn- connect*
  "Creates a new connection from existing configuration"
  [conn-id ref]
  (let [conn-data (get @conn-register conn-id)
        {:keys [config queries jwt]} conn-data
        {:keys [servers ledger id keepAlive]} config
        cb        (if keepAlive
                    (fn []
                      (async/go
                        (async/<! (async/timeout 100))
                        (connect* id ref)))
                    nil)
        opts      (assoc-in config [:keep-alive-fn] cb)]
    (-> :TODO #_(conn-handler/connect-p servers opts)
        (.then (fn [conn]
                 (when jwt
                   :TODO #_(conn-handler/check-connection conn {:jwt jwt}))
                 (register-connection conn config queries)
                 (ledger-listener conn ledger id)
                 (worker-action conn-id :connReset ref {:status  200
                                                        :message "Connection reset"})
                 (process-all-queries conn-id)))
        (.catch (fn [error]
                  (let [error-data (if-let [ex-data (ex-data error)]
                                     (merge {:status  (or (:status ex-data) 500)
                                             :message (ex-message error)}
                                            ex-data)
                                     {:status  500
                                      :message (str "Unexpected error: " (pr-str error))})]
                    (worker-action conn-id :connReset ref error-data)))))))

(defn- close-connection*
  ([conn-id] (close-connection* conn-id false))
  ([conn-id save-config?]
   (let [conn-data  (get @conn-register conn-id)
         {:keys [conn ledger config jwt]} conn-data
         new-config (if save-config?
                      {:closed true :config config :jwt jwt}
                      {:closed true})]
     (if (or (nil? conn-data) (true? (:closed conn-data)))
       ;; connection either doesn't exist or is already closed.
       (throw (ex-info "Connection doesn't exist, or is already closed."
                       {:status 400
                        :error  :db/invalid-connection}))
       (do
         (remove-conn-listener conn conn-id ledger)
         ;; (conn-handler/close conn) ; TODO
         (swap! conn-register assoc conn-id new-config)
         {:status  200
          :message "Connection closed."})))))

(defn- close-connection
  [conn-id ref]
  (try
    (worker-action conn-id :connClosed ref (close-connection* conn-id))
    (catch :default e
      (let [msg  (or (ex-message e) "Unexpected error.")
            data (or (ex-data e) {:status 500
                                  :error  :db/unexpected-error})]
        (worker-action conn-id :connClosed ref (assoc data :message msg))))))

(defn- reset-connection
  [conn-id ref]
  (try
    (let [save-config? true]
      (close-connection* conn-id save-config?)
      (connect* conn-id ref))
    (catch :default e
      (let [msg  (or (ex-message e) "Unexpected error.")
            data (or (ex-data e) {:status 500
                                  :error  :db/unexpected-error})]
        (worker-action conn-id :connReset ref (assoc data :message msg))))))

(defn- connect-p
  "Open new connection to a Fluree instance"
  [config ref]
  (let [config* (js->clj config :keywordize-keys true)
        {:keys [servers ledger id keepAlive]} config*
        _       (when (:log config*)                        ;; set log level to finest if log: true
                  (log/set-level! :finest))
        cb      (if keepAlive
                  (fn []
                    (async/go
                      (async/<! (async/timeout 100))
                      (connect* id ref)))
                  nil)
        opts    (assoc-in config* [:keep-alive-fn] cb)]
    (-> :TODO #_(conn-handler/connect-p servers opts)
        (.then (fn [conn]
                 (register-connection conn config* nil)
                 (ledger-listener conn ledger id)
                 (worker-action id :connStatus ref {:status  200
                                                    :message "Connection is ready."})))
        (.catch (fn [error]
                  (worker-action id :connStatus ref {:status  500
                                                     :message (str error)}))))))

(defn- conn-closed?
  "Returns true if connection has been closed."
  [conn-id]
  (get-in @conn-register [conn-id :closed]))

(defn- unregisterQuery
  [conn-id ref]
  (swap! conn-register update-in [conn-id :queries] dissoc ref))

(defn- registerQuery
  "Registers a new flureeQL query. 'opts' gets merged in with the flureeQL.opts, and is there
  to ease adding options to a GraphQL (and later other string-based queries like SPARQL and SQL),
  which do not have the same ability to dynamically add options."
  [conn-id ref js-flurql force-update?]
  (let [flureeQL (js->clj js-flurql :keywordize-keys true)]
    ;; Only process query if different than existing query. If query hasn't changed
    (when (or (not= flureeQL (get-in @conn-register [conn-id :queries ref]))
              (true? force-update?))
      (swap! conn-register update-in [conn-id :queries] assoc ref flureeQL)
      (process-query conn-id ref (get-in @conn-register [conn-id :opts])))
    true))

(defn- transact
  "Submits a transaction across a websocket to a fluree instance"
  [conn-id ref map-data]
  :TODO
  #_(let [{:keys [conn ledger jwt]} (get @conn-register conn-id)
          _    (when-not ledger
                 (worker-action conn-id :login ref
                                {:status  400
                                 :message "Connection missing Ledger information. Password authentication is specific to a ledger."}))
          {:keys [txn options]} map-data
          txn' (if (string? txn)
                 (json/parse txn)
                 txn)
          opts (if options
                 (-> (select-keys options (keys options))
                     (assoc :jwt jwt))
                 {:jwt jwt})]
      (async/go
        (let [result (async/<! (fdb-js/transact-async conn ledger txn' (util/without-nils opts)))]
          (if (util/exception? result)
            (let [error (or (ex-data result)
                            {:status 500
                             :error  :db/unexpected-error})]
              (worker-action conn-id :setTransact ref {:error  (assoc error :message (ex-message result))
                                                       :status (:status error)}))
            (worker-action conn-id :setTransact ref {:result result
                                                     :status 200}))))))

(defn- js-array->clj-list
  "Takes javascript array and move it into a clojurescript list without
  converting individual elements."
  [js-array]
  (map #(aget js-array %) (range (.-length js-array))))

(defn decode-message
  "Main handler function for worker events
  Even data is a javascript object with the following keys:
  - action - the action to perform
  - params - parameters for the action we are calling
  - conn - the connection ID
  - ref - the reference ID to include in the response
  "
  [event]
  (try

    (let [data       (aget event "data")
          action-str (aget data "action")
          conn-id    (aget data "conn")
          ref        (aget data "ref")
          params-js  (aget data "params")
          ;; convert params array into a list, but don't convert the elements to CLJ
          params     (cond
                       (or (= action-str "pwGenerate")
                           (= action-str "transact"))
                       (obj->clj (first params-js))

                       :else
                       (js-array->clj-list params-js))]

      (if (conn-closed? conn-id)
        (when (not= action-str "unregisterQuery")           ;; ignore unregisterQuery actions after connection closed
          (postMessage {:conn  conn-id
                        :event action-str
                        :ref   ref
                        :data  #js {:status  400
                                    :message "Connection has been closed."}}))
        (cond
          (= action-str "connect")
          (connect-p (first params) ref)

          (= action-str "close")
          (close-connection conn-id ref)

          (= action-str "reset")
          (reset-connection conn-id ref)

          (= action-str "registerQuery")
          (apply registerQuery conn-id params)

          (= action-str "unregisterQuery")
          (unregisterQuery conn-id ref)

          (= action-str "transact")
          (transact conn-id ref params)

          :else
          (throw (ex-info (str "Unknown action: " action-str) {:status 400})))))
    (catch :default e
      (let [data       (aget event "data")
            error-data (if-let [ex-data (ex-data e)]
                         (merge {:status  (or (:status ex-data) 500)
                                 :message (ex-message e)}
                                ex-data)
                         {:status  500
                          :message (str "Unexpected error: " (pr-str e))})]
        (js/console.error "Error executing message:" data)
        (js/console.error (pr-str error-data))
        (postMessage {:conn  (aget data "conn")
                      :event (aget data "action")
                      :ref   (aget data "ref")
                      :data  error-data})))))

(defn log-error
  "Log errors in this web worker"
  [error]
  (js/console.error error))

(defn init
  []
  ;; when we detect that we are in a web worker, register the onmessage handler
  (when-let [worker? (not (.-document js/self))]
    (set! (.-onerror js/self) log-error)
    (set! (.-onmessage js/self) decode-message))
  ;; let server know we're alive
  (worker-action nil :connInit))
