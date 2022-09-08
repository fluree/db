(ns fluree.db.messages.command
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.log :as log]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))

(defn with-auth
  [cmd-data private-key opts]
  (if-let [{:keys [auth] :as verified-auth} (:verified-auth opts)]
    (do (log/debug "Using verified auth:" auth)
        (assoc cmd-data :auth auth))
    (let [key-auth-id (crypto/account-id-from-private private-key)]
      (if-let [auth (:auth opts)]
        (assoc cmd-data
               :auth auth
               :authority (when-not (= auth key-auth-id)
                            key-auth-id))
        (assoc cmd-data :auth key-auth-id)))))

(defn txn->cmd-data
  [txn ledger timestamp private-key opts]
  (let [{:keys [expire nonce deps]
         :or   {nonce  timestamp
                expire (+ timestamp 300000)}}
        opts

        cmd-data {:type      :tx
                  :ledger    ledger
                  :tx        txn
                  :nonce     nonce
                  :expire    expire
                  :deps      deps}]
    (-> cmd-data
        (with-auth private-key opts)
        util/without-nils)))

(defn cmd-data->json
  [cmd-data]
  (try (json/stringify cmd-data)
       (catch Exception _
         (throw (ex-info (str "Transaction contains data that cannot be serialized into JSON.")
                         {:status 400 :error :db/invalid-tx})))))

(defn txn->json
  [txn ledger timestamp private-key opts]
  (-> txn
      (txn->cmd-data ledger timestamp private-key opts)
      cmd-data->json))

(defn with-id
  [{:keys [cmd] :as command}]
  (let [id (crypto/sha3-256 cmd)]
    (assoc command :id id)))

(defn sign
  [{:keys [cmd] :as command} private-key opts]
  (if-let [{:keys [signature signed]} (:verified-auth opts)]
    (assoc command
           :sig    signature
           :signed signed)
    (let [sig (crypto/sign-message cmd private-key)]
      (assoc command :sig sig))))

(defn build-and-sign
  [txn ledger timestamp private-key opts]
  (let [cmd (-> txn
                (txn->cmd-data ledger timestamp private-key opts)
                cmd-data->json)]
    (-> {:cmd cmd, :ledger ledger}
        with-id
        (sign private-key opts))))
