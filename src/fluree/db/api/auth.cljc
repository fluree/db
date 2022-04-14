(ns fluree.db.api.auth
  (:require [fluree.crypto :as crypto]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.operations :as ops]
            [clojure.core.async :as async]))

(defn sign
  "Returns a signature for a message given provided private key."
  [message private-key]
  (crypto/sign-message message private-key))


(defn public-key-from-private
  "Returns a public key given a private key."
  [private-key] (crypto/pub-key-from-private private-key))


(defn public-key
  "Returns a public key from a message and a signature."
  [message signature] (crypto/pub-key-from-message message signature))


(defn new-private-key
  "Generates a new private key, returned in a map along with
  the public key and account id. Return keys are :public, :private, and :id."
  []
  (let [kp      (crypto/generate-key-pair)
        account (crypto/account-id-from-private (:private kp))]
    (assoc kp :id account)))


(defn set-default-key-async
  "Sets a new default private key for the entire tx-group, network or db level.
  This will only succeed if signed by the default private key for the tx-group,
  or if setting for a ledger-id, either the tx-group or network.

  It will overwrite any existing default private key.

  Returns core async channel that will respond with true or false, indicating success."
  ([conn private-key] (set-default-key-async conn nil nil private-key nil))
  ([conn network private-key] (set-default-key-async conn network nil private-key nil))
  ([conn network ledger-id private-key] (set-default-key-async conn network ledger-id private-key nil))
  ([conn network ledger-id private-key opts]
   (let [{:keys [nonce expire signing-key]} opts
         timestamp (System/currentTimeMillis)
         nonce     (or nonce timestamp)
         expire    (or expire (+ timestamp 30000)) ;; 5 min default
         cmd-map   {:type        :default-key
                    :network     network
                    :ledger-id   ledger-id
                    :private-key private-key
                    :nonce       nonce
                    :expire      expire}
         cmd       (when signing-key
                     (-> cmd-map
                         (util/without-nils)
                         (json/stringify)))
         sig       (when signing-key
                     (crypto/sign-message cmd signing-key))]
     (if signing-key
       (ops/command-async conn {:cmd cmd :sig sig})
       (ops/unsigned-command-async conn cmd-map)))))

(defn set-default-key
  "Sets a new default private key for the entire tx-group, network or db level.
  This will only succeed if signed by the default private key for the tx-group,
  or if setting for a ledger-id, either the tx-group or network.

  It will overwrite any existing default private key.

  Returns a promise of true or false, indicating success."
  ([conn private-key] (set-default-key-async conn nil nil private-key nil))
  ([conn network private-key] (set-default-key-async conn network nil private-key nil))
  ([conn network ledger-id private-key] (set-default-key-async conn network ledger-id private-key nil))
  ([conn network ledger-id private-key opts]
   (let [p (promise)]
     (async/go
       (deliver p (async/<! (set-default-key-async conn network ledger-id private-key opts))))
     p)))
