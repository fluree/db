(ns fluree.db.did
  (:require [fluree.crypto :as crypto]
            [alphabase.core :as alphabase]
            [alphabase.base58 :as base58]
            [clojure.string :as str]))

;; did operations

#?(:clj (set! *warn-on-reflection* true))

(defn did-map
  "Returns Fluree standard did-map based on values."
  [did public private]
  {:id      did
   :public  public
   :private private})

(defn auth-id->did
  "Takes legacy Fluree _auth/id value and returns a did id for it."
  [auth-id]
  (str "did:fluree:" auth-id))

(defn auth-id->did-map
  "Takes legacy Fluree _auth/id value and returns a did map for it."
  [auth-id]
  (did-map (auth-id->did auth-id) nil nil))

(defn private->did
  [private-key]
  (let [acct-id (crypto/account-id-from-private private-key)]
    (str "did:fluree:" acct-id)))

(defn private->did-map
  "Returns a complete did map from a private key."
  [private-key]
  (let [public  (crypto/pub-key-from-private private-key)
        auth-id (crypto/account-id-from-public public)
        did-id  (auth-id->did auth-id)]
    (did-map did-id public private-key)))

;; https://github.com/multiformats/multicodec/blob/master/table.csv
(def secp256k1-pub
  "The multicodec prefix for a secp256k1 public key."
  "e7")

(defn encode-did-key
  "Encodes a secp256k1 public key as a base58 multibase did:key."
  [pubkey]
  (let [pubkey-header secp256k1-pub]
    (str "did:key:z" (base58/encode (alphabase/hex->bytes (str pubkey-header pubkey))))))

;; https://github.com/multiformats/multibase/blob/master/multibase.csv
(def base58btc
  "The multibase prefix for a base58btc encoded string."
  "z")

(defn decode-did-key
  "Return the hex encoded public key from a did:key, or nil if it is not a properly
  encoded secp256k1 public key."
  [did]
  (let [[_ _ multibase-value] (str/split did #":")
        prefix                (str (first multibase-value))
        base-key              (subs multibase-value 1)
        _                     (when (not= prefix base58btc)
                                (throw (ex-info (str "The prefix " (pr-str prefix) " does not map to a supported multibase encoding.")
                                                {:value multibase-value
                                                 :prefix prefix})))
        multicodec            (alphabase/bytes->hex (base58/decode base-key))
        pubkey-header         (subs multicodec 0 2)
        pubkey                (subs multicodec 2)]
    (when (not= pubkey-header secp256k1-pub)
      (throw (ex-info (str "The multicodec header " (pr-str pubkey-header) " does not map to a supported multicodec encoding.")
                      {:value multicodec
                       :header pubkey-header})))
    pubkey))
