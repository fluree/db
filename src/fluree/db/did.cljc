(ns fluree.db.did
  (:require [fluree.crypto :as crypto]))

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

(defn private->did-map
  "Returns a complete did map from a private key."
  [private-key]
  (let [public  (crypto/pub-key-from-private private-key)
        auth-id (crypto/account-id-from-public public)
        did-id  (auth-id->did auth-id)]
    (did-map did-id public private-key)))