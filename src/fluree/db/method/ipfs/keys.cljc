(ns fluree.db.method.ipfs.keys
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.xhttp :as xhttp])
  (:refer-clojure :exclude [list key]))

#?(:clj (set! *warn-on-reflection* true))

(defn list
  "Returns a map of key names and corresponding key ids"
  [ipfs-endpoint]
  (go-try
    (let [url (str ipfs-endpoint "api/v0/key/list")
          res (<? (xhttp/post-json url {} nil))]
      (log/debug "IPNS keys http api response: " res)
      (->> res
           :Keys
           (reduce (fn [acc {:keys [Name Id]}]
                     (assoc acc Name Id))
                   {})))))

(defn address*
  "Like address, but pass in already resolve key 'list' via above command."
  [key-map key]
  (let [key* (or key "self")]
    (get key-map key*)))

(defn address
  "Returns the IPNS address for a specific IPNS key, or nil if does not exist.
  If key is nil, returns default key, which IPFS labels 'self'"
  [ipfs-endpoint key]
  (go-try
    (let [key-map (<? (list ipfs-endpoint))]
      (address* key-map key))))

(defn key
  "Returns the IPNS key for a specific address (opposite of address)"
  [ipfs-endpoint address]
  (go-try
    (let [key-map (<? (list ipfs-endpoint))]
      (some (fn [[key addr]] (when (= address addr) key)) key-map))))

(comment
  (clojure.core.async/<!! (list "http://127.0.0.1:5001/"))
  (clojure.core.async/<!! (key "http://127.0.0.1:5001/" "k51qzi5uqu5dllaos3uy3sx0o8gw221tyaiu2qwmgdzy5lofij0us0h4ai41az")))
