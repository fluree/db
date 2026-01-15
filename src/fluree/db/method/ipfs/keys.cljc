(ns fluree.db.method.ipfs.keys
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.xhttp :as xhttp])
  (:refer-clojure :exclude [list]))

#?(:clj (set! *warn-on-reflection* true))

(defn list
  "Returns a map of ipns profile names and corresponding profile addresses"
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
  [key-map ipns-profile]
  (let [ipns-profile* (or ipns-profile "self")]
    (get key-map ipns-profile*)))

(defn address
  "Returns the IPNS address for a specific IPNS profile name, or nil if it does not exist.
  If key is nil, returns default key, which IPFS labels 'self'"
  [ipfs-endpoint ipns-profile]
  (go-try
    (let [profiles-map (<? (list ipfs-endpoint))]
      (address* profiles-map ipns-profile))))

(defn profile
  "Returns the IPNS key for a specific address (opposite of address)"
  [ipfs-endpoint address]
  (go-try
    (let [profile-map (<? (list ipfs-endpoint))]
      (some (fn [[profile addr]] (when (= address addr) profile)) profile-map))))
