(ns fluree.db.method.ipfs.core
  (:require [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.method.ipfs.push :refer [push!]]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn default-commit-fn
  "Default push function for IPFS"
  [ipfs-endpoint]
  (fn [data]
    (go-try
      (let [json (json-ld/normalize-data data)
            res  (<? (ipfs/add ipfs-endpoint json))
            {:keys [name]} res]
        (when-not name
          (throw (ex-info (str "IPFS publish error, unable to retrieve IPFS name. Response object: " res)
                          {:status 500 :error :db/push-ipfs})))
        (assoc res :address (str "fluree:ipfs://" name))))))

(defn default-push-fn
  "Default publish function updates IPNS record based on a
  provided Fluree IPFS database ID, i.e.
  fluree:ipfs:<ipfs cid>

  Returns an async promise-chan that will eventually contain a result."
  [ipfs-endpoint]
  (fn [commit-metadata]
    (push! ipfs-endpoint commit-metadata)))

(defn default-read-fn
  "Default reading function for IPFS. Reads either IPFS or IPNS docs"
  [ipfs-endpoint]
  (fn [file-key]
    (when-not (string? file-key)
      (throw (ex-info (str "Invalid file key, cannot read: " file-key)
                      {:status 500 :error :db/invalid-commit})))
    (let [[address path] (str/split file-key #"://")
          [type method] (str/split address #":")
          ipfs-cid (str "/" method "/" path)]
      (when-not (and (= "fluree" type)
                     (#{"ipfs" "ipns"} method))
        (throw (ex-info (str "Invalid file type or method: " file-key)
                        {:status 500 :error :db/invalid-commit})))
      (ipfs/cat ipfs-endpoint ipfs-cid))))
