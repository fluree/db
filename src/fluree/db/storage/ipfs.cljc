(ns fluree.db.storage.ipfs
  (:refer-clojure :exclude [read list])
  (:require [clojure.string :as str]
            [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.storage.util :as store-util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]))

(defn ipfs-address
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:ipfs:" path)
    (str "fluree:ipfs://" path)))

(defn ipfs-write
  [ipfs-endpoint k v _]
  (go-try
    (let [content (if (string? v)
                    v
                    (json-ld/normalize-data v))

          {:keys [hash size] :as res} (<? (ipfs/add ipfs-endpoint k content))]
      (when-not size
        (throw
          (ex-info
            "IPFS publish error, unable to retrieve IPFS name."
            {:status 500 :error :db/push-ipfs :result res})))
      {:k hash
       :hash hash
       :address (ipfs-address hash)
       :size size})))

(defn ipfs-read
  [ipfs-endpoint address]

  (let [{:keys [ns local method]} (store-util/address-parts address)
        ipfs-path              (str "/" method "/" local)]
    (when-not (and (= "fluree" ns)
                   (#{"ipfs" "ipns"} method))
      (throw (ex-info (str "Invalid file type or method: " address)
                      {:status 500 :error :db/invalid-address})))
    (ipfs/cat ipfs-endpoint ipfs-path false)))

(defn ipfs-exists?
  "If we can't find the content within the default 5 seconds, then we say it doesn't exist."
  [ipfs-endpoint address]
  (go-try
    (let [resp (<? (ipfs-read ipfs-endpoint address))]
      (if (util/exception? resp)
        (if (= (-> resp ex-data :error) :xhttp/timeout)
          false
          (throw resp))
        (boolean resp)))))

(defrecord IpfsStore [ipfs-endpoint ipns]
  store-proto/Store
  (address [_ k] (ipfs-address k))
  (write [_ k v opts] (ipfs-write ipfs-endpoint k v opts))
  (list [_ prefix] (throw (ex-info "Unsupported operation IpfsStore method: list." {:prefix prefix})))
  (exists? [_ address] (ipfs-exists? ipfs-endpoint address))
  (read [_ address] (ipfs-read ipfs-endpoint address))
  (delete [_ address] (throw (ex-info "Unsupported operation IpfsStore method: delete." {:address address}))))

(defn create-ipfs-store
  [{:keys [:ipfs-store/server :ipfs-store/ipns] :as config}]
  (map->IpfsStore {:config config
                   :ipfs-endpoint (or server "http://127.0.0.1:5001/")}))
