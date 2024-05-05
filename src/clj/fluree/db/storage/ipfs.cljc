(ns fluree.db.storage.ipfs
  (:require [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [clojure.core.async :refer [<!]]
            [clojure.string :as str]))

(def method-name "ipfs")

(defn build-ipfs-path
  [method local]
  (str/join "/" ["" method local]))

(defn ipfs-address
  [path]
  (storage/build-fluree-address method-name path))

(defrecord IpfsStore [endpoint]
  storage/Store
  (write [_ path v]
    (go-try
      (let [content (if (string? v)
                      v
                      (json-ld/normalize-data v))

            {:keys [hash size] :as res} (<? (ipfs/add endpoint path content))]
        (when-not size
          (throw
            (ex-info
              "IPFS publish error, unable to retrieve IPFS name."
              {:status 500 :error :db/push-ipfs :result res})))
        {:path    hash
         :hash    hash
         :address (ipfs-address hash)
         :size    size})))

  (list [_ prefix]
    (throw (ex-info "Unsupported operation IpfsStore method: list." {:prefix prefix})))

  (exists? [_ address]
    (go-try
      (let [resp (<! (storage/read endpoint address))]
        (if (util/exception? resp)
          (if (= (-> resp ex-data :error) :xhttp/timeout)
            false ; treat timeouts as non-existent
            (throw resp))
          (boolean resp)))))

  (read [_ address]
    (let [{:keys [ns local method]} (storage/parse-address address)
          path                      (build-ipfs-path method local)]
      (when-not (and (= "fluree" ns)
                     (#{"ipfs" "ipns"} method))
        (throw (ex-info (str "Invalid file type or method: " address)
                        {:status 500 :error :db/invalid-address})))
      (ipfs/cat endpoint path false)))

  (delete [_ address]
    (throw (ex-info "Unsupported operation IpfsStore method: delete." {:address address}))))

(defn open
  [endpoint]
  (->IpfsStore endpoint))
