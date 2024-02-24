(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [cognitect.aws.client.api :as aws]
            [fluree.db.method.s3.core :as s3]
            [fluree.db.storage.proto :as store-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.bytes :as bytes]))

(defn s3-write
  [client bucket prefix k v {:keys [content-address?] :as opts}]
  (async/go
    (let [hash  (crypto/sha2-256 v)
          k*    (if content-address?
                  (str k hash)
                  k)
          bytes (if (string? v)
                  (bytes/string->UTF8 v)
                  v)
          result (async/<! (s3/write-s3-data client bucket prefix k* bytes))

          address (s3/s3-address bucket prefix k*)]
      (if (instance? Throwable result)
        result
        {:hash hash
         :k    (str/replace address #"fluree:s3://" "")
         :size (count bytes)
         :address address}))))

(defrecord S3Store [client bucket prefix]
  store-proto/Store
  (address [_ k] (s3/s3-address bucket prefix k))
  (write [_ k v opts] (s3-write client bucket prefix k v opts))
  (read [_ address] (s3/read-address client bucket prefix address))
  (exists? [_ address] (s3/s3-key-exists? client bucket prefix address))
  (list [_ prefix] (throw (ex-info "Unsupported operation S3Store method: list." {:prefix prefix})))
  (delete [_ address] (throw (ex-info "Unsupported operation S3Store method: delete." {:prefix prefix}))))

(defn open
  ([bucket prefix]
   (open bucket prefix nil))
  ([bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                    endpoint-override (assoc :endpoint-override endpoint-override))
         client   (aws/client aws-opts)]
     (->S3Store client bucket prefix))))
