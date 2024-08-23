(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [cognitect.aws.client.api :as aws]
            [fluree.db.method.s3.core :as s3]
            [fluree.db.storage :as storage]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.bytes :as bytes]))

(defn s3-address [bucket prefix k]
  (s3/s3-address bucket prefix k))

(defrecord S3Store [client bucket prefix]
  storage/ContentAddressedStore
  (write [_ dir data]
    (go
      (let [hash     (crypto/sha2-256 data)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)
            filename (str hash ".json")
            path     (str/join "/" [dir filename])
            result   (<! (s3/write-s3-data client bucket prefix path bytes))
            address  (s3/s3-address bucket prefix path)]
        (if (instance? Throwable result)
          result
          {:hash    hash
           :path    (str/replace address #"fluree:s3://" "")
           :size    (count bytes)
           :address address}))))

  (read [_ address]
    (s3/read-address client bucket prefix address))

  (exists? [_ address]
    (s3/s3-key-exists? client bucket prefix address))

  ;; TODO: Implement `list` and `delete` methods. We should never throw
  ;; exceptions for protocol implementations
  (list [_ prefix]
    (throw (ex-info "Unsupported operation S3Store method: list." {:prefix prefix})))

  (delete [_ address]
    (throw (ex-info "Unsupported operation S3Store method: delete." {:prefix prefix})))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (s3/write-s3-data client bucket prefix path bytes))

  (read-bytes [_ path]
    (s3/read-s3-data client bucket prefix path)))

(defn open
  ([bucket prefix]
   (open bucket prefix nil))
  ([bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                    endpoint-override (assoc :endpoint-override endpoint-override))
         client   (aws/client aws-opts)]
     (->S3Store client bucket prefix))))
