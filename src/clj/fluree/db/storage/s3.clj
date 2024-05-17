(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [cognitect.aws.client.api :as aws]
            [fluree.db.method.s3.core :as s3]
            [fluree.db.storage :as storage]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.bytes :as bytes]))

(defrecord S3Store [client bucket prefix]
  storage/Store
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

  (list [_ prefix]
    (throw (ex-info "Unsupported operation S3Store method: list." {:prefix prefix})))

  (delete [_ address]
    (throw (ex-info "Unsupported operation S3Store method: delete." {:prefix prefix}))))

(defn open
  ([bucket prefix]
   (open bucket prefix nil))
  ([bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                          endpoint-override (assoc :endpoint-override endpoint-override))
         client   (aws/client aws-opts)]
     (->S3Store client bucket prefix))))
