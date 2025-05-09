(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log])
  (:import (java.io ByteArrayOutputStream Closeable)))

(def method-name "s3")

(defn handle-s3-response
  [resp]
  (if (:cognitect.anomalies/category resp)
    (if (:cognitect.aws.client/throwable resp)
      resp
      (ex-info "S3 read failed"
               {:status 500, :error :db/unexpected-error, :aws/response resp}))
    (let [{in :Body} resp
          _        (log/debug "S3 response:" resp)
          body-str (when in
                     (with-open [out (ByteArrayOutputStream.)]
                       (io/copy in out)
                       (.close ^Closeable in)
                       (String. (.toByteArray out))))]
      (cond-> resp
        body-str (assoc :Body body-str)))))

(defn read-s3-data
  [s3-client s3-bucket s3-prefix path]
  (let [ch        (async/promise-chan (map handle-s3-response))
        full-path (str s3-prefix "/" path)
        req       {:op      :GetObject
                   :ch      ch
                   :request {:Bucket s3-bucket, :Key full-path}}]
    (aws/invoke-async s3-client req)
    ch))

(defn write-s3-data
  [s3-client s3-bucket s3-prefix path ^bytes data]
  (let [ch        (async/promise-chan (map handle-s3-response))
        full-path (str s3-prefix "/" path)
        req       {:op      :PutObject
                   :ch      ch
                   :request {:Bucket s3-bucket, :Key full-path, :Body data}}]
    (aws/invoke-async s3-client req)
    ch))

(defn s3-list*
  ([s3-client s3-bucket s3-prefix path]
   (s3-list* s3-client s3-bucket s3-prefix path nil))
  ([s3-client s3-bucket s3-prefix path continuation-token]
   (let [ch        (async/promise-chan (map handle-s3-response))
         base-req  {:op      :ListObjectsV2
                    :ch      ch
                    :request {:Bucket s3-bucket}}
         full-path (if (empty? s3-prefix)
                     path
                     (str s3-prefix "/" path))
         req       (cond-> base-req
                     (not= full-path "/") (assoc-in [:request :Prefix]
                                                    full-path)
                     continuation-token (assoc-in
                                         [:request :ContinuationToken]
                                         continuation-token))]
     (log/debug "s3-list* req:" req)
     (aws/invoke-async s3-client req)
     ch)))

(defn s3-list
  "Returns a core.async channel that will contain one or more result batches of
  1000 or fewer object names. You should continue to take from the channel until
  it closes (i.e. returns nil)."
  [s3-client s3-bucket s3-prefix path]
  (let [ch (async/chan 1)]
    (go-loop [results (<! (s3-list* s3-client s3-bucket s3-prefix path))]
      (>! ch results)
      (let [truncated?         (:IsTruncated results)
            continuation-token (:NextContinuationToken results)]
        (if truncated?
          (recur (<! (s3-list* s3-client s3-bucket s3-prefix path continuation-token)))
          (async/close! ch))))
    ch))

(defn s3-key-exists?
  [s3-client s3-bucket s3-prefix key]
  (go
    (let [list (<! (s3-list s3-client s3-bucket s3-prefix key))]
      (< 0 (:KeyCount list)))))

(defn s3-address
  [identifier s3-bucket s3-prefix path]
  (storage/build-fluree-address identifier method-name path [s3-bucket s3-prefix]))

(defrecord S3Store [identifier client bucket prefix]
  storage/Addressable
  (location [_]
    (storage/build-location storage/fluree-namespace identifier method-name [bucket prefix]))

  storage/Identifiable
  (identifiers [_]
    #{identifier})

  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go-try
      (when-let [data (<? (read-s3-data client bucket prefix address))]
        (json/parse data keywordize?))))

  storage/ContentAddressedStore
  (-content-write-bytes [_ dir data]
    (go
      (let [hash     (crypto/sha2-256 data :base32)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)
            filename (str hash ".json")
            path     (str/join "/" [dir filename])
            result   (<! (write-s3-data client bucket prefix path bytes))]
        (if (instance? Throwable result)
          result
          {:hash    hash
           :path    path
           :size    (count bytes)
           :address (s3-address identifier bucket prefix path)}))))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (write-s3-data client bucket prefix path bytes))

  (read-bytes [_ path]
    (read-s3-data client bucket prefix path)))

(defn open
  ([bucket prefix]
   (open nil bucket prefix))
  ([identifier bucket prefix]
   (open identifier bucket prefix nil))
  ([identifier bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                    endpoint-override (assoc :endpoint-override endpoint-override))
         client   (aws/client aws-opts)]
     (->S3Store identifier client bucket prefix))))
