(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [fluree.db.util.log :as log]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [fluree.db.util.bytes :as bytes])
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

(defn address-path
  ([s3-bucket s3-prefix address] (address-path s3-bucket s3-prefix address true))
  ([s3-bucket s3-prefix address strip-prefix?]
   (log/debug "address-path address:" address)
   (let [path (-> address (str/split #"://") last)]
     (if strip-prefix?
       (-> path (str/replace-first (str s3-bucket "/" s3-prefix "/") ""))
       (str "//" path)))))

(defn read-s3-address
  [s3-client s3-bucket s3-prefix address]
  (->> address
       (address-path s3-bucket s3-prefix)
       (read-s3-data s3-client s3-bucket s3-prefix)))

(defn full-path
  [s3-bucket s3-prefix path]
  (let [path* (if (str/starts-with? path "//")
                (-> path (str/split #"//") last)
                path)]
    (str/join "/" [s3-bucket s3-prefix path*])))

(defn s3-address
  [s3-bucket s3-prefix path]
  (let [path* (full-path s3-bucket s3-prefix path)]
    (storage/build-fluree-address method-name path*)))

(defrecord S3Store [client bucket prefix]
  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go-try
      (when-let [data (<? (read-s3-address client bucket prefix address))]
        (json/parse data keywordize?))))

  storage/ContentAddressedStore
  (-content-write-bytes [_ dir data]
    (go
      (let [hash     (crypto/sha2-256 data)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)
            filename (str hash ".json")
            path     (str/join "/" [dir filename])
            result   (<! (write-s3-data client bucket prefix path bytes))
            address  (s3-address bucket prefix path)]
        (if (instance? Throwable result)
          result
          {:hash    hash
           :path    (str/replace address #"fluree:s3://" "")
           :size    (count bytes)
           :address address}))))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (write-s3-data client bucket prefix path bytes))

  (read-bytes [_ path]
    (read-s3-data client bucket prefix path)))

(defn open
  ([bucket prefix]
   (open bucket prefix nil))
  ([bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                    endpoint-override (assoc :endpoint-override endpoint-override))
         client   (aws/client aws-opts)]
     (->S3Store client bucket prefix))))
