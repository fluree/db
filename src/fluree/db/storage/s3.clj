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

(defn create-aws-client
  "Creates an AWS S3 client with the given options. 
   This function exists to facilitate testing with with-redefs."
  [aws-opts]
  (aws/client aws-opts))

(defn handle-s3-response
  [resp]
  (if (:cognitect.anomalies/category resp)
    (cond
      ;; Handle NoSuchKey as a special not-found response (file doesn't exist)
      ;; Return a keyword instead of nil to avoid core.async channel issues
      (= "NoSuchKey" (get-in resp [:Error :Code]))
      ::not-found

      ;; Return throwables as-is
      (:cognitect.aws.client/throwable resp)
      resp

      ;; Other errors become exceptions
      :else
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
      (let [path (storage/get-local-path address)
            resp (<? (read-s3-data client bucket prefix path))]
        (when (not= resp ::not-found)
          (some-> resp :Body (json/parse keywordize?))))))

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
    (go-try
      (let [resp (<? (read-s3-data client bucket prefix path))]
        (when (not= resp ::not-found)
          (when-let [body (:Body resp)]
            (.getBytes ^String body))))))

  storage/EraseableStore
  (delete [_ address]
    (go-try
      (let [path (storage/get-local-path address)
            ch (async/promise-chan (map handle-s3-response))
            req {:op :DeleteObject
                 :ch ch
                 :request {:Bucket bucket
                           :Key (str prefix "/" path)}}]
        (log/debug "Deleting S3 object:" {:bucket bucket :key (str prefix "/" path)})
        (aws/invoke-async client req)
        (<? ch)))))

(defn open
  ([bucket prefix]
   (open nil bucket prefix))
  ([identifier bucket prefix]
   (open identifier bucket prefix nil))
  ([identifier bucket prefix endpoint-override]
   (let [aws-opts (cond-> {:api :s3}
                    endpoint-override
                    (assoc :endpoint-override
                           (if (string? endpoint-override)
                             ;; Parse URL string like "http://localhost:4566"
                             (let [url (java.net.URL. endpoint-override)]
                               {:protocol (keyword (.getProtocol url))
                                :hostname (.getHost url)
                                :port (let [p (.getPort url)]
                                        (if (= -1 p)
                                          (if (= "https" (.getProtocol url)) 443 80)
                                          p))})
                             ;; Already a map
                             endpoint-override)))
         client   (create-aws-client aws-opts)]
     (->S3Store identifier client bucket prefix))))
