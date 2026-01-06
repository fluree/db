(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [alphabase.core :as alphabase]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [clojure.data.xml :as xml]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
            [fluree.db.storage.s3-express :as s3-express]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.util.xhttp :as xhttp])
  (:import (java.net URLEncoder)
           (java.time Instant ZoneOffset)
           (java.time.format DateTimeFormatter)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)))

(def method-name "s3")

;; AWS Signature V4 constants
(def aws-signing-algorithm "AWS4-HMAC-SHA256")
(def aws-service "s3")
(def aws4-request "aws4_request")

;; Date formatters for AWS
(def ^DateTimeFormatter amz-date-formatter
  (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'")
             ZoneOffset/UTC))

(def ^DateTimeFormatter date-stamp-formatter
  (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd")
             ZoneOffset/UTC))

(defn get-base-credentials
  "Get base AWS credentials from environment variables or system properties.
   Returns a map with :access-key and :secret-key.
   Note: For Express One Zone buckets, these will be exchanged for session credentials."
  []
  (let [access-key (or (System/getenv "AWS_ACCESS_KEY_ID")
                       (System/getProperty "aws.accessKeyId"))
        secret-key (or (System/getenv "AWS_SECRET_ACCESS_KEY")
                       (System/getProperty "aws.secretKey"))
        session-token (or (System/getenv "AWS_SESSION_TOKEN")
                          (System/getProperty "aws.sessionToken"))]
    (when (and access-key secret-key)
      (cond-> {:access-key access-key
               :secret-key secret-key}
        session-token (assoc :session-token session-token)))))

(defn get-credentials
  "Get appropriate credentials for the given bucket and region.
   For Express One Zone buckets, returns session credentials.
   For standard S3 buckets, returns base credentials."
  [bucket region base-credentials]
  (s3-express/get-credentials-for-bucket bucket region base-credentials))

(defn hmac-sha256
  "Generate HMAC-SHA256 signature"
  [^bytes key ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")
        secret-key-spec (SecretKeySpec. key "HmacSHA256")]
    (.init mac secret-key-spec)
    (.doFinal mac (.getBytes data "UTF-8"))))

(defn sha256-hex
  "Generate SHA256 hash and return as hex string"
  [^String data]
  (crypto/sha2-256 data :hex))

(defn get-signature-key
  "Derive the signing key for AWS Signature V4"
  [secret-key date-stamp region service-name]
  (-> (str "AWS4" secret-key)
      (.getBytes "UTF-8")
      (hmac-sha256 date-stamp)
      (hmac-sha256 region)
      (hmac-sha256 service-name)
      (hmac-sha256 aws4-request)))

(defn url-encode
  "URL encode a string for AWS"
  [^String s]
  (-> s
      (URLEncoder/encode "UTF-8")
      ;; AWS requires specific encoding
      (str/replace "+" "%20")
      (str/replace "*" "%2A")
      (str/replace "%7E" "~")))

(defn encode-s3-path
  "Encode S3 path segments individually to match S3's automatic encoding"
  [path]
  (let [segments (str/split path #"/")]
    (str/join "/" (map url-encode segments))))

(defn canonical-uri
  "Create canonical URI for AWS signature"
  [path]
  ;; Path must already be encoded when passed to this function
  (str "/" path))

(defn canonical-query-string
  "Create canonical query string for AWS signature"
  [params]
  (when (seq params)
    (->> params
         (map (fn [[k v]] [(url-encode (name k)) (url-encode (str v))]))
         (sort-by first)
         (map (fn [[k v]] (str k "=" v)))
         (str/join "&"))))

(defn canonical-headers
  "Create canonical headers string for AWS signature"
  [headers]
  (->> headers
       (map (fn [[k v]] [(str/lower-case k) (str/trim v)]))
       (sort-by first)
       (map (fn [[k v]] (str k ":" v "\n")))
       (apply str)))

(defn signed-headers
  "Create signed headers string for AWS signature"
  [headers]
  (->> headers
       (map (fn [[k _]] (str/lower-case k)))
       sort
       (str/join ";")))

(defn create-canonical-request
  "Create the canonical request for AWS Signature V4"
  [method uri query-params headers payload-hash]
  (str/join "\n" [method
                  uri
                  (or query-params "")
                  (canonical-headers headers)
                  (signed-headers headers)
                  payload-hash]))

(defn create-string-to-sign
  "Create the string to sign for AWS Signature V4"
  [amz-date credential-scope canonical-request-hash]
  (str/join "\n" [aws-signing-algorithm
                  amz-date
                  credential-scope
                  canonical-request-hash]))

(defn sign-request
  "Sign an S3 request using AWS Signature V4"
  [{:keys [method path headers payload region bucket credentials query-params endpoint]}]
  (let [{:keys [access-key secret-key session-token]} credentials
        now (Instant/now)
        amz-date (.format amz-date-formatter now)
        date-stamp (.format date-stamp-formatter now)
        payload-hash (if payload
                       (if (bytes? payload)
                         (crypto/sha2-256 payload :hex)
                         (sha256-hex payload))
                       (sha256-hex ""))

        ;; Determine URL style and adjust host/path for signature
        ;; S3 Express One Zone ALWAYS uses virtual-hosted style
        ;; Standard S3 with no endpoint uses virtual-hosted style
        ;; Path-style is only for custom endpoints (LocalStack, etc.)
        virtual-hosted? (or (s3-express/express-one-bucket? bucket)  ; S3 Express always virtual-hosted
                            (nil? endpoint)                          ; No endpoint = AWS default (virtual-hosted)
                            (str/includes? endpoint (str bucket ".")))
        host-header (if virtual-hosted?
                      ;; Virtual-hosted-style or no endpoint (use AWS default)
                      ;; S3 Express One Zone buckets use different host format
                      (if (s3-express/express-one-bucket? bucket)
                        ;; Extract AZ ID for S3 Express: bucket--use1-az4--x-s3 -> use1-az4
                        (let [az-id (second (re-matches #".*--([a-z0-9]+-az\d+)--x-s3$" bucket))]
                          (str bucket ".s3express-" az-id "." region ".amazonaws.com"))
                        ;; Standard S3
                        (str bucket ".s3." region ".amazonaws.com"))
                      ;; Path-style: extract host from endpoint
                      (-> endpoint
                          (str/replace #"^https?://" "")
                          (str/replace #"/.*$" "")))
        canonical-path (if virtual-hosted?
                         path  ; Virtual-hosted: path as-is
                         (str bucket "/" path))  ; Path-style: include bucket
        ;; Remove restricted headers that Java 11 HTTP client sets automatically
        headers-cleaned (dissoc headers "host" "Host" "content-length" "Content-Length")
        ;; S3 Express One Zone uses x-amz-s3session-token instead of x-amz-security-token
        session-token-header (if (s3-express/express-one-bucket? bucket)
                               "x-amz-s3session-token"
                               "x-amz-security-token")
        headers* (merge headers-cleaned
                        {"x-amz-date" amz-date
                         "x-amz-content-sha256" payload-hash}
                        (when session-token
                          {session-token-header session-token}))
        ;; Include host header for signing but don't send it (Java 11 HTTP client sets it automatically)
        headers-for-signing (assoc headers* "host" host-header)

        ;; Create canonical request
        canonical-req (create-canonical-request
                       method
                       (canonical-uri canonical-path)
                       (canonical-query-string query-params)
                       headers-for-signing
                       payload-hash)

        ;; Determine service name for signature - S3 Express uses "s3express"
        service-name (if (s3-express/express-one-bucket? bucket)
                       "s3express"
                       aws-service)
        ;; Create string to sign
        credential-scope (str date-stamp "/" region "/" service-name "/" aws4-request)
        string-to-sign (create-string-to-sign
                        amz-date
                        credential-scope
                        (sha256-hex canonical-req))

        ;; Calculate signature
        signing-key (get-signature-key secret-key date-stamp region service-name)
        signature (-> (hmac-sha256 signing-key string-to-sign)
                      (alphabase/base-to-base :bytes :hex))

        ;; Create authorization header
        authorization (str aws-signing-algorithm " "
                           "Credential=" access-key "/" credential-scope ", "
                           "SignedHeaders=" (signed-headers headers-for-signing) ", "
                           "Signature=" signature)]

    (assoc headers* "authorization" authorization)))

(defn build-s3-url
  "Build the S3 REST API URL. If endpoint is provided, uses that instead of
   the default AWS endpoint.

   Supports two endpoint styles:
   1. Virtual-hosted-style (full URL with bucket in hostname):
      'https://bucket.s3.region.amazonaws.com' or
      'https://bucket.s3express-azid.region.amazonaws.com'
      -> appends path only: {endpoint}/{path}

   2. Path-style (base URL, for LocalStack/S3-compatible services):
      'http://localhost:4566'
      -> includes bucket in path: {endpoint}/{bucket}/{path}"
  ([bucket region path]
   (build-s3-url bucket region path nil))
  ([bucket region path endpoint]
   (let [url (if endpoint
               ;; Check if bucket is already in the endpoint hostname (virtual-hosted-style)
               ;; If the endpoint contains the bucket name, it's virtual-hosted
               (if (str/includes? endpoint (str bucket "."))
                 ;; Virtual-hosted-style: bucket is in hostname, just append path
                 (str endpoint "/" path)
                 ;; Path-style: bucket not in hostname, include it in path
                 (str endpoint "/" bucket "/" path))
               ;; No endpoint - build URL based on bucket type
               ;; S3 Express One Zone buckets use format: bucket--azid--x-s3
               ;; and require endpoint: bucket.s3express-azid.region.amazonaws.com
               (if (s3-express/express-one-bucket? bucket)
                 ;; Extract AZ ID from bucket name: bucket--use1-az4--x-s3 -> use1-az4
                 (let [az-id (second (re-matches #".*--([a-z0-9]+-az\d+)--x-s3$" bucket))]
                   (str "https://" bucket ".s3express-" az-id "." region ".amazonaws.com/" path))
                 ;; Standard S3 bucket
                 (str "https://" bucket ".s3." region ".amazonaws.com/" path)))]
     url)))

(declare with-retries parse-list-objects-response)

;; HTTP client for binary requests (avoids xhttp which uses String body handlers)
(def ^:private ^java.net.http.HttpClient binary-http-client
  (-> (java.net.http.HttpClient/newBuilder)
      (.connectTimeout (java.time.Duration/ofSeconds 30))
      (.build)))

(defn s3-get-binary
  "Make an S3 GET request returning raw bytes.
   This bypasses xhttp to properly handle binary data like Parquet/Avro files."
  [{:keys [bucket region path credentials request-timeout endpoint headers]
    :or   {request-timeout 20000 headers {}}}]
  (go-try
    (let [start        (System/nanoTime)
          encoded-path (encode-s3-path path)
          query-string nil  ;; No query params for simple GET
          url          (str (build-s3-url bucket region encoded-path endpoint)
                            (when query-string (str "?" query-string)))
          signed-hdrs  (sign-request {:method       "GET"
                                      :path         encoded-path
                                      :headers      headers
                                      :payload      nil
                                      :region       region
                                      :bucket       bucket
                                      :credentials  credentials
                                      :query-params nil
                                      :endpoint     endpoint})
          ;; Build request with signed headers
          builder      (-> (java.net.http.HttpRequest/newBuilder)
                           (.uri (java.net.URI/create url))
                           (.timeout (java.time.Duration/ofMillis request-timeout))
                           (.GET))
          _            (doseq [[k v] signed-hdrs]
                         (.header builder k v))
          request      (.build builder)
          ;; Use byte array body handler for binary data
          response     (.send binary-http-client request
                              (java.net.http.HttpResponse$BodyHandlers/ofByteArray))
          status       (.statusCode response)
          ^bytes body  (.body response)]
      (log/trace "s3-get-binary done" {:bucket      bucket
                                       :path        encoded-path
                                       :status      status
                                       :size        (when body (alength body))
                                       :duration-ms (long (/ (- (System/nanoTime) start)
                                                             1000000))})
      (cond
        (= status 404)
        (throw (ex-info "Not found" {:status 404 :path path}))

        (< 299 status)
        (throw (ex-info (str "S3 error: " status)
                        {:status status :path path}))

        :else
        {:status  status
         :body    body
         :headers {}}))))

(defn s3-request
  "Make an S3 REST API request"
  [{:keys [method bucket region path headers body credentials query-params request-timeout endpoint]
    :or   {method  "GET"
           headers {}}}]
  (go-try
    (let [start                     (System/nanoTime)
          ;; Encode path segments for both URL and signature to match S3's encoding
          encoded-path              (encode-s3-path path)
          query-string              (canonical-query-string query-params)
          url                       (str (build-s3-url bucket region encoded-path endpoint)
                                         (when query-string (str "?" query-string)))
          headers-with-content-type (if (and (= method "PUT") body)
                                      (assoc headers "Content-Type" "application/octet-stream")
                                      headers)
          signed-headers            (sign-request
                                     {:method       method
                                      :path         encoded-path  ;; Use encoded path for signature
                                      :headers      headers-with-content-type
                                      :payload      body
                                      :region       region
                                      :bucket       bucket
                                      :credentials  credentials
                                      :query-params query-params
                                      :endpoint     endpoint})

          response (<? (case method
                         "GET"    (xhttp/get-response url {:headers         signed-headers
                                                           :request-timeout request-timeout})
                         "PUT"    (xhttp/put-response url body {:headers         signed-headers
                                                                :request-timeout request-timeout})
                         "DELETE" (xhttp/delete-response url {:headers         signed-headers
                                                              :request-timeout request-timeout})
                         (throw (ex-info "Unsupported HTTP method" {:method method}))))]
      (log/trace "s3-request done" {:method      method
                                    :bucket      bucket
                                    :path        encoded-path
                                    :duration-ms (long (/ (- (System/nanoTime) start)
                                                          1000000))})
      response)))

(defn- tag-matches?
  "Check if an XML element's tag matches the given name, ignoring namespace"
  [tag-name elem]
  (and (:tag elem)
       (= tag-name (name (:tag elem)))))

(defn- get-xml-text
  "Get text content of first matching XML element, ignoring namespace"
  [tag-name elements]
  (some (fn [element]
          (when (tag-matches? tag-name element)
            (first (:content element))))
        elements))

(defn parse-list-objects-response
  "Parse XML response from S3 ListObjectsV2"
  [xml-str]
  (let [parsed (xml/parse-str xml-str)
        contents (xml-seq parsed)]
    {:truncated? (= "true" (get-xml-text "IsTruncated" contents))
     :next-continuation-token (get-xml-text "NextContinuationToken" contents)
     :contents (for [x contents
                     :when (tag-matches? "Contents" x)]
                 (let [obj-content (:content x)]
                   {:key (get-xml-text "Key" obj-content)
                    :size (get-xml-text "Size" obj-content)
                    :last-modified (get-xml-text "LastModified" obj-content)}))}))

(defn not-found?
  [e]
  (-> e ex-data :status (= 404)))

(defn read-s3-data
  "Read an object from S3"
  ([client path]
   (read-s3-data client path {}))
  ([client path headers]
   (let [{:keys [base-credentials bucket region prefix endpoint read-timeout-ms max-retries
                 retry-base-delay-ms retry-max-delay-ms]}
         client

         ch           (async/promise-chan)
         full-path    (str prefix path)
         ;; Get appropriate credentials for this bucket (session-based for Express One)
         credentials  (get-credentials bucket region base-credentials)
         policy       {:max-retries         max-retries
                       :retry-base-delay-ms retry-base-delay-ms
                       :retry-max-delay-ms  retry-max-delay-ms}
         thunk        (fn []
                        (let [req (cond-> {:method          "GET"
                                           :bucket          bucket
                                           :region          region
                                           :path            path
                                           :credentials     credentials
                                           :endpoint        endpoint
                                           :request-timeout read-timeout-ms}
                                    (seq headers) (assoc :headers headers))]
                          (s3-request req)))]
     (go
       (try
         (let [response (<? (with-retries thunk (assoc policy :log-context {:method "GET" :bucket bucket :path full-path})))]
           (>! ch response))
         (catch Exception e
           (if (not-found? e)
             (>! ch ::not-found)
             (>! ch e)))))
     ch)))

(defn write-s3-data
  "Write an object to S3"
  ([client path data]
   (write-s3-data client path data {}))
  ([client path data headers]
   (let [{:keys [base-credentials bucket region prefix endpoint write-timeout-ms max-retries
                 retry-base-delay-ms retry-max-delay-ms]}
         client

         ch           (async/promise-chan)
         full-path    (str prefix path)
         ;; Get appropriate credentials for this bucket (session-based for Express One)
         credentials  (get-credentials bucket region base-credentials)
         policy       {:max-retries         max-retries
                       :retry-base-delay-ms retry-base-delay-ms
                       :retry-max-delay-ms  retry-max-delay-ms}
         thunk        (fn []
                        (let [req (cond-> {:method          "PUT"
                                           :bucket          bucket
                                           :region          region
                                           :path            path
                                           :body            data
                                           :credentials     credentials
                                           :endpoint        endpoint
                                           :request-timeout write-timeout-ms}
                                    (seq headers) (assoc :headers headers))]
                          (s3-request req)))]
     (go
       (let [res (<? (with-retries thunk (assoc policy :log-context {:method "PUT"
                                                                     :bucket bucket
                                                                     :path   full-path})))]
         (>! ch (:body res))))
     ch)))

(defn s3-list*
  "List objects in S3 with optional continuation token"
  ([client path]
   (s3-list* client path nil))
  ([client path continuation-token]
   (let [{:keys [base-credentials bucket region prefix endpoint list-timeout-ms max-retries retry-base-delay-ms retry-max-delay-ms]} client
         ch (async/promise-chan)
         full-path (str prefix path)
         ;; Get appropriate credentials for this bucket (session-based for Express One)
         credentials (get-credentials bucket region base-credentials)]
     (go
       (try
         (let [query-params (cond-> {"list-type" "2"}
                              (not= full-path "/") (assoc "prefix" full-path)
                              continuation-token (assoc "continuation-token" continuation-token))
               response (<? (with-retries (fn [] (s3-request {:method "GET"
                                                              :bucket bucket
                                                              :region region
                                                              :path ""
                                                              :credentials credentials
                                                              :endpoint endpoint
                                                              :query-params query-params
                                                              :request-timeout list-timeout-ms}))
                              {:max-retries max-retries
                               :retry-base-delay-ms retry-base-delay-ms
                               :retry-max-delay-ms retry-max-delay-ms
                               :log-context {:method "LIST" :bucket bucket :path full-path}}))
               parsed (parse-list-objects-response (:body response))]
           (>! ch (update parsed :contents
                          (fn [contents]
                            (mapv #(select-keys % [:key]) contents)))))
         (catch Exception e
           (>! ch e))))
     ch)))

(defn s3-list
  "List all objects in S3 path. Returns a channel that emits batches of results."
  [client path]
  (let [ch (async/chan 1)]
    (go-loop [continuation-token nil]
      (let [results (<! (s3-list* client path continuation-token))]
        (>! ch results)
        (if (and (:truncated? results)
                 (:next-continuation-token results))
          (recur (:next-continuation-token results))
          (async/close! ch))))
    ch))

(defn s3-address
  [identifier path]
  (storage/build-fluree-address identifier method-name path))

(defrecord S3Store [identifier base-credentials bucket region prefix endpoint
                   ;; timeouts (ms)
                    read-timeout-ms
                    write-timeout-ms
                    list-timeout-ms
                   ;; retry policy
                    max-retries
                    retry-base-delay-ms
                    retry-max-delay-ms]
  storage/Addressable
  (location [_]
    (storage/build-location storage/fluree-namespace identifier method-name))

  storage/Identifiable
  (identifiers [_]
    #{identifier})

  storage/JsonArchive
  (-read-json [this address keywordize?]
    (go-try
      (let [path (storage/get-local-path address)
            resp (<? (read-s3-data this path))]
        (when (not= resp ::not-found)
          (some-> (:body resp) (json/parse keywordize?))))))

  storage/ContentAddressedStore
  (-content-write-bytes [this dir data]
    (go
      (let [hash     (crypto/sha2-256 data :base32)
            bytes    (if (string? data)
                       (bytes/string->UTF8 data)
                       data)
            filename (str hash ".json")
            path     (str/join "/" [dir filename])
            result   (<! (write-s3-data this path bytes))]
        (if (instance? Throwable result)
          result
          {:hash    hash
           :path    path
           :size    (count bytes)
           :address (s3-address identifier path)}))))

  storage/ContentArchive
  (-content-read-bytes [this address]
    (go-try
      (let [path (storage/get-local-path address)
            resp (<? (read-s3-data this path))]
        (when (not= resp ::not-found)
          (:body resp)))))

  (get-hash [_ address]
    (go
      (-> address
          storage/split-address
          last
          (str/split #"/")
          last
          storage/strip-extension)))

  storage/ByteStore
  (write-bytes [this path bytes]
    (write-s3-data this path bytes))

  (read-bytes [this path]
    ;; Use s3-get-binary for proper binary data handling (Parquet, Avro, etc.)
    (let [{:keys [base-credentials bucket region prefix endpoint read-timeout-ms]} this
          credentials (get-credentials bucket region base-credentials)
          full-path   (str prefix path)]
      (go-try
        (try
          (let [resp (<? (s3-get-binary {:bucket          bucket
                                         :region          region
                                         :path            full-path
                                         :credentials     credentials
                                         :endpoint        endpoint
                                         :request-timeout read-timeout-ms}))]
            (:body resp))
          (catch Exception e
            ;; Return nil for not-found to match expected behavior
            (if (= 404 (:status (ex-data e)))
              nil
              (throw e)))))))

  (swap-bytes [this path f]
    (go-try
      (let [swap-thunk (fn []
                         (go-try
                           (try
                             (let [read-resp     (<? (read-s3-data this path))
                                   current-bytes (when-not (= read-resp ::not-found)
                                                   (.getBytes ^String (:body read-resp)))
                                   ;; etag header value is a vector from xhttp, extract first element
                                   etag          (when-not (= read-resp ::not-found)
                                                   (first (get-in read-resp [:headers "etag"])))
                                   new-bytes     (f current-bytes)]
                               (when new-bytes
                                 (let [headers (if etag
                                                 {"If-Match" etag}
                                                 {"If-None-Match" "*"})]
                                   (<? (write-s3-data this path new-bytes headers)))))
                             (catch Exception e
                               (if (not-found? e)
                                 (let [new-bytes (f nil)]
                                   (when new-bytes
                                     (<? (write-s3-data this path new-bytes {"If-None-Match" "*"}))))
                                 ;; Other error, re-throw
                                 (throw e))))))]
        ;; Swap operations use 3x retries because 412 conflicts under high contention
        ;; may require many attempts before winning the race
        (<? (with-retries swap-thunk {:max-retries         (* 3 max-retries)
                                      :retry-base-delay-ms retry-base-delay-ms
                                      :retry-max-delay-ms  retry-max-delay-ms
                                      :log-context         {:method "SWAP"
                                                            :bucket bucket
                                                            :path   path}})))))

  storage/EraseableStore
  (delete [_ address]
    (go-try
      (let [path        (storage/get-local-path address)
            full-path   (str prefix path)
            ;; Get appropriate credentials for this bucket (session-based for Express One)
            credentials (get-credentials bucket region base-credentials)
            policy      {:max-retries         max-retries
                         :retry-base-delay-ms retry-base-delay-ms
                         :retry-max-delay-ms  retry-max-delay-ms}
            response    (<? (with-retries (fn [] (s3-request {:method          "DELETE"
                                                              :bucket          bucket
                                                              :region          region
                                                              :path            full-path
                                                              :credentials     credentials
                                                              :endpoint        endpoint
                                                              :request-timeout write-timeout-ms}))
                              policy))]
        (:body response))))

  storage/RecursiveListableStore
  (list-paths-recursive [this path-prefix]
    (go-try
      ;; Use existing s3-list function to list objects with the prefix
      (let [results-ch  (s3-list this path-prefix)
            all-results (loop [acc []]
                          (let [batch (<! results-ch)]
                            (if batch
                              (let [contents (:contents batch)]
                                (recur (into acc (map :key contents))))
                              acc)))]
        ;; Filter for .json files and return relative paths
        (->> all-results
             (filter #(str/ends-with? % ".json"))
             vec)))))

(defn- jitter
  "Adds +/- 50% jitter to a delay in ms."
  [ms]
  (let [delta (max 1 (long (* 0.5 ms)))
        low (- ms delta)
        high (+ ms delta)]
    (+ low (rand-int (inc (- high low))))))

(defn- retryable-error?
  [e]
  (let [data (ex-data e)
        status (:status data)
        err (:error data)]
    (or (= err :xhttp/timeout)
        (nil? status)
        (= status 412) ; Precondition Failed - retry for conditional writes (ETag mismatch)
        (= status 429)
        (= status 500)
        (= status 502)
        (= status 503)
        (= status 504))))

(defn- retry-reason
  "Returns a human-readable reason for why a request is being retried."
  [e]
  (let [data (ex-data e)
        status (:status data)
        err (:error data)]
    (cond
      (= err :xhttp/timeout) "request timeout"
      (= status 412)         "conditional write conflict (ETag mismatch)"
      (= status 429)         "rate limited"
      (= status 500)         "internal server error"
      (= status 502)         "bad gateway"
      (= status 503)         "service unavailable"
      (= status 504)         "gateway timeout"
      (nil? status)          "unknown error (no status)"
      :else                  (str "HTTP " status))))

(defn- etag-conflict?
  "Returns true if the error is a 412 Precondition Failed (ETag mismatch)."
  [e]
  (= 412 (:status (ex-data e))))

(defn- with-retries
  "Runs thunk returning a channel; retries on retryable errors with backoff/jitter.
  policy may include :log-context with keys like {:method :bucket :path}"
  [thunk {:keys [max-retries retry-base-delay-ms retry-max-delay-ms log-context] :as _policy}]
  (let [out (async/chan 1)]
    (go-loop [attempt 0]
      (let [start (System/nanoTime)
            res (<! (thunk))
            duration-ms (long (/ (- (System/nanoTime) start) 1000000))]
        (if (instance? Throwable res)
          (if (and (< attempt max-retries) (retryable-error? res))
            (let [;; For 412 ETag conflicts: use fixed 200ms delay with jitter (not exponential backoff)
                  ;; since we just need to spread out concurrent writers, not wait for server recovery.
                  ;; 200ms accounts for S3's ~100ms latency plus buffer for the read-modify-write cycle.
                  delay (if (etag-conflict? res)
                          200
                          (min (* retry-base-delay-ms (long (Math/pow 2 attempt))) retry-max-delay-ms))
                  wait-ms (jitter delay)
                  reason (retry-reason res)
                  data (merge {:event       "s3.retry"
                               :reason      reason
                               :attempt     (inc attempt)
                               :max-retries max-retries
                               :wait-ms     wait-ms
                               :duration-ms duration-ms
                               :error       (ex-message res)}
                              (ex-data res)
                              log-context)]
              (log/info "S3 request retrying" data)
              (<! (async/timeout wait-ms))
              (recur (inc attempt)))
            (let [data (merge {:event "s3.error"
                               :attempt attempt
                               :duration-ms duration-ms
                               :error (ex-message res)}
                              (ex-data res)
                              log-context)]
              (log/error "S3 request failed permanently" data)
              (>! out res)
              (async/close! out)))
          (do
            (when (pos? attempt)
              (log/info "S3 request succeeded after retries"
                        (merge {:event "s3.success-after-retry"
                                :attempts (inc attempt)
                                :duration-ms duration-ms}
                               log-context)))
            (>! out res)
            (async/close! out)))))
    out))

(defn open
  "Open an S3 store using direct HTTP implementation.
   Supports both standard S3 and S3 Express One Zone buckets.
   For Express One Zone buckets (ending in --x-s3), session credentials
   will be automatically managed."
  ([bucket prefix]
   (open nil bucket prefix))
  ([identifier bucket prefix]
   (open identifier bucket prefix nil nil))
  ([identifier bucket prefix endpoint-override]
   (open identifier bucket prefix endpoint-override nil))
  ([identifier bucket prefix endpoint-override {:keys [read-timeout-ms write-timeout-ms list-timeout-ms
                                                       max-retries retry-base-delay-ms retry-max-delay-ms]}]
   (let [region (or (System/getenv "AWS_REGION")
                    (System/getenv "AWS_DEFAULT_REGION")
                    "us-east-1")
         base-credentials (get-base-credentials)
         ;; Normalize prefix to always end with / (unless empty)
         normalized-prefix (when (and prefix (not (str/blank? prefix)))
                             (if (str/ends-with? prefix "/")
                               prefix
                               (str prefix "/")))
         read-timeout-ms* (or read-timeout-ms 20000)
         write-timeout-ms* (or write-timeout-ms 60000)
         list-timeout-ms* (or list-timeout-ms 20000)
         max-retries* (or max-retries 4)
         retry-base-delay-ms* (or retry-base-delay-ms 150)
         retry-max-delay-ms* (or retry-max-delay-ms 2000)]
     (when-not base-credentials
       (throw (ex-info "AWS credentials not found"
                       {:error :s3/missing-credentials
                        :hint "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"})))
     ;; Log if this is an Express One Zone bucket
     (when (s3-express/express-one-bucket? bucket)
       (log/info "Opening S3 Express One Zone bucket - session credentials will be managed automatically"
                 {:bucket bucket :region region}))
     (->S3Store identifier base-credentials bucket region normalized-prefix endpoint-override
                read-timeout-ms* write-timeout-ms* list-timeout-ms*
                max-retries* retry-base-delay-ms* retry-max-delay-ms*))))
