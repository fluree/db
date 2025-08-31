(ns fluree.db.storage.s3
  (:refer-clojure :exclude [read list])
  (:require [alphabase.core :as alphabase]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [clojure.data.xml :as xml]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.storage :as storage]
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

(defn get-credentials
  "Get AWS credentials from environment variables or system properties.
   Returns a map with :access-key and :secret-key"
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
  [{:keys [method path headers payload region bucket credentials query-params]}]
  (let [{:keys [access-key secret-key session-token]} credentials
        now (Instant/now)
        amz-date (.format amz-date-formatter now)
        date-stamp (.format date-stamp-formatter now)
        payload-hash (if payload
                       (if (bytes? payload)
                         (crypto/sha2-256 payload :hex)
                         (sha256-hex payload))
                       (sha256-hex ""))

        ;; Add required headers  
        host-header (str bucket ".s3." region ".amazonaws.com")
        ;; Remove restricted headers that Java 11 HTTP client sets automatically
        headers-cleaned (dissoc headers "host" "Host" "content-length" "Content-Length")
        headers* (merge headers-cleaned
                        {"x-amz-date" amz-date
                         "x-amz-content-sha256" payload-hash}
                        (when session-token
                          {"x-amz-security-token" session-token}))
        ;; Include host header for signing but don't send it (Java 11 HTTP client sets it automatically)
        headers-for-signing (assoc headers* "host" host-header)

        ;; Create canonical request
        canonical-req (create-canonical-request
                       method
                       (canonical-uri path)
                       (canonical-query-string query-params)
                       headers-for-signing
                       payload-hash)

        ;; Create string to sign
        credential-scope (str date-stamp "/" region "/" aws-service "/" aws4-request)
        string-to-sign (create-string-to-sign
                        amz-date
                        credential-scope
                        (sha256-hex canonical-req))

        ;; Calculate signature
        signing-key (get-signature-key secret-key date-stamp region aws-service)
        signature (-> (hmac-sha256 signing-key string-to-sign)
                      (alphabase/base-to-base :bytes :hex))

        ;; Create authorization header
        authorization (str aws-signing-algorithm " "
                           "Credential=" access-key "/" credential-scope ", "
                           "SignedHeaders=" (signed-headers headers-for-signing) ", "
                           "Signature=" signature)]

    (assoc headers* "authorization" authorization)))

(defn build-s3-url
  "Build the S3 REST API URL"
  [bucket region path]
  (str "https://" bucket ".s3." region ".amazonaws.com/" path))

(declare with-retries parse-list-objects-response)

(defn s3-request
  "Make an S3 REST API request"
  [{:keys [method bucket region path headers body credentials query-params request-timeout]
    :or {method "GET"
         headers {}}}]
  (go-try
    (let [;; Encode path segments for both URL and signature to match S3's encoding
          encoded-path (encode-s3-path path)
          query-string (canonical-query-string query-params)
          url (str (build-s3-url bucket region encoded-path)
                   (when query-string (str "?" query-string)))
          headers-with-content-type (if (and (= method "PUT") body)
                                      (assoc headers "Content-Type" "application/octet-stream")
                                      headers)
          signed-headers (sign-request
                          {:method method
                           :path encoded-path  ;; Use encoded path for signature
                           :headers headers-with-content-type
                           :payload body
                           :region region
                           :bucket bucket
                           :credentials credentials
                           :query-params query-params})

          ;; Use xhttp for the actual request
          response (<? (case method
                         "GET" (xhttp/get url {:headers signed-headers
                                               :request-timeout request-timeout})
                         "PUT" (xhttp/put url body {:headers signed-headers
                                                    :request-timeout request-timeout})
                         "DELETE" (xhttp/delete url {:headers signed-headers
                                                     :request-timeout request-timeout})
                         (throw (ex-info "Unsupported HTTP method" {:method method}))))]

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

(defn read-s3-data
  "Read an object from S3"
  [client path]
  (let [{:keys [credentials bucket region prefix read-timeout-ms max-retries retry-base-delay-ms retry-max-delay-ms]} client
        ch (async/promise-chan)
        full-path (str prefix path)
        policy {:max-retries max-retries
                :retry-base-delay-ms retry-base-delay-ms
                :retry-max-delay-ms retry-max-delay-ms}
        thunk (fn []
                (s3-request {:method "GET"
                             :bucket bucket
                             :region region
                             :path full-path
                             :credentials credentials
                             :request-timeout read-timeout-ms}))]
    (go
      (try
        (let [response (<? (with-retries thunk (assoc policy :log-context {:method "GET" :bucket bucket :path full-path})))]
          (>! ch {:Body response}))
        (catch Exception e
          (if (and (ex-data e) (= 404 (:status (ex-data e))))
            (>! ch ::not-found)
            (>! ch e)))))
    ch))

(defn write-s3-data
  "Write an object to S3"
  [client path data]
  (let [{:keys [credentials bucket region prefix write-timeout-ms max-retries retry-base-delay-ms retry-max-delay-ms]} client
        ch (async/promise-chan)
        full-path (str prefix path)
        policy {:max-retries max-retries
                :retry-base-delay-ms retry-base-delay-ms
                :retry-max-delay-ms retry-max-delay-ms}
        thunk (fn []
                (s3-request {:method "PUT"
                             :bucket bucket
                             :region region
                             :path full-path
                             :body data
                             :credentials credentials
                             :request-timeout write-timeout-ms}))]
    (log/debug "S3 PUT starting"
               {:bucket bucket
                :region region
                :key full-path
                :bytes (if (string? data) (count (.getBytes ^String data "UTF-8")) (count ^bytes data))
                :timeout-ms write-timeout-ms})
    (async/pipe (with-retries thunk (assoc policy :log-context {:method "PUT" :bucket bucket :path full-path})) ch)))

(defn s3-list*
  "List objects in S3 with optional continuation token"
  ([client path]
   (s3-list* client path nil))
  ([client path continuation-token]
   (let [{:keys [credentials bucket region prefix list-timeout-ms max-retries retry-base-delay-ms retry-max-delay-ms]} client
         ch (async/promise-chan)
         full-path (str prefix path)]
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
                                                              :query-params query-params
                                                              :request-timeout list-timeout-ms}))
                              {:max-retries max-retries
                               :retry-base-delay-ms retry-base-delay-ms
                               :retry-max-delay-ms retry-max-delay-ms
                               :log-context {:method "LIST" :bucket bucket :path full-path}}))
               parsed (parse-list-objects-response response)]
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

(defrecord S3Store [identifier credentials bucket region prefix
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
          (some-> resp :Body (json/parse keywordize?))))))

  storage/ContentAddressedStore
  (-content-write-bytes [this dir data]
    (go
      (let [hash (crypto/sha2-256 data :base32)
            bytes (if (string? data)
                    (bytes/string->UTF8 data)
                    data)
            filename (str hash ".json")
            path (str/join "/" [dir filename])
            result (<! (write-s3-data this path bytes))]
        (if (instance? Throwable result)
          result
          {:hash hash
           :path path
           :size (count bytes)
           :address (s3-address identifier path)}))))

  storage/ByteStore
  (write-bytes [this path bytes]
    (write-s3-data this path bytes))

  (read-bytes [this path]
    (go-try
      (let [resp (<? (read-s3-data this path))]
        (when (not= resp ::not-found)
          (when-let [body (:Body resp)]
            (.getBytes ^String body))))))

  storage/EraseableStore
  (delete [_ address]
    (go-try
      (let [path (storage/get-local-path address)
            full-path (str prefix path)
            policy {:max-retries max-retries
                    :retry-base-delay-ms retry-base-delay-ms
                    :retry-max-delay-ms retry-max-delay-ms}]
        (<? (with-retries (fn [] (s3-request {:method "DELETE"
                                              :bucket bucket
                                              :region region
                                              :path full-path
                                              :credentials credentials
                                              :request-timeout write-timeout-ms}))
              policy)))))

  storage/RecursiveListableStore
  (list-paths-recursive [this path-prefix]
    (go-try
      ;; Use existing s3-list function to list objects with the prefix
      (let [results-ch (s3-list this path-prefix)
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
        (= status 429)
        (= status 500)
        (= status 502)
        (= status 503)
        (= status 504))))

(defn- with-retries
  "Runs thunk returning a channel; retries on retryable errors with backoff/jitter.
  policy may include :log-context with keys like {:method :bucket :path}"
  [thunk {:keys [max-retries retry-base-delay-ms retry-max-delay-ms log-context] :as _policy}]
  (let [out (async/promise-chan)]
    (go-loop [attempt 0]
      (let [start (System/nanoTime)
            res (<! (thunk))
            duration-ms (long (/ (- (System/nanoTime) start) 1000000))]
        (if (instance? Throwable res)
          (if (and (< attempt max-retries) (retryable-error? res))
            (let [delay (min (* retry-base-delay-ms (long (Math/pow 2 attempt))) retry-max-delay-ms)
                  wait-ms (jitter delay)
                  data (merge {:event "s3.retry"
                               :attempt attempt
                               :wait-ms wait-ms
                               :duration-ms duration-ms
                               :error (ex-message res)}
                              (ex-data res)
                              log-context)]
              (log/warn "S3 request failed, retrying" data)
              (<! (async/timeout wait-ms))
              (recur (inc attempt)))
            (let [data (merge {:event "s3.error"
                               :attempt attempt
                               :duration-ms duration-ms
                               :error (ex-message res)}
                              (ex-data res)
                              log-context)]
              (log/error "S3 request failed permanently" data)
              (>! out res)))
          (do
            (when (pos? attempt)
              (log/info "S3 request succeeded after retries"
                        (merge {:event "s3.success-after-retry"
                                :attempts (inc attempt)
                                :duration-ms duration-ms}
                               log-context)))
            (>! out res)))))
    out))

(defn open
  "Open an S3 store using direct HTTP implementation"
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
         credentials (get-credentials)
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
     (when-not credentials
       (throw (ex-info "AWS credentials not found"
                       {:error :s3/missing-credentials
                        :hint "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"})))
     ;; Note: endpoint-override can be handled via with-redefs of build-s3-url in tests
     (when endpoint-override
       (log/warn "endpoint-override provided - can be handled via with-redefs of build-s3-url in tests"))
     (->S3Store identifier credentials bucket region normalized-prefix
                read-timeout-ms* write-timeout-ms* list-timeout-ms*
                max-retries* retry-base-delay-ms* retry-max-delay-ms*))))