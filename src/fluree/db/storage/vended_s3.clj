(ns fluree.db.storage.vended-s3
  "S3 storage backed by vended (temporary) credentials.

   This store uses a credential-provider function to obtain temporary S3
   credentials on demand. The provider handles caching and refresh logic,
   making this store agnostic about where credentials come from.

   Use cases:
   - Iceberg REST catalog vended credentials (Polaris, Snowflake)
   - AWS STS AssumeRole temporary credentials
   - Any system that provides temporary S3 access

   Features:
   - Implements StatStore, RangeReadableStore, and ByteStore protocols
   - Path-style S3 access support (for MinIO, etc.)
   - Read-only by design (data files are immutable)"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.storage.s3 :as s3]
            [fluree.db.util.async :refer [go-try]]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; S3 Operations with Credentials
;;; ---------------------------------------------------------------------------

(defn- build-endpoint
  "Build S3 endpoint URL, handling path-style if needed."
  [{:keys [endpoint path-style?]}]
  (when endpoint
    (if path-style?
      endpoint
      ;; Virtual-hosted style - bucket in hostname
      (let [base (if (re-find #"^https?://" endpoint)
                   endpoint
                   (str "https://" endpoint))]
        ;; For virtual-hosted, we don't modify the endpoint
        ;; The S3 client will prepend bucket to hostname
        base))))

(defn- s3-head-with-creds
  "Make S3 HEAD request using provided credentials."
  [bucket path credentials]
  (let [{:keys [access-key secret-key session-token region endpoint path-style?]} credentials
        creds {:access-key    access-key
               :secret-key    secret-key
               :session-token session-token}
        region (or region "us-east-1")]
    (s3/s3-head {:bucket      bucket
                 :region      region
                 :path        path
                 :credentials creds
                 :endpoint    (build-endpoint {:endpoint endpoint :path-style? path-style?})})))

(defn- s3-get-with-creds
  "Make S3 GET request using provided credentials."
  [bucket path credentials]
  (let [{:keys [access-key secret-key session-token region endpoint path-style?]} credentials
        creds {:access-key    access-key
               :secret-key    secret-key
               :session-token session-token}
        region (or region "us-east-1")]
    (go-try
      (let [result (async/<! (s3/s3-get-binary
                              {:bucket      bucket
                               :region      region
                               :path        path
                               :credentials creds
                               :endpoint    (build-endpoint {:endpoint endpoint :path-style? path-style?})}))]
        (if (instance? Throwable result)
          (if (= 404 (:status (ex-data result)))
            nil  ;; Not found returns nil
            (throw result))
          (:body result))))))

(defn- s3-get-range-with-creds
  "Make S3 range GET request using provided credentials."
  [bucket path offset length credentials]
  (let [{:keys [access-key secret-key session-token region endpoint path-style?]} credentials
        creds {:access-key    access-key
               :secret-key    secret-key
               :session-token session-token}
        region (or region "us-east-1")]
    (s3/s3-get-range {:bucket      bucket
                      :region      region
                      :path        path
                      :offset      offset
                      :length      length
                      :credentials creds
                      :endpoint    (build-endpoint {:endpoint endpoint :path-style? path-style?})})))

;;; ---------------------------------------------------------------------------
;;; Path Parsing
;;; ---------------------------------------------------------------------------

(defn- parse-s3-path
  "Parse an S3 URL into bucket and path components.

   Handles:
   - s3://bucket/path/to/file
   - s3a://bucket/path/to/file

   Returns {:bucket \"bucket\" :path \"path/to/file\"}"
  [^String url]
  (when (or (.startsWith url "s3://")
            (.startsWith url "s3a://"))
    (let [without-scheme (str/replace-first url #"^s3a?://" "")
          slash-idx (.indexOf without-scheme "/")]
      (when (pos? slash-idx)
        {:bucket (subs without-scheme 0 slash-idx)
         :path   (subs without-scheme (inc slash-idx))}))))

;;; ---------------------------------------------------------------------------
;;; VendedS3Store
;;; ---------------------------------------------------------------------------

(defrecord VendedS3Store [credential-provider default-context]
  storage/FullURIStore
  (expects-full-uri? [_] true)

  storage/ByteStore
  (write-bytes [_ _path _bytes]
    (throw (ex-info "VendedS3Store is read-only" {})))

  (read-bytes [_ path]
    (if-let [{:keys [bucket path]} (parse-s3-path path)]
      (let [creds (credential-provider default-context)]
        (s3-get-with-creds bucket path creds))
      (throw (ex-info "Invalid S3 path" {:path path}))))

  (swap-bytes [_ _path _f]
    (throw (ex-info "VendedS3Store is read-only" {})))

  storage/StatStore
  (stat [_ path]
    (if-let [{:keys [bucket path]} (parse-s3-path path)]
      (let [creds (credential-provider default-context)]
        (s3-head-with-creds bucket path creds))
      (throw (ex-info "Invalid S3 path" {:path path}))))

  storage/RangeReadableStore
  (read-bytes-range [_ path offset length]
    (if-let [{:keys [bucket path]} (parse-s3-path path)]
      (let [creds (credential-provider default-context)]
        (s3-get-range-with-creds bucket path offset length creds))
      (throw (ex-info "Invalid S3 path" {:path path})))))

;;; ---------------------------------------------------------------------------
;;; Factory Functions
;;; ---------------------------------------------------------------------------

(defn create-vended-s3-store
  "Create an S3 store that uses vended (temporary) credentials.

   Parameters:
   - credential-provider: A function (fn [context] -> credentials-map) that returns
                          credentials when called. The function should handle caching
                          and refresh internally. Returns a map with:
                          - :access-key     - AWS access key ID
                          - :secret-key     - AWS secret access key
                          - :session-token  - Session token (for STS credentials)
                          - :region         - AWS region (optional, defaults to us-east-1)
                          - :endpoint       - S3 endpoint URL (optional, for MinIO etc.)
                          - :path-style?    - Use path-style S3 access (optional)

   - default-context: Context value passed to credential-provider on each call.
                      Can be anything the provider needs (e.g., table name, namespace).

   Example with Iceberg REST catalog:
     (let [provider (make-iceberg-credential-provider rest-uri auth-token)]
       (create-vended-s3-store provider \"openflights.airlines\"))

   Example with static credentials (for testing):
     (create-vended-s3-store
       (constantly {:access-key \"key\" :secret-key \"secret\"})
       nil)"
  [credential-provider default-context]
  (->VendedS3Store credential-provider default-context))

;;; ---------------------------------------------------------------------------
;;; Credential Provider Helpers
;;; ---------------------------------------------------------------------------

(def ^:private refresh-buffer-ms
  "Refresh credentials this many ms before expiration."
  30000)

(defn- credentials-expired?
  "Check if credentials are expired or will expire soon."
  [{:keys [expiration-ms]}]
  (if expiration-ms
    (let [now-ms (System/currentTimeMillis)]
      (>= now-ms (- expiration-ms refresh-buffer-ms)))
    ;; No expiration info - assume not expired
    false))

(defn make-cached-credential-provider
  "Create a credential provider that caches credentials and refreshes when expired.

   Parameters:
   - fetch-fn: A function (fn [context] -> credentials-map) that fetches fresh
               credentials. Called when cache is empty or credentials expired.

   Returns a function suitable for use with create-vended-s3-store.

   The returned provider:
   - Caches credentials per context value
   - Automatically refreshes 30 seconds before expiration
   - Thread-safe via atom"
  [fetch-fn]
  (let [cache (atom {})]
    (fn [context]
      (let [cached (get @cache context)]
        (if (and cached (not (credentials-expired? cached)))
          cached
          ;; Need to refresh
          (let [fresh (fetch-fn context)]
            (if fresh
              (do
                (log/debug "VendedS3Store: Refreshed credentials for" context
                           {:expires-in-ms (when-let [exp (:expiration-ms fresh)]
                                             (- exp (System/currentTimeMillis)))})
                (swap! cache assoc context fresh)
                fresh)
              (throw (ex-info "Failed to get vended credentials"
                              {:context context})))))))))

(defn make-credential-provider-with-initial
  "Create a credential provider with pre-seeded credentials.

   Useful when you already have credentials from an initial API call
   and want to avoid an extra round-trip.

   Parameters:
   - fetch-fn: Function to fetch fresh credentials (same as make-cached-credential-provider)
   - initial-context: The context key for the initial credentials
   - initial-credentials: Pre-fetched credentials to seed the cache"
  [fetch-fn initial-context initial-credentials]
  (let [cache (atom {initial-context initial-credentials})]
    (fn [context]
      (let [cached (get @cache context)]
        (if (and cached (not (credentials-expired? cached)))
          cached
          (let [fresh (fetch-fn context)]
            (if fresh
              (do
                (swap! cache assoc context fresh)
                fresh)
              (throw (ex-info "Failed to get vended credentials"
                              {:context context})))))))))
