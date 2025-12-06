(ns fluree.db.storage.s3-express
  "S3 Express One Zone session management using AWS SDK v2.

   S3 Express One Zone requires session-based authentication that differs from
   standard S3. This namespace handles:
   - Creating directory bucket sessions via the AWS SDK
   - Caching session credentials with automatic refresh
   - Providing session credentials to the HTTP-based S3 implementation

   The hybrid approach uses the AWS SDK only for session management while
   continuing to use direct HTTP requests for actual data operations.

   NOTE: This namespace uses dynamic class loading for AWS SDK v2 classes.
   The classes are loaded on-demand when Express One Zone buckets are accessed.
   If AWS SDK v2 is not on the classpath, standard S3 buckets will work normally,
   but Express One Zone buckets will fail with a clear error message."
  (:require [clojure.core.cache.wrapped :as cache]
            [fluree.db.util.log :as log])
  (:import (java.time Duration Instant)))

;; AWS SDK v2 classes are loaded dynamically to avoid ClassNotFoundException
;; when this namespace is loaded but AWS SDK isn't on the classpath.
;; This allows the db library to be used as a git dependency without
;; requiring consumers to include AWS SDK unless they actually use Express One Zone.

(defn- load-class-dynamic
  "Load a class by name, constructing the class name dynamically to avoid
   GraalVM native image compile-time resolution. GraalVM detects Class/forName
   calls with constant strings and tries to resolve them at build time."
  [base-package class-suffix]
  (Class/forName (str base-package class-suffix)))

(defonce ^:private aws-sdk-classes
  (delay
    (try
      (log/debug "s3-express: Loading AWS SDK v2 classes dynamically")
      (let [auth-pkg "software.amazon.awssdk.auth."
            s3-pkg "software.amazon.awssdk.services."
            region-pkg "software.amazon.awssdk."]
        {:AwsBasicCredentials      (load-class-dynamic auth-pkg "credentials.AwsBasicCredentials")
         :StaticCredentialsProvider (load-class-dynamic auth-pkg "credentials.StaticCredentialsProvider")
         :Region                    (load-class-dynamic region-pkg "regions.Region")
         :S3Client                  (load-class-dynamic s3-pkg "s3.S3Client")
         :CreateSessionRequest      (load-class-dynamic s3-pkg "s3.model.CreateSessionRequest")
         :CreateSessionResponse     (load-class-dynamic s3-pkg "s3.model.CreateSessionResponse")
         :SessionCredentials        (load-class-dynamic s3-pkg "s3.model.SessionCredentials")})
      (catch ClassNotFoundException e
        (throw (ex-info (str "AWS SDK v2 is required for S3 Express One Zone but not found on classpath. "
                             "Add software.amazon.awssdk/s3 dependency to use Express One Zone buckets. "
                             "Missing class: " (.getMessage e))
                        {:error :s3-express/aws-sdk-not-found
                         :missing-class (.getMessage e)
                         :required-dependency "software.amazon.awssdk/s3"
                         :min-version "2.20.0"}
                        e))))))

(defn- get-aws-classes
  "Returns the AWS SDK classes, loading them if needed.
   Throws if AWS SDK v2 is not on the classpath."
  []
  @aws-sdk-classes)

(defn- invoke-static
  "Helper to invoke static methods on dynamically loaded classes."
  [^Class class method-name & args]
  (let [method (.getMethod class method-name (into-array Class (map class args)))]
    (.invoke method nil (into-array Object args))))

(defn- invoke-instance
  "Helper to invoke instance methods on objects."
  [obj method-name & args]
  (let [method (.getMethod (class obj) method-name (into-array Class (map class args)))]
    (.invoke method obj (into-array Object args))))

(defn express-one-bucket?
  "Returns true if the bucket name follows S3 Express One Zone naming convention.
   Express One Zone buckets end with '--x-s3' and contain an availability zone ID.
   Example: my-bucket--use1-az1--x-s3"
  [bucket]
  (boolean
   (and (string? bucket)
        (re-matches #".*--[a-z0-9]+-az\d+--x-s3$" bucket))))

(defn- build-s3-client
  "Builds an AWS SDK S3 client with the provided credentials and region.
   Uses reflection to avoid compile-time dependency on AWS SDK classes."
  [access-key secret-key region]
  (let [classes (get-aws-classes)
        ;; AwsBasicCredentials.create(accessKey, secretKey)
        credentials (invoke-static (:AwsBasicCredentials classes) "create" access-key secret-key)
        ;; StaticCredentialsProvider.create(credentials)
        credentials-provider (invoke-static (:StaticCredentialsProvider classes) "create" credentials)
        ;; Region.of(region)
        region-obj (invoke-static (:Region classes) "of" region)
        ;; S3Client.builder()
        builder (invoke-static (:S3Client classes) "builder")]
    ;; builder.region(regionObj).credentialsProvider(credentialsProvider).build()
    (-> builder
        (invoke-instance "region" region-obj)
        (invoke-instance "credentialsProvider" credentials-provider)
        (invoke-instance "build"))))

(defn- create-session-credentials
  "Calls the S3 CreateSession API to obtain session credentials for an Express One bucket.
   Returns a map with :access-key, :secret-key, :session-token, and :expiration.
   Uses reflection to avoid compile-time dependency on AWS SDK classes."
  [client bucket]
  (try
    (log/debug "s3-express: Creating session for bucket" {:bucket bucket})
    (let [classes (get-aws-classes)
          ;; CreateSessionRequest.builder()
          request-builder (invoke-static (:CreateSessionRequest classes) "builder")
          ;; builder.bucket(bucket).build()
          request (-> request-builder
                      (invoke-instance "bucket" bucket)
                      (invoke-instance "build"))
          ;; client.createSession(request)
          response (invoke-instance client "createSession" request)
          ;; response.credentials()
          credentials (invoke-instance response "credentials")
          ;; Extract credential values
          access-key (invoke-instance credentials "accessKeyId")
          secret-key (invoke-instance credentials "secretAccessKey")
          session-token (invoke-instance credentials "sessionToken")
          expiration (invoke-instance credentials "expiration")]
      (log/info "s3-express: Session created successfully"
                {:bucket bucket
                 :expiration (str expiration)})
      {:access-key access-key
       :secret-key secret-key
       :session-token session-token
       :expiration expiration})
    (catch Exception e
      (log/error e "s3-express: Failed to create session" {:bucket bucket})
      (throw (ex-info "Failed to create S3 Express One Zone session"
                      {:error :s3-express/session-creation-failed
                       :bucket bucket
                       :cause (ex-message e)}
                      e)))))

(defn- expired?
  "Returns true if the session credentials have expired or will expire within
   the buffer period (default 30 seconds before actual expiration)."
  [expiration-instant buffer-seconds]
  (let [now (Instant/now)
        buffer (Duration/ofSeconds buffer-seconds)
        expiration-with-buffer (.minus ^Instant expiration-instant ^Duration buffer)]
    (.isAfter ^Instant now ^Instant expiration-with-buffer)))

(defn- cache-key
  "Generates a cache key for session credentials based on bucket and base credentials."
  [bucket access-key]
  (str bucket ":" access-key))

(def ^:private session-cache
  "Cache for S3 Express One Zone session credentials.
   Keys are [bucket access-key], values are session credential maps with :expiration."
  (cache/ttl-cache-factory {} :ttl (* 5 60 1000))) ; 5 minutes TTL

(defn get-session-credentials
  "Gets or creates session credentials for an S3 Express One Zone bucket.

   Args:
   - bucket: The Express One Zone bucket name (must end with --x-s3)
   - region: AWS region code (e.g., 'us-east-1')
   - base-credentials: Map with :access-key and :secret-key (long-term credentials)
   - options: Optional map with:
     - :refresh-buffer-seconds - Seconds before expiration to refresh (default 30)
     - :force-refresh - If true, bypass cache and create new session

   Returns:
   A map with :access-key, :secret-key, and :session-token for use in request signing."
  [bucket region base-credentials & [{:keys [refresh-buffer-seconds force-refresh]
                                      :or {refresh-buffer-seconds 30
                                           force-refresh false}}]]
  (let [{:keys [access-key secret-key]} base-credentials
        key (cache-key bucket access-key)]

    ;; Check if we have valid cached credentials
    (if-let [cached (and (not force-refresh)
                         (cache/lookup session-cache key))]
      (if (expired? (:expiration cached) refresh-buffer-seconds)
        ;; Cached but expired, refresh
        (do
          (log/debug "s3-express: Cached session expired, refreshing" {:bucket bucket})
          (cache/evict session-cache key)
          (recur bucket region base-credentials {:refresh-buffer-seconds refresh-buffer-seconds
                                                 :force-refresh true}))
        ;; Cached and valid
        (do
          (log/trace "s3-express: Using cached session" {:bucket bucket})
          (select-keys cached [:access-key :secret-key :session-token])))

      ;; No valid cache, create new session
      (let [client (build-s3-client access-key secret-key region)
            session (try
                      (create-session-credentials client bucket)
                      (finally
                        (.close ^java.lang.AutoCloseable client)))]
        ;; Cache the session credentials
        (cache/miss session-cache key session)
        (select-keys session [:access-key :secret-key :session-token])))))

(defn clear-session-cache!
  "Clears all cached session credentials. Useful for testing or manual cache invalidation."
  []
  (log/debug "s3-express: Clearing session cache")
  (reset! session-cache (cache/ttl-cache-factory {} :ttl (* 5 60 1000))))

(defn get-credentials-for-bucket
  "Returns appropriate credentials for the given bucket.

   For Express One Zone buckets (ending in --x-s3), returns session credentials.
   For standard S3 buckets, returns the base credentials unchanged.

   Args:
   - bucket: The S3 bucket name
   - region: AWS region code
   - base-credentials: Map with :access-key and :secret-key

   Returns:
   A map with :access-key, :secret-key, and optionally :session-token"
  [bucket region base-credentials]
  (if (express-one-bucket? bucket)
    (do
      (log/trace "s3-express: Detected Express One Zone bucket, getting session" {:bucket bucket})
      (get-session-credentials bucket region base-credentials))
    (do
      (log/trace "s3-express: Standard S3 bucket, using base credentials" {:bucket bucket})
      base-credentials)))
