(ns fluree.db.nameservice.dynamodb
  "DynamoDB-based nameservice implementation.

   This nameservice uses DynamoDB for storing ledger metadata, providing:
   - Atomic conditional updates (no contention between transactor and indexer)
   - Strong consistency reads
   - High availability and durability

   Table Schema:
   - Partition key: ledger_alias (String) - e.g., 'my-ledger:main'
   - Attributes:
     - commit_address (String) - latest commit address
     - commit_t (Number) - commit t value
     - index_address (String) - latest index address
     - index_t (Number) - index t value
     - ledger_name (String) - ledger name without branch
     - branch (String) - branch name
     - status (String) - ledger status

   The key advantage is that publish-commit and publish-index update different
   attributes, so they never contend with each other even when running concurrently."
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.db.constants :as const]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.db.util.xhttp :as xhttp])
  (:import (java.time Instant ZoneOffset)
           (java.time.format DateTimeFormatter)
           (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)))

;; AWS Signature V4 constants
(def aws-signing-algorithm "AWS4-HMAC-SHA256")
(def aws-service "dynamodb")
(def aws4-request "aws4_request")

;; Date formatters for AWS
(def ^DateTimeFormatter amz-date-formatter
  (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'")
             ZoneOffset/UTC))

(def ^DateTimeFormatter date-stamp-formatter
  (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd")
             ZoneOffset/UTC))

(defn get-credentials
  "Get AWS credentials from environment variables or system properties."
  []
  (let [access-key (or (System/getenv "AWS_ACCESS_KEY_ID")
                       (System/getProperty "aws.accessKeyId"))
        secret-key (or (System/getenv "AWS_SECRET_ACCESS_KEY")
                       (System/getProperty "aws.secretKey"))
        session-token (or (System/getenv "AWS_SESSION_TOKEN")
                          (System/getProperty "aws.sessionToken"))]
    (when (and (not (str/blank? access-key))
               (not (str/blank? secret-key)))
      (cond-> {:access-key access-key
               :secret-key secret-key}
        ;; Only include session token if it's not blank
        (and session-token (not (str/blank? session-token)))
        (assoc :session-token session-token)))))

(defn hmac-sha256
  "Generate HMAC-SHA256 signature"
  [^bytes key ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")
        secret-key-spec (SecretKeySpec. key "HmacSHA256")]
    (.init mac secret-key-spec)
    (.doFinal mac (.getBytes data "UTF-8"))))

(defn sha256-hex
  "Generate SHA256 hash and return as lowercase hex string"
  [^String data]
  ;; Ensure lowercase - AWS SigV4 requires lowercase hex
  (str/lower-case (crypto/sha2-256 data :hex)))

(defn get-signature-key
  "Derive the signing key for AWS Signature V4"
  [secret-key date-stamp region]
  (-> (str "AWS4" secret-key)
      (.getBytes "UTF-8")
      (hmac-sha256 date-stamp)
      (hmac-sha256 region)
      (hmac-sha256 aws-service)
      (hmac-sha256 aws4-request)))

(defn bytes->hex
  "Convert byte array to lowercase hex string"
  [^bytes bytes]
  (let [hex-chars "0123456789abcdef"
        sb (StringBuilder.)]
    (doseq [b bytes]
      (.append sb (.charAt hex-chars (bit-and (bit-shift-right b 4) 0xF)))
      (.append sb (.charAt hex-chars (bit-and b 0xF))))
    (.toString sb)))

(defn create-canonical-request
  "Create canonical request for AWS Signature V4"
  [method uri query-string headers signed-headers payload-hash]
  (str method "\n"
       uri "\n"
       (or query-string "") "\n"
       headers "\n"
       signed-headers "\n"
       payload-hash))

(defn create-string-to-sign
  "Create string to sign for AWS Signature V4"
  [amz-date date-stamp region canonical-request-hash]
  (str aws-signing-algorithm "\n"
       amz-date "\n"
       date-stamp "/" region "/" aws-service "/" aws4-request "\n"
       canonical-request-hash))

(defn sign-request
  "Sign a DynamoDB request using AWS Signature V4"
  [{:keys [region endpoint]} operation payload]
  (let [credentials (get-credentials)
        _ (when-not credentials
            (throw (ex-info "AWS credentials not found" {:status 500 :error :db/missing-credentials})))
        {:keys [access-key secret-key session-token]} credentials
        now (Instant/now)
        amz-date (.format amz-date-formatter now)
        date-stamp (.format date-stamp-formatter now)
        host (-> endpoint
                 (str/replace #"^https?://" "")
                 (str/replace #"/$" ""))
        payload-str (json/stringify payload)
        payload-hash (sha256-hex payload-str)

        ;; Build headers - values must be trimmed per AWS spec
        base-headers {"content-type" "application/x-amz-json-1.0"
                      "host" host
                      "x-amz-date" amz-date
                      "x-amz-target" (str "DynamoDB_20120810." operation)}
        headers (if session-token
                  (assoc base-headers "x-amz-security-token" session-token)
                  base-headers)

        ;; Canonical headers must be sorted by lowercase header name
        ;; Header values must be trimmed and multiple spaces collapsed
        sorted-header-names (sort (keys headers))
        canonical-headers (str/join ""
                                    (map #(str % ":" (str/trim (get headers %)) "\n")
                                         sorted-header-names))
        signed-headers (str/join ";" sorted-header-names)

        ;; Create canonical request
        canonical-request (create-canonical-request "POST" "/" "" canonical-headers signed-headers payload-hash)
        canonical-request-hash (sha256-hex canonical-request)

        ;; Create string to sign
        string-to-sign (create-string-to-sign amz-date date-stamp region canonical-request-hash)

        ;; Calculate signature
        signing-key (get-signature-key secret-key date-stamp region)
        signature (bytes->hex (hmac-sha256 signing-key string-to-sign))

        ;; Build authorization header
        authorization (str aws-signing-algorithm " "
                           "Credential=" access-key "/" date-stamp "/" region "/" aws-service "/" aws4-request ", "
                           "SignedHeaders=" signed-headers ", "
                           "Signature=" signature)]

    {:url endpoint
     :headers (assoc headers "authorization" authorization)
     :body payload-str}))

(defn dynamodb-request
  "Make a signed request to DynamoDB"
  [config operation payload timeout-ms]
  (go-try
    (let [{:keys [url headers body]} (sign-request config operation payload)
          ;; Java's HttpClient doesn't allow setting restricted headers like "host"
          ;; It sets them automatically based on the URL
          headers* (dissoc headers "host")
          ;; Use xhttp/post with the pre-serialized JSON body
          response (<? (xhttp/post url body
                                   {:headers headers*
                                    :request-timeout timeout-ms}))]
      (if (:error response)
        (throw (ex-info "DynamoDB request failed"
                        {:status (:status response)
                         :error (:error response)
                         :operation operation}))
        ;; Parse the JSON response body with string keys (not keywords)
        ;; DynamoDB uses keys like "Item", "Items", "LastEvaluatedKey"
        (when-let [resp-body (:body response)]
          (json/parse resp-body false))))))

;; --- DynamoDB Operations ---

(defn get-item
  "Get an item from DynamoDB by ledger alias"
  [{:keys [table-name timeout-ms] :as config} ledger-alias]
  (go-try
    (let [payload {:TableName table-name
                   :Key {"ledger_alias" {"S" ledger-alias}}
                   :ConsistentRead true}
          response (<? (dynamodb-request config "GetItem" payload timeout-ms))
          item     (get response "Item")]
      (log/debug "DynamoDB get-item" {:ledger-alias ledger-alias
                                      :response-keys (keys response)
                                      :has-item? (some? item)})
      item)))

(defn- dynamo-value->clj
  "Convert a DynamoDB attribute value to Clojure"
  [v]
  (cond
    (contains? v "S") (get v "S")
    (contains? v "N") (parse-long (get v "N"))
    (contains? v "BOOL") (get v "BOOL")
    (contains? v "NULL") nil
    (contains? v "L") (mapv dynamo-value->clj (get v "L"))
    (contains? v "M") (into {} (map (fn [[k v]] [k (dynamo-value->clj v)]) (get v "M")))
    :else v))

(defn- item->ns-record
  "Convert a DynamoDB item to a nameservice record format"
  [item ledger-alias]
  (when item
    (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
          commit-address (dynamo-value->clj (get item "commit_address"))
          commit-t (dynamo-value->clj (get item "commit_t"))
          index-address (dynamo-value->clj (get item "index_address"))
          index-t (dynamo-value->clj (get item "index_t"))]
      (cond-> {"@context" {"f" "https://ns.flur.ee/ledger#"}
               "@id" ledger-alias
               "@type" ["f:Database" "f:PhysicalDatabase"]
               "f:ledger" {"@id" ledger-name}
               "f:branch" (or branch const/default-branch-name)
               "f:status" "ready"}
        commit-address (assoc "f:commit" {"@id" commit-address})
        commit-t (assoc "f:t" commit-t)
        index-address (assoc "f:index" (cond-> {"@id" index-address}
                                         index-t (assoc "f:t" index-t)))))))

(defn put-item
  "Put a full item to DynamoDB (used by legacy publish)"
  [{:keys [table-name timeout-ms] :as config} ledger-alias commit-address commit-t index-address index-t]
  (go-try
    (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
          branch (or branch const/default-branch-name)
          item (cond-> {"ledger_alias" {"S" ledger-alias}
                        "ledger_name" {"S" ledger-name}
                        "branch" {"S" branch}
                        "status" {"S" "ready"}}
                 commit-address (assoc "commit_address" {"S" commit-address})
                 commit-t (assoc "commit_t" {"N" (str commit-t)})
                 index-address (assoc "index_address" {"S" index-address})
                 index-t (assoc "index_t" {"N" (str index-t)}))
          payload {:TableName table-name
                   :Item item}]
      (<? (dynamodb-request config "PutItem" payload timeout-ms)))))

(defn update-commit
  "Atomically update commit info, only if new commit-t > existing.
   Uses DynamoDB conditional expressions for atomic updates."
  [{:keys [table-name timeout-ms] :as config} ledger-alias commit-address commit-t]
  (go-try
    (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
          branch (or branch const/default-branch-name)
          payload {:TableName table-name
                   :Key {"ledger_alias" {"S" ledger-alias}}
                   :UpdateExpression "SET commit_address = :addr, commit_t = :t, ledger_name = :ln, branch = :br, #st = :status"
                   :ConditionExpression "attribute_not_exists(commit_t) OR commit_t < :t"
                   :ExpressionAttributeNames {"#st" "status"} ;; status is reserved word
                   :ExpressionAttributeValues {":addr" {"S" commit-address}
                                               ":t" {"N" (str commit-t)}
                                               ":ln" {"S" ledger-name}
                                               ":br" {"S" branch}
                                               ":status" {"S" "ready"}}}]
      (try
        (<? (dynamodb-request config "UpdateItem" payload timeout-ms))
        (catch Exception e
          (let [error-type (get (ex-data e) :error)]
            ;; ConditionalCheckFailedException means our t was not greater - that's OK
            (if (= error-type "ConditionalCheckFailedException")
              (log/debug "update-commit condition not met (existing commit-t >= new)" {:ledger ledger-alias :commit-t commit-t})
              (throw e))))))))

(defn update-index
  "Atomically update index info, only if new index-t > existing.
   Uses DynamoDB conditional expressions for atomic updates."
  [{:keys [table-name timeout-ms] :as config} ledger-alias index-address index-t]
  (go-try
    (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
          branch (or branch const/default-branch-name)
          payload {:TableName table-name
                   :Key {"ledger_alias" {"S" ledger-alias}}
                   :UpdateExpression "SET index_address = :addr, index_t = :t, ledger_name = :ln, branch = :br, #st = :status"
                   :ConditionExpression "attribute_not_exists(index_t) OR index_t < :t"
                   :ExpressionAttributeNames {"#st" "status"}
                   :ExpressionAttributeValues {":addr" {"S" index-address}
                                               ":t" {"N" (str index-t)}
                                               ":ln" {"S" ledger-name}
                                               ":br" {"S" branch}
                                               ":status" {"S" "ready"}}}]
      (try
        (<? (dynamodb-request config "UpdateItem" payload timeout-ms))
        (catch Exception e
          (let [error-type (get (ex-data e) :error)]
            (if (= error-type "ConditionalCheckFailedException")
              (log/debug "update-index condition not met (existing index-t >= new)" {:ledger ledger-alias :index-t index-t})
              (throw e))))))))

(defn delete-item
  "Delete an item from DynamoDB"
  [{:keys [table-name timeout-ms] :as config} ledger-alias]
  (go-try
    (let [payload {:TableName table-name
                   :Key {"ledger_alias" {"S" ledger-alias}}}]
      (<? (dynamodb-request config "DeleteItem" payload timeout-ms)))))

(defn scan-all
  "Scan all items from the table. Use with caution on large tables."
  [{:keys [table-name timeout-ms] :as config}]
  (go-try
    (loop [items []
           exclusive-start-key nil]
      (let [payload (cond-> {:TableName table-name}
                      exclusive-start-key (assoc :ExclusiveStartKey exclusive-start-key))
            response (<? (dynamodb-request config "Scan" payload timeout-ms))
            new-items (get response "Items" [])
            all-items (into items new-items)
            last-key (get response "LastEvaluatedKey")]
        (if last-key
          (recur all-items last-key)
          all-items)))))

;; --- Nameservice Implementation ---

(defrecord DynamoDBNameService [config]
  nameservice/Publisher
  (publish [_ data]
    (let [ledger-alias (get data "alias")]
      (if (not ledger-alias)
        (do (log/warn "nameservice.dynamodb/publish missing alias in commit data; skipping" {:data-keys (keys data)})
            (go nil))
        (let [commit-address (get data "address")
              commit-t (get-in data ["data" "t"])
              index-address (get-in data ["index" "address"])
              index-t (get-in data ["index" "data" "t"])]
          (log/debug "nameservice.dynamodb/publish" {:ledger ledger-alias})
          (put-item config ledger-alias commit-address commit-t index-address index-t)))))

  (publish-commit [_ ledger-alias commit-address commit-t]
    (log/debug "nameservice.dynamodb/publish-commit" {:ledger ledger-alias :commit-t commit-t})
    (update-commit config ledger-alias commit-address commit-t))

  (publish-index [_ ledger-alias index-address index-t]
    (log/debug "nameservice.dynamodb/publish-index" {:ledger ledger-alias :index-t index-t})
    (update-index config ledger-alias index-address index-t))

  (retract [_ ledger-alias]
    (log/debug "nameservice.dynamodb/retract" {:ledger ledger-alias})
    (delete-item config ledger-alias))

  (publishing-address [_ ledger-alias]
    (go ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (log/debug "DynamoDBNameService lookup:" {:ledger-address ledger-address})
      (let [item (<? (get-item config ledger-address))
            record (when item (item->ns-record item ledger-address))]
        (log/debug "DynamoDBNameService lookup result:" {:ledger-address ledger-address
                                                          :has-item? (some? item)
                                                          :has-record? (some? record)
                                                          :commit-address (get record "f:commit")})
        record)))

  (alias [_ ledger-address]
    ;; For DynamoDB, the address is the alias
    ledger-address)

  (all-records [_]
    (go-try
      (let [items (<? (scan-all config))]
        (mapv (fn [item]
                (let [ledger-alias (dynamo-value->clj (get item "ledger_alias"))]
                  (item->ns-record item ledger-alias)))
              items)))))

(defn start
  "Create a DynamoDB nameservice.

   Config options:
   - :table-name - DynamoDB table name (required)
   - :region - AWS region (default: us-east-1)
   - :endpoint - Custom endpoint URL (optional, for local development)
   - :timeout-ms - Request timeout in milliseconds (default: 5000)"
  [{:keys [table-name region endpoint timeout-ms]
    :or {region "us-east-1"
         timeout-ms 5000}}]
  (when-not table-name
    (throw (ex-info "DynamoDB nameservice requires :table-name" {:status 400 :error :db/invalid-config})))
  (let [endpoint (or endpoint (str "https://dynamodb." region ".amazonaws.com"))
        config {:table-name table-name
                :region region
                :endpoint endpoint
                :timeout-ms timeout-ms}]
    (log/info "Starting DynamoDB nameservice" {:table table-name :region region :endpoint endpoint})
    (->DynamoDBNameService config)))
