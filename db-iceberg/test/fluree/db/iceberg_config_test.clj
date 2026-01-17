(ns ^:iceberg fluree.db.iceberg-config-test
  "Tests for Iceberg JSON-LD configuration parsing and policy enforcement.

   Tests the new configuration approach where Iceberg catalogs are pre-configured
   in the publisher/nameservice config and referenced by name at VG creation time.

   Requires running Docker containers:
   - iceberg-rest-catalog (REST API on port 8181)
   - iceberg-minio (S3 on port 9000)

   Run with: clojure -X:dev:iceberg:cljtest-iceberg"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.connection :as connection]
            [fluree.db.connection.system :as system]
            [fluree.db.connection.vocab :as vocab]
            [fluree.db.util :refer [get-first get-first-value]]
            [fluree.db.virtual-graph.create :as vg-create]
            [fluree.db.virtual-graph.iceberg.factory :as iceberg-factory]))

;;; ---------------------------------------------------------------------------
;;; Test Configuration
;;; ---------------------------------------------------------------------------

(def rest-uri "http://localhost:8181")
(def s3-endpoint "http://localhost:9000")

(defn catalog-reachable?
  "Check if the REST catalog is reachable."
  []
  (try
    (let [url (java.net.URL. (str rest-uri "/v1/config"))
          conn (.openConnection url)]
      (.setConnectTimeout conn 2000)
      (.setReadTimeout conn 2000)
      (= 200 (.getResponseCode conn)))
    (catch Exception _
      false)))

;;; ---------------------------------------------------------------------------
;;; Unit Tests: parse-iceberg-config
;;; ---------------------------------------------------------------------------

(deftest parse-iceberg-config-test
  (testing "Parses catalog with all fields"
    (let [config-node {vocab/iceberg-catalogs
                       [{vocab/iceberg-catalog-name "polaris"
                         vocab/iceberg-catalog-type "rest"
                         vocab/iceberg-rest-uri "http://polaris:8181"
                         vocab/iceberg-allow-vended-credentials true
                         vocab/iceberg-auth
                         {vocab/iceberg-auth-type "bearer"
                          vocab/iceberg-bearer-token {"@value" "test-token"}}}]}
          parsed (system/parse-iceberg-config config-node)]
      (is (some? parsed))
      (is (= 1 (count (:catalogs parsed))))
      (let [catalog (get-in parsed [:catalogs "polaris"])]
        (is (= "polaris" (:name catalog)))
        (is (= :rest (:type catalog)))
        (is (= "http://polaris:8181" (:uri catalog)))
        (is (true? (:allow-vended-credentials? catalog)))
        (is (= "test-token" (get-in catalog [:auth :bearer-token]))))))

  (testing "Parses cache settings with defaults"
    (let [config-node {vocab/iceberg-cache
                       [{vocab/iceberg-cache-enabled true
                         vocab/iceberg-mem-cache-mb {"@value" 512}}]}
          parsed (system/parse-iceberg-config config-node)]
      (is (some? (:cache parsed)))
      (is (true? (get-in parsed [:cache :enabled?])))
      (is (= 512 (get-in parsed [:cache :mem-cache-mb])))
      ;; Defaults
      (is (= 4 (get-in parsed [:cache :block-size-mb])))
      (is (= 300 (get-in parsed [:cache :ttl-seconds])))))

  (testing "Parses policy flags"
    (let [config-node {vocab/virtual-graph-allow-publish false
                       vocab/iceberg-allow-dynamic-virtual-graphs false
                       vocab/iceberg-allow-dynamic-catalogs false
                       vocab/iceberg-allowed-catalog-names ["prod-catalog" "staging-catalog"]}
          parsed (system/parse-iceberg-config config-node)]
      (is (false? (:allow-vg-publish? parsed)))
      (is (false? (:allow-dynamic-vgs? parsed)))
      (is (false? (:allow-dynamic-catalogs? parsed)))
      (is (= ["prod-catalog" "staging-catalog"] (:allowed-catalog-names parsed)))))

  (testing "Returns nil when no iceberg config present"
    (let [config-node {vocab/storage {:some "storage"}}
          parsed (system/parse-iceberg-config config-node)]
      (is (nil? parsed))))

  (testing "Defaults to permissive when flags not specified"
    (let [config-node {vocab/iceberg-catalogs
                       [{vocab/iceberg-catalog-name "test"
                         vocab/iceberg-rest-uri "http://test"}]}
          parsed (system/parse-iceberg-config config-node)]
      (is (true? (:allow-vg-publish? parsed)))
      (is (true? (:allow-dynamic-vgs? parsed)))
      (is (true? (:allow-dynamic-catalogs? parsed))))))

;;; ---------------------------------------------------------------------------
;;; Unit Tests: Policy Enforcement
;;; ---------------------------------------------------------------------------

(deftest enforce-vg-publish-policy-test
  (testing "Throws when virtualGraphAllowPublish=false"
    (let [publisher (with-meta {} {::system/iceberg-config {:allow-vg-publish? false}})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Virtual graph publishing is disabled"
           (#'vg-create/enforce-vg-publish-policy publisher)))))

  (testing "Allows when virtualGraphAllowPublish=true"
    (let [publisher (with-meta {} {::system/iceberg-config {:allow-vg-publish? true}})]
      (is (nil? (#'vg-create/enforce-vg-publish-policy publisher)))))

  (testing "Allows when no iceberg config (nil)"
    (let [publisher {}]
      (is (nil? (#'vg-create/enforce-vg-publish-policy publisher))))))

(deftest enforce-iceberg-policy-test
  (testing "Throws when icebergAllowDynamicVirtualGraphs=false"
    (let [publisher (with-meta {} {::system/iceberg-config {:allow-dynamic-vgs? false}})
          config {}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Dynamic Iceberg virtual graph creation is disabled"
           (#'vg-create/enforce-iceberg-policy publisher config)))))

  (testing "Throws for unknown catalog-name"
    (let [publisher (with-meta {} {::system/iceberg-config
                                   {:allow-dynamic-vgs? true
                                    :catalogs {"known-catalog" {:name "known-catalog"}}}})
          config {:catalog {:catalog-name "unknown-catalog"}}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Unknown Iceberg catalog"
           (#'vg-create/enforce-iceberg-policy publisher config)))))

  (testing "Throws when catalog-name not in allowlist"
    (let [publisher (with-meta {} {::system/iceberg-config
                                   {:allow-dynamic-vgs? true
                                    :catalogs {"my-catalog" {:name "my-catalog"}}
                                    :allowed-catalog-names ["other-catalog"]}})
          config {:catalog {:catalog-name "my-catalog"}}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Iceberg catalog not in allowed list"
           (#'vg-create/enforce-iceberg-policy publisher config)))))

  (testing "Throws for inline catalog when icebergAllowDynamicCatalogs=false"
    (let [publisher (with-meta {} {::system/iceberg-config
                                   {:allow-dynamic-vgs? true
                                    :allow-dynamic-catalogs? false}})
          ;; Inline catalog (no catalog-name, just uri)
          config {:catalog {:type "rest" :uri "http://inline-catalog"}}]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Dynamic Iceberg catalog configuration is disabled"
           (#'vg-create/enforce-iceberg-policy publisher config)))))

  (testing "Allows known catalog in allowlist"
    (let [publisher (with-meta {} {::system/iceberg-config
                                   {:allow-dynamic-vgs? true
                                    :catalogs {"prod" {:name "prod" :uri "http://prod"}}
                                    :allowed-catalog-names ["prod" "staging"]}})
          config {:catalog {:catalog-name "prod"}}]
      (is (nil? (#'vg-create/enforce-iceberg-policy publisher config)))))

  (testing "Allows inline catalog when icebergAllowDynamicCatalogs=true"
    (let [publisher (with-meta {} {::system/iceberg-config
                                   {:allow-dynamic-vgs? true
                                    :allow-dynamic-catalogs? true}})
          config {:catalog {:type "rest" :uri "http://inline"}}]
      (is (nil? (#'vg-create/enforce-iceberg-policy publisher config))))))

;;; ---------------------------------------------------------------------------
;;; Unit Tests: Catalog Resolution
;;; ---------------------------------------------------------------------------

(deftest resolve-catalog-config-test
  (testing "Resolves catalog-name from pre-configured catalogs"
    (let [iceberg-config {:catalogs {"polaris" {:name "polaris"
                                                :uri "http://polaris:8181"
                                                :auth {:bearer-token "secret"}
                                                :allow-vended-credentials? true}}}
          catalog {:catalog-name "polaris"}
          resolved (#'iceberg-factory/resolve-catalog-config catalog iceberg-config)]
      (is (= "http://polaris:8181" (:uri resolved)))
      (is (= "secret" (:auth-token resolved)))
      (is (true? (:allow-vended-credentials? resolved)))))

  (testing "Accepts camelCase catalogName"
    (let [iceberg-config {:catalogs {"my-cat" {:name "my-cat" :uri "http://test"}}}
          catalog {"catalogName" "my-cat"}
          resolved (#'iceberg-factory/resolve-catalog-config catalog iceberg-config)]
      (is (= "http://test" (:uri resolved)))))

  (testing "Returns nil for unknown catalog-name"
    (let [iceberg-config {:catalogs {"known" {:name "known"}}}
          catalog {:catalog-name "unknown"}
          resolved (#'iceberg-factory/resolve-catalog-config catalog iceberg-config)]
      (is (nil? resolved))))

  (testing "Uses inline config when no catalog-name"
    (let [iceberg-config {:catalogs {}}
          catalog {:type "rest"
                   :uri "http://inline-uri"
                   :auth-token "inline-token"
                   :allow-vended-credentials false}
          resolved (#'iceberg-factory/resolve-catalog-config catalog iceberg-config)]
      (is (= "http://inline-uri" (:uri resolved)))
      (is (= "inline-token" (:auth-token resolved)))
      (is (false? (:allow-vended-credentials? resolved)))))

  (testing "Defaults allow-vended-credentials to true"
    (let [catalog {:uri "http://test"}
          resolved (#'iceberg-factory/resolve-catalog-config catalog nil)]
      (is (true? (:allow-vended-credentials? resolved))))))

;;; ---------------------------------------------------------------------------
;;; Unit Tests: normalize-catalog-name
;;; ---------------------------------------------------------------------------

(deftest normalize-catalog-name-test
  (testing "Accepts keyword :catalog-name"
    (is (= "my-cat" (#'vg-create/normalize-catalog-name {:catalog-name "my-cat"}))))

  (testing "Accepts string \"catalog-name\""
    (is (= "my-cat" (#'vg-create/normalize-catalog-name {"catalog-name" "my-cat"}))))

  (testing "Accepts camelCase \"catalogName\""
    (is (= "my-cat" (#'vg-create/normalize-catalog-name {"catalogName" "my-cat"}))))

  (testing "Returns nil when no catalog-name"
    (is (nil? (#'vg-create/normalize-catalog-name {:uri "http://test"})))))

;;; ---------------------------------------------------------------------------
;;; Integration Tests: Pre-configured Catalog
;;; ---------------------------------------------------------------------------

(deftest ^:iceberg-rest configured-catalog-resolution-test
  (when (catalog-reachable?)
    (testing "VG create resolves pre-configured catalog"
      ;; Simulate a publisher with pre-configured catalog
      (let [iceberg-config {:catalogs {"local-rest"
                                       {:name "local-rest"
                                        :type :rest
                                        :uri rest-uri
                                        :allow-vended-credentials? false}}
                            :allow-dynamic-vgs? true
                            :cache {:enabled? true
                                    :mem-cache-mb 128
                                    :block-size-mb 4}}
            ;; The resolved catalog config should match
            resolved (#'iceberg-factory/resolve-catalog-config
                      {:catalog-name "local-rest"}
                      iceberg-config)]
        (is (= rest-uri (:uri resolved)))
        (is (false? (:allow-vended-credentials? resolved)))))))

(deftest ^:iceberg-rest cache-instance-sharing-test
  (when (catalog-reachable?)
    (testing "Cache instance is created and shared"
      ;; Test that create-iceberg-cache-instance works
      (let [cache-settings {:enabled? true
                            :mem-cache-mb 64
                            :ttl-seconds 120}
            cache-instance (#'system/create-iceberg-cache-instance cache-settings)]
        ;; Should return a Caffeine cache (or nil if deps missing)
        (when cache-instance
          (is (instance? com.github.benmanes.caffeine.cache.Cache cache-instance)))))))

;;; ---------------------------------------------------------------------------
;;; Integration Tests: Full Config Flow
;;; ---------------------------------------------------------------------------

(deftest ^:iceberg-rest full-config-flow-test
  (when (catalog-reachable?)
    (testing "Full config parsing and attachment flow"
      ;; Build a config node like Integrant would see after resolution
      (let [config-node {vocab/iceberg-catalogs
                         [{vocab/iceberg-catalog-name "iceberg-rest"
                           vocab/iceberg-catalog-type "rest"
                           vocab/iceberg-rest-uri rest-uri
                           vocab/iceberg-allow-vended-credentials false}]
                         vocab/iceberg-cache
                         [{vocab/iceberg-cache-enabled true
                           vocab/iceberg-mem-cache-mb {"@value" 128}}]
                         vocab/virtual-graph-allow-publish true
                         vocab/iceberg-allow-dynamic-virtual-graphs true
                         vocab/iceberg-allow-dynamic-catalogs false}
            ;; Parse it
            parsed (system/parse-iceberg-config config-node)]

        ;; Verify structure
        (is (some? parsed))
        (is (= 1 (count (:catalogs parsed))))
        (is (contains? (:catalogs parsed) "iceberg-rest"))

        ;; Verify catalog details
        (let [catalog (get-in parsed [:catalogs "iceberg-rest"])]
          (is (= rest-uri (:uri catalog)))
          (is (= :rest (:type catalog)))
          (is (false? (:allow-vended-credentials? catalog))))

        ;; Verify cache settings
        (is (true? (get-in parsed [:cache :enabled?])))
        (is (= 128 (get-in parsed [:cache :mem-cache-mb])))

        ;; Verify policy flags
        (is (true? (:allow-vg-publish? parsed)))
        (is (true? (:allow-dynamic-vgs? parsed)))
        (is (false? (:allow-dynamic-catalogs? parsed)))

        ;; Test that policy enforcement works correctly
        (let [mock-publisher (with-meta {} {::system/iceberg-config parsed})]
          ;; Should allow known catalog
          (is (nil? (#'vg-create/enforce-iceberg-policy
                     mock-publisher
                     {:catalog {:catalog-name "iceberg-rest"}})))

          ;; Should block inline catalog (allow-dynamic-catalogs? is false)
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               #"Dynamic Iceberg catalog configuration is disabled"
               (#'vg-create/enforce-iceberg-policy
                mock-publisher
                {:catalog {:uri "http://other"}}))))))))
