(ns ^:iceberg fluree.db.virtual-graph.iceberg-test
  "Integration tests for Iceberg virtual graph with R2RML mappings.

   Requires :iceberg alias for dependencies.
   Run with: clojure -M:dev:iceberg:cljtest -e \"(require '[fluree.db.virtual-graph.iceberg-test]) (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test)\""
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.query.exec.where :as where]
            [fluree.db.virtual-graph.iceberg :as iceberg-vg])
  (:import [java.io File]))

;;; ---------------------------------------------------------------------------
;;; Test Fixtures
;;; ---------------------------------------------------------------------------

(def ^:private warehouse-path
  (str (System/getProperty "user.dir") "/dev-resources/openflights/warehouse"))

(def ^:private mapping-path
  (str (System/getProperty "user.dir") "/dev-resources/openflights/airlines-r2rml.ttl"))

(def ^:private vg (atom nil))

(defn- warehouse-exists? []
  (.exists (File. (str warehouse-path "/openflights/airlines"))))

(defn- mapping-exists? []
  (.exists (File. mapping-path)))

(defn vg-fixture [f]
  (if (and (warehouse-exists?) (mapping-exists?))
    (do
      (reset! vg (iceberg-vg/create {:alias "airlines"
                                     :config {:warehouse-path warehouse-path
                                              :mapping mapping-path}}))
      (try
        (f)
        (finally
          (reset! vg nil))))
    (println "SKIP: OpenFlights warehouse or mapping not found. Run 'make iceberg-openflights' first.")))

(use-fixtures :once vg-fixture)

;;; ---------------------------------------------------------------------------
;;; Helper Functions
;;; ---------------------------------------------------------------------------

(defn- collect-solutions
  "Collect all solutions from an async channel."
  [ch]
  (loop [results []]
    (if-let [sol (async/<!! ch)]
      (recur (conj results sol))
      results)))

(defn- make-triple
  "Create a triple pattern for testing."
  [s p o]
  [s p o])

(defn- var-map [v]
  {::where/var (symbol v)})

(defn- iri-map [iri]
  {::where/iri iri})

(defn- val-map [v]
  {::where/val v})

;;; ---------------------------------------------------------------------------
;;; Virtual Graph Creation Tests
;;; ---------------------------------------------------------------------------

(deftest create-vg-test
  (when @vg
    (testing "Virtual graph is created with correct alias"
      (is (= "airlines" (:alias @vg))))

    (testing "Virtual graph has mappings"
      (is (seq (:mappings @vg))))

    (testing "Virtual graph has Iceberg source"
      (is (some? (:source @vg))))))

(deftest r2rml-mapping-parsed-test
  (when @vg
    (testing "R2RML mapping has airline table"
      (let [mappings (:mappings @vg)
            mapping (first (vals mappings))]
        (is (= "openflights/airlines" (:table mapping)))))

    (testing "R2RML mapping has subject template"
      (let [mappings (:mappings @vg)
            mapping (first (vals mappings))]
        (is (string? (:subject-template mapping)))
        (is (re-find #"\{id\}" (:subject-template mapping)))))

    (testing "R2RML mapping has predicate mappings"
      (let [mappings (:mappings @vg)
            mapping (first (vals mappings))
            predicates (:predicates mapping)]
        (is (get predicates "http://example.org/airlines/name"))
        (is (get predicates "http://example.org/airlines/country"))))))

;;; ---------------------------------------------------------------------------
;;; Pattern Matching Tests (using where/Matcher protocol)
;;; ---------------------------------------------------------------------------

(deftest match-triple-accumulates-patterns-test
  (when @vg
    (testing "-match-triple accumulates patterns in solution"
      (let [solution {}
            triple (make-triple (var-map "?airline")
                                (iri-map "http://example.org/airlines/name")
                                (var-map "?name"))
            result-ch (where/-match-triple @vg nil solution triple nil)
            result (async/<!! result-ch)]
        (is (vector? (::iceberg-vg/iceberg-patterns result)))
        (is (= 1 (count (::iceberg-vg/iceberg-patterns result))))))))

(deftest match-class-accumulates-patterns-test
  (when @vg
    (testing "-match-class accumulates class patterns"
      (let [solution {}
            class-triple [:class
                          [(var-map "?airline")
                           (iri-map "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                           (iri-map "http://example.org/airlines/Airline")]]
            result-ch (where/-match-class @vg nil solution class-triple nil)
            result (async/<!! result-ch)]
        (is (vector? (::iceberg-vg/iceberg-patterns result)))
        (is (= 1 (count (::iceberg-vg/iceberg-patterns result))))))))

;;; ---------------------------------------------------------------------------
;;; End-to-End Query Tests
;;; ---------------------------------------------------------------------------

(deftest finalize-simple-query-test
  (when @vg
    (testing "Finalize executes query and returns solutions"
      (let [;; Build a solution with accumulated patterns
            patterns [(make-triple (var-map "?airline")
                                   (iri-map "http://example.org/airlines/name")
                                   (var-map "?name"))]
            solution {::iceberg-vg/iceberg-patterns patterns}
            solution-ch (async/to-chan! [solution])
            error-ch (async/chan 1)
            result-ch (where/-finalize @vg nil error-ch solution-ch)
            results (collect-solutions result-ch)]
        (is (pos? (count results)) "Should return some results")
        (is (every? #(contains? % (symbol "?name")) results)
            "Each result should have ?name binding")))))

(deftest finalize-with-filter-test
  (when @vg
    (testing "Finalize with literal filter pushes predicate to Iceberg"
      (let [;; Query: ?airline ex:country "United States"
            patterns [(make-triple (var-map "?airline")
                                   (iri-map "http://example.org/airlines/country")
                                   (val-map "United States"))
                      (make-triple (var-map "?airline")
                                   (iri-map "http://example.org/airlines/name")
                                   (var-map "?name"))]
            solution {::iceberg-vg/iceberg-patterns patterns}
            solution-ch (async/to-chan! [solution])
            error-ch (async/chan 1)
            result-ch (where/-finalize @vg nil error-ch solution-ch)
            results (collect-solutions result-ch)]
        ;; Should return fewer results than full scan (filtered)
        (is (pos? (count results)) "Should return some US airlines")
        (is (< (count results) 6162) "Should filter (not return all 6162 airlines)")))))

(deftest finalize-multiple-variables-test
  (when @vg
    (testing "Query with multiple variable bindings"
      (let [patterns [(make-triple (var-map "?airline")
                                   (iri-map "http://example.org/airlines/name")
                                   (var-map "?name"))
                      (make-triple (var-map "?airline")
                                   (iri-map "http://example.org/airlines/country")
                                   (var-map "?country"))]
            solution {::iceberg-vg/iceberg-patterns patterns}
            solution-ch (async/to-chan! [solution])
            error-ch (async/chan 1)
            result-ch (where/-finalize @vg nil error-ch solution-ch)
            results (take 10 (collect-solutions result-ch))]
        (is (pos? (count results)))
        (is (every? #(and (contains? % (symbol "?name"))
                          (contains? % (symbol "?country")))
                    results)
            "Each result should have both ?name and ?country")))))

;;; ---------------------------------------------------------------------------
;;; Alias Parsing Tests (Fluree naming convention)
;;; ---------------------------------------------------------------------------

(deftest create-with-branch-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "Virtual graph with explicit branch in alias"
      (let [vg (iceberg-vg/create {:alias "airlines:main"
                                   :config {:warehouse-path warehouse-path
                                            :mapping mapping-path}})]
        (is (= "airlines:main" (:alias vg)))
        (is (nil? (:time-travel vg)))))

    (testing "Virtual graph without branch defaults correctly"
      (let [vg (iceberg-vg/create {:alias "airlines"
                                   :config {:warehouse-path warehouse-path
                                            :mapping mapping-path}})]
        (is (= "airlines" (:alias vg)))
        (is (nil? (:time-travel vg)))))))

(deftest time-travel-rejected-at-registration-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "Time-travel in alias is rejected at registration"
      ;; Time-travel should be a query-time concern, not registration-time
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"cannot contain '@'"
           (iceberg-vg/create {:alias "airlines@t:12345"
                               :config {:warehouse-path warehouse-path
                                        :mapping mapping-path}}))))))

(deftest create-requires-store-or-warehouse-test
  (when (mapping-exists?)
    (testing "Create throws when neither store nor warehouse-path provided"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"requires :warehouse-path or :store"
           (iceberg-vg/create {:alias "test"
                               :config {:mapping mapping-path}}))))))

(deftest create-requires-mapping-test
  (when (warehouse-exists?)
    (testing "Create throws when mapping not provided"
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"requires :mapping or :mappingInline"
           (iceberg-vg/create {:alias "test"
                               :config {:warehouse-path warehouse-path}}))))))

;;; ---------------------------------------------------------------------------
;;; End-to-End Integration Tests (Full Fluree API)
;;; ---------------------------------------------------------------------------

(def ^:private e2e-system (atom nil))
(def ^:private e2e-conn (atom nil))
(def ^:private e2e-publisher (atom nil))

(defn- setup-fluree-system
  "Set up Fluree system for end-to-end testing."
  []
  (let [memory-config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                   "@vocab" "https://ns.flur.ee/system#"}
                       "@id"      "memory"
                       "@graph"   [{"@id"   "memoryStorage"
                                    "@type" "Storage"}
                                   {"@id"              "connection"
                                    "@type"            "Connection"
                                    "parallelism"      4
                                    "cacheMaxMb"       100
                                    "commitStorage"    {"@id" "memoryStorage"}
                                    "indexStorage"     {"@id" "memoryStorage"}
                                    "primaryPublisher" {"@type"   "Publisher"
                                                        "storage" {"@id" "memoryStorage"}}}]}
        sys (system/initialize (config/parse memory-config))]
    (reset! e2e-system sys)
    (reset! e2e-conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys))
    (reset! e2e-publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys))))

(defn- teardown-fluree-system []
  (when @e2e-system
    (reset! e2e-system nil)
    (reset! e2e-conn nil)
    (reset! e2e-publisher nil)))

(deftest e2e-register-and-query-iceberg-vg-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Register Iceberg VG and query via Fluree API"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Query using FQL with FROM clause
        (let [query {"from" ["iceberg/airlines"]
                     "select" ["?name" "?country"]
                     "where" {"@id" "?airline"
                              "http://example.org/airlines/name" "?name"
                              "http://example.org/airlines/country" "?country"}
                     "limit" 10}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results as vector")
          (is (= 10 (count res)) "Should return 10 results (limit)")
          (is (every? #(= 2 (count %)) res) "Each result should have 2 values"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-iceberg-literal-filter-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Query with literal filter pushdown"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-filter:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Query with literal filter - should push predicate to Iceberg
        (let [query {"from" ["iceberg/airlines-filter"]
                     "select" ["?name"]
                     "where" {"@id" "?airline"
                              "http://example.org/airlines/name" "?name"
                              "http://example.org/airlines/country" "United States"}}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have some US airlines")
          (is (< (count res) 6162) "Should filter (not return all airlines)"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-iceberg-sparql-query-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: SPARQL query against Iceberg VG"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-sparql:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Query using SPARQL with FROM clause
        (let [sparql "PREFIX ex: <http://example.org/airlines/>
                      SELECT ?name ?country
                      FROM <iceberg/airlines-sparql>
                      WHERE {
                        ?airline ex:name ?name .
                        ?airline ex:country ?country .
                      }
                      LIMIT 5"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results as vector")
          (is (= 5 (count res)) "Should return 5 results (limit)")
          (is (every? #(= 2 (count %)) res) "Each result should have name and country"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-iceberg-count-query-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Aggregate COUNT query"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-count:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Count all airlines
        (let [query {"from" ["iceberg/airlines-count"]
                     "select" ["(count ?airline)"]
                     "where" {"@id" "?airline"
                              "http://example.org/airlines/name" "?name"}}
              res @(fluree/query-connection @e2e-conn query)]
          (is (= [[6162]] res) "Should count all 6162 airlines"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test))
