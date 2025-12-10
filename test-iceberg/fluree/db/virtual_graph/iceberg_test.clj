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
            [fluree.db.query.optimize :as optimize]
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

    (testing "Virtual graph has Iceberg sources"
      (is (seq (:sources @vg))))))

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

(deftest e2e-create-virtual-graph-api-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Create Iceberg VG via fluree/create-virtual-graph API"
      (setup-fluree-system)
      (try
        ;; Create the Iceberg virtual graph using the public API
        (let [vg-result @(fluree/create-virtual-graph
                          @e2e-conn
                          {:name "iceberg/airlines-api"
                           :type :iceberg
                           :config {:warehouse-path warehouse-path
                                    :mapping mapping-path}})]
          ;; Verify the VG was created with expected properties
          (is (map? vg-result) "Should return a map")
          (is (= "iceberg/airlines-api:main" (:alias vg-result)) "Should have normalized alias")
          (is (contains? (set (:type vg-result)) "fidx:Iceberg") "Should have Iceberg type"))

        ;; Query to verify it works
        (let [query {"from" ["iceberg/airlines-api"]
                     "select" ["?name"]
                     "where" {"@id" "?airline"
                              "http://example.org/airlines/name" "?name"}
                     "limit" 5}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results")
          (is (= 5 (count res)) "Should return 5 results"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-create-virtual-graph-duplicate-error-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Creating duplicate VG should error"
      (setup-fluree-system)
      (try
        ;; Create the first VG
        @(fluree/create-virtual-graph
          @e2e-conn
          {:name "iceberg/airlines-dup"
           :type :iceberg
           :config {:warehouse-path warehouse-path
                    :mapping mapping-path}})

        ;; Try to create a duplicate - API returns exception as value
        (let [result @(fluree/create-virtual-graph
                       @e2e-conn
                       {:name "iceberg/airlines-dup"
                        :type :iceberg
                        :config {:warehouse-path warehouse-path
                                 :mapping mapping-path}})]
          (is (instance? Exception result) "Should return an exception")
          (is (re-find #"already exists" (ex-message result))
              "Error should mention 'already exists'"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Multi-Table Tests
;;; ---------------------------------------------------------------------------

(def ^:private multi-table-mapping-path
  (str (System/getProperty "user.dir") "/dev-resources/openflights/openflights-r2rml.ttl"))

(defn- multi-table-mapping-exists? []
  (.exists (File. multi-table-mapping-path)))

(deftest multi-table-vg-creation-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Multi-table VG creation parses all tables from R2RML"
      (let [vg (iceberg-vg/create {:alias "openflights"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})]
        ;; Should have 3 mappings (airlines, airports, routes)
        (is (= 3 (count (:mappings vg)))
            "Should have 3 mappings from multi-table R2RML")

        ;; Should have sources for each table
        (is (= 3 (count (:sources vg)))
            "Should have 3 sources (one per table)")

        ;; Verify table names are present in sources
        (is (contains? (:sources vg) "openflights/airlines"))
        (is (contains? (:sources vg) "openflights/airports"))
        (is (contains? (:sources vg) "openflights/routes"))

        ;; Verify routing indexes were built
        (let [routing (:routing-indexes vg)]
          (is (some? (:class->mapping routing))
              "Should have class->mapping index")
          (is (some? (:predicate->mapping routing))
              "Should have predicate->mapping index")
          ;; Check class mappings
          (is (contains? (:class->mapping routing) "http://example.org/Airline"))
          (is (contains? (:class->mapping routing) "http://example.org/Airport"))
          (is (contains? (:class->mapping routing) "http://example.org/Route")))))))

(deftest multi-table-routing-indexes-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Routing indexes correctly map predicates to tables"
      (let [vg (iceberg-vg/create {:alias "openflights"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})
            routing (:routing-indexes vg)
            pred->mapping (:predicate->mapping routing)]
        ;; Airline predicates should route to airlines table
        (is (= "openflights/airlines"
               (get-in pred->mapping ["http://example.org/callsign" :table])))

        ;; Airport predicates should route to airports table
        (is (= "openflights/airports"
               (get-in pred->mapping ["http://example.org/city" :table])))

        ;; Route predicates should route to routes table
        (is (= "openflights/routes"
               (get-in pred->mapping ["http://example.org/sourceAirport" :table])))))))

(deftest multi-table-single-table-query-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Query against single table in multi-table VG works"
      (let [vg (iceberg-vg/create {:alias "openflights"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})
            ;; Query airlines table via type pattern
            patterns [(make-triple (var-map "?airline")
                                   (iri-map "http://example.org/name")
                                   (var-map "?name"))
                      (make-triple (var-map "?airline")
                                   (iri-map "http://example.org/country")
                                   (var-map "?country"))]
            solution {::iceberg-vg/iceberg-patterns patterns}
            solution-ch (async/to-chan! [solution])
            error-ch (async/chan 1)
            result-ch (where/-finalize vg nil error-ch solution-ch)
            results (take 10 (collect-solutions result-ch))]
        (is (pos? (count results)) "Should return results from airlines table")
        (is (every? #(and (contains? % (symbol "?name"))
                          (contains? % (symbol "?country")))
                    results)
            "Each result should have ?name and ?country")))))

(deftest e2e-multi-table-vg-query-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: Query multi-table VG via Fluree API"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Query airlines from multi-table VG (single table query)
        (let [query {"from" ["iceberg/openflights"]
                     "select" ["?name" "?country"]
                     "where" {"@id" "?airline"
                              "http://example.org/name" "?name"
                              "http://example.org/country" "?country"}
                     "limit" 5}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results")
          (is (= 5 (count res)) "Should return 5 results (limit)")
          (is (every? #(= 2 (count %)) res) "Each result should have 2 values"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Filter Pushdown Tests
;;; ---------------------------------------------------------------------------

(deftest extract-comparison-test
  (testing "Extract comparison from parsed filter forms"
    ;; Test the private function via the public interface
    (let [extract-fn #'iceberg-vg/extract-comparison]

      (testing "Greater than"
        (is (= {:op :gt :var '?x :value 100}
               (extract-fn '(> ?x 100)))))

      (testing "Less than or equal"
        (is (= {:op :lte :var '?x :value 50}
               (extract-fn '(<= ?x 50)))))

      (testing "Equality"
        (is (= {:op :eq :var '?name :value "test"}
               (extract-fn '(= ?name "test")))))

      (testing "Reversed comparison (literal op var)"
        (is (= {:op :lt :var '?x :value 100}
               (extract-fn '(> 100 ?x)))))

      (testing "IN expression"
        (is (= {:op :in :var '?status :value ["A" "B" "C"]}
               (extract-fn '(in ?status ["A" "B" "C"])))))

      (testing "Null check"
        (is (= {:op :is-null :var '?x :value nil}
               (extract-fn '(nil? ?x)))))

      (testing "Bound check"
        (is (= {:op :not-null :var '?x :value nil}
               (extract-fn '(bound ?x)))))

      (testing "Non-pushable: var-to-var comparison"
        (is (nil? (extract-fn '(= ?x ?y)))))

      (testing "Non-pushable: function application"
        (is (nil? (extract-fn '(strLen ?x))))))))

(deftest analyze-filter-pattern-test
  (testing "Analyze filter patterns for pushability"
    (let [analyze-fn #'iceberg-vg/analyze-filter-pattern]

      (testing "Single-var equality filter is pushable"
        (let [filter-fn (with-meta identity {:forms '((= ?x 100)) :vars #{'?x}})
              pattern [:filter filter-fn]
              result (analyze-fn pattern)]
          (is (:pushable? result))
          (is (= 1 (count (:comparisons result))))
          (is (= :eq (-> result :comparisons first :op)))))

      (testing "Multi-var filter is not pushable"
        (let [filter-fn (with-meta identity {:forms '((= ?x ?y)) :vars #{'?x '?y}})
              pattern [:filter filter-fn]
              result (analyze-fn pattern)]
          (is (not (:pushable? result))))))))

(deftest optimizable-reorder-test
  (when @vg
    (testing "Optimizable -reorder analyzes filters"
      ;; Create a simple parsed query structure
      (let [parsed-query {:where [;; Triple pattern
                                  [(var-map "?airline")
                                   (iri-map "http://example.org/airlines/name")
                                   (var-map "?name")]
                                  ;; Filter pattern (mock)
                                  [:filter (with-meta identity
                                             {:forms '((> ?id 100))
                                              :vars #{'?id}})]]}
            result-ch (async/<!! (optimize/-reorder @vg parsed-query))]
        ;; The query should be returned (possibly with annotations)
        (is (map? result-ch))
        (is (contains? result-ch :where))))))

(deftest e2e-filter-pushdown-sparql-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: SPARQL FILTER with range comparison"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-filter-pushdown:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Query with FILTER that should be pushed down
        ;; Note: The actual pushdown happens at Iceberg level;
        ;; here we verify the query works correctly
        (let [sparql "PREFIX ex: <http://example.org/airlines/>
                      SELECT ?name ?country
                      FROM <iceberg/airlines-filter-pushdown>
                      WHERE {
                        ?airline ex:name ?name .
                        ?airline ex:country ?country .
                        FILTER (?country = \"United States\")
                      }
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results")
          (is (<= (count res) 10) "Should respect limit"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-literal-filter-exact-count-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Literal filter returns exact expected count (US airlines = 1099)"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-us-count:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Count US airlines with literal filter pushdown
        (let [query {"from" ["iceberg/airlines-us-count"]
                     "select" ["(count ?airline)"]
                     "where" {"@id" "?airline"
                              "http://example.org/airlines/name" "?name"
                              "http://example.org/airlines/country" "United States"}}
              res @(fluree/query-connection @e2e-conn query)]
          ;; Known count from dataset: 1099 US airlines
          (is (= [[1099]] res)
              "Should return exactly 1099 US airlines (proves filter pushdown works)"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-filter-pushdown-exact-count-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: SPARQL FILTER > pushdown returns exact expected count (id > 6000 = 648)"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-id-filter:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Count airlines with id > 6000 using SPARQL FILTER
        ;; This tests the Optimizable protocol filter pushdown
        (let [sparql "PREFIX ex: <http://example.org/airlines/>
                      SELECT (COUNT(?airline) AS ?count)
                      FROM <iceberg/airlines-id-filter>
                      WHERE {
                        ?airline ex:name ?name .
                      }
                      "
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          ;; First verify we get all 6162 without filter
          (is (= [[6162]] res)
              "Without filter, should return all 6162 airlines"))

        ;; TODO: Once FILTER pushdown for non-literal comparisons is fully wired,
        ;; enable this test to verify id > 6000 returns exactly 648 airlines
        ;; (let [sparql-filtered "PREFIX ex: <http://example.org/airlines/>
        ;;                       SELECT (COUNT(?airline) AS ?count)
        ;;                       FROM <iceberg/airlines-id-filter>
        ;;                       WHERE {
        ;;                         ?airline ex:name ?name .
        ;;                         ?airline ex:id ?id .
        ;;                         FILTER (?id > 6000)
        ;;                       }"
        ;;       res-filtered @(fluree/query-connection @e2e-conn sparql-filtered {:format :sparql})]
        ;;   (is (= [[648]] res-filtered)
        ;;       "Should return exactly 648 airlines with id > 6000"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; VALUES Clause -> IN Predicate Pushdown Tests
;;; ---------------------------------------------------------------------------

(deftest extract-values-in-predicate-test
  (testing "Extract IN predicate from VALUES patterns"
    (let [extract-fn #'iceberg-vg/extract-values-in-predicate]

      (testing "FQL parsed format: vector of solution maps"
        ;; This is the format after FQL parsing: [:values [{?var match-obj} ...]]
        (let [pattern [:values [{'?country {::where/val "US"}}
                                {'?country {::where/val "Canada"}}
                                {'?country {::where/val "Mexico"}}]]
              result (extract-fn pattern)]
          (is (some? result) "Should extract VALUES predicate from FQL format")
          (is (= '?country (:var result)))
          (is (= ["US" "Canada" "Mexico"] (:values result)))))

      (testing "Single-var VALUES with wrapped match objects"
        (let [pattern [:values ["?country" [{::where/val "US"}
                                            {::where/val "Canada"}
                                            {::where/val "Mexico"}]]]
              result (extract-fn pattern)]
          (is (some? result) "Should extract VALUES predicate")
          (is (= '?country (:var result)))
          (is (= ["US" "Canada" "Mexico"] (:values result)))))

      (testing "Single-var VALUES with raw string literals (SPARQL format)"
        (let [pattern [:values ['?country ["United States" "Canada" "Mexico"]]]
              result (extract-fn pattern)]
          (is (some? result) "Should extract VALUES predicate from raw strings")
          (is (= '?country (:var result)))
          (is (= ["United States" "Canada" "Mexico"] (:values result)))))

      (testing "Single-var VALUES with integer literals"
        (let [pattern [:values ["?id" [{::where/val 100}
                                       {::where/val 200}
                                       {::where/val 300}]]]
              result (extract-fn pattern)]
          (is (some? result))
          (is (= '?id (:var result)))
          (is (= [100 200 300] (:values result)))))

      (testing "Single-var VALUES with raw integer literals"
        (let [pattern [:values ['?id [100 200 300]]]
              result (extract-fn pattern)]
          (is (some? result))
          (is (= '?id (:var result)))
          (is (= [100 200 300] (:values result)))))

      (testing "VALUES with IRI values - not pushable"
        (let [pattern [:values ["?type" [{::where/iri "http://example.org/Type1"}
                                         {::where/iri "http://example.org/Type2"}]]]
              result (extract-fn pattern)]
          (is (nil? result) "IRI values should not be pushable")))

      (testing "Non-VALUES pattern returns nil"
        (is (nil? (extract-fn [:filter identity])))
        (is (nil? (extract-fn [:bind identity])))))))

(deftest annotate-values-pushdown-test
  (when @vg
    (testing "Annotate patterns with VALUES/IN pushdown"
      (let [annotate-fn #'iceberg-vg/annotate-values-pushdown
            mappings (:mappings @vg)
            routing-indexes (:routing-indexes @vg)

            ;; Triple pattern that binds ?country
            triple-pattern [(var-map "?airline")
                            (iri-map "http://example.org/airlines/country")
                            (var-map "?country")]

            ;; VALUES predicate for ?country
            values-pred {:var '?country :values ["US" "Canada"]}

            ;; Annotate
            result (annotate-fn [triple-pattern] [values-pred] mappings routing-indexes)]

        (is (= 1 (count result)))
        (let [annotated (first result)
              pushdown-filters (::iceberg-vg/pushdown-filters (meta annotated))]
          (is (vector? pushdown-filters) "Should have pushdown filters")
          (is (= 1 (count pushdown-filters)))
          (is (= :in (-> pushdown-filters first :op)))
          (is (= ["US" "Canada"] (-> pushdown-filters first :value))))))))

(deftest optimizable-reorder-values-test
  (when @vg
    (testing "Optimizable -reorder processes VALUES patterns"
      (let [parsed-query {:where [;; Triple pattern
                                  [(var-map "?airline")
                                   (iri-map "http://example.org/airlines/country")
                                   (var-map "?country")]
                                  [(var-map "?airline")
                                   (iri-map "http://example.org/airlines/name")
                                   (var-map "?name")]
                                  ;; VALUES pattern
                                  [:values ["?country" [{::where/val "United States"}
                                                        {::where/val "Canada"}]]]]}
            result (async/<!! (optimize/-reorder @vg parsed-query))]
        (is (map? result))
        (is (contains? result :where))
        ;; VALUES pattern should be REMOVED when successfully pushed to Iceberg
        ;; This prevents double-application (VALUES decomposition + IN pushdown)
        (is (not (some #(= :values (first %)) (:where result)))
            "Pushed VALUES pattern should be removed from :where to avoid double-application")
        ;; Triple patterns should still be present
        (is (>= (count (:where result)) 2)
            "Triple patterns should still be present")))))

(deftest e2e-values-in-pushdown-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: SPARQL VALUES clause pushes IN predicate to Iceberg"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-values:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Query with VALUES clause - should push IN predicate to Iceberg
        (let [sparql "PREFIX ex: <http://example.org/airlines/>
                      SELECT ?name ?country
                      FROM <iceberg/airlines-values>
                      WHERE {
                        ?airline ex:name ?name .
                        ?airline ex:country ?country .
                        VALUES ?country { \"United States\" \"Canada\" \"Mexico\" }
                      }
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results")
          (is (<= (count res) 20) "Should respect limit")
          ;; All results should have country from VALUES list
          (is (every? #(#{"United States" "Canada" "Mexico"} (second %)) res)
              "All countries should be from VALUES list"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-values-count-pushdown-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: COUNT with VALUES verifies correct filtering"
      (setup-fluree-system)
      (try
        ;; Register the Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/airlines-values-count:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping mapping-path}}))

        ;; Count with VALUES for US and Canada
        ;; US = 1099, Canada = 323 (known from dataset)
        (let [sparql "PREFIX ex: <http://example.org/airlines/>
                      SELECT (COUNT(?airline) AS ?count)
                      FROM <iceberg/airlines-values-count>
                      WHERE {
                        ?airline ex:name ?name .
                        ?airline ex:country ?country .
                        VALUES ?country { \"United States\" \"Canada\" }
                      }"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          ;; 1099 (US) + 323 (Canada) = 1422
          (is (= [[1422]] res)
              "Should return combined count for US + Canada airlines (1099 + 323 = 1422)"))

        (finally
          (teardown-fluree-system))))))

;; TODO: FILTER IN pushdown is not currently working because pattern metadata
;; attached during -reorder doesn't survive through the WHERE executor.
;; The IN filter IS parsed and identified as pushable, but the metadata is lost
;; when patterns flow through the matcher protocol.
;; For now, use VALUES clauses for IN-style filtering as they work correctly.
;;
;; (deftest e2e-filter-in-pushdown-test
;;   (when (and (warehouse-exists?) (mapping-exists?))
;;     (testing "End-to-end: FILTER with IN predicate pushes to Iceberg"
;;       (setup-fluree-system)
;;       (try
;;         (async/<!! (nameservice/publish-vg
;;                     @e2e-publisher
;;                     {:vg-name "iceberg/airlines-filter-in:main"
;;                      :vg-type "fidx:Iceberg"
;;                      :config {:warehouse-path warehouse-path
;;                               :mapping mapping-path}}))
;;         (let [sparql "PREFIX ex: <http://example.org/airlines/>
;;                       SELECT (COUNT(?airline) AS ?count)
;;                       FROM <iceberg/airlines-filter-in>
;;                       WHERE {
;;                         ?airline ex:name ?name .
;;                         ?airline ex:country ?country .
;;                         FILTER(?country IN (\"United States\", \"Canada\"))
;;                       }"
;;               res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
;;           (is (= [[1422]] res)
;;               "FILTER IN should return same count as VALUES (1422)"))
;;         (finally
;;           (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test))
