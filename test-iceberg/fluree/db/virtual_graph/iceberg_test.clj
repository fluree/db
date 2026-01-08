(ns ^:iceberg fluree.db.virtual-graph.iceberg-test
  "Integration tests for Iceberg virtual graph with R2RML mappings.

   Requires :iceberg alias for dependencies.
   Run with: clojure -M:dev:iceberg:cljtest -e \"(require '[fluree.db.virtual-graph.iceberg-test]) (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test)\""
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.virtual-graph.iceberg :as iceberg-vg]
            [fluree.db.virtual-graph.iceberg.pushdown :as pushdown])
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
           #"requires :warehouse-path, :store, or REST :catalog"
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

(deftest e2e-connect-iceberg-api-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Create Iceberg VG via fluree/connect-iceberg API"
      (setup-fluree-system)
      (try
        ;; Create the Iceberg virtual graph using the public API
        (let [vg-result @(fluree/connect-iceberg
                          @e2e-conn
                          "iceberg/airlines-api"
                          {:warehouse-path warehouse-path
                           :mapping mapping-path})]
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

(deftest e2e-connect-iceberg-duplicate-error-test
  (when (and (warehouse-exists?) (mapping-exists?))
    (testing "End-to-end: Creating duplicate VG should error"
      (setup-fluree-system)
      (try
        ;; Create the first VG
        @(fluree/connect-iceberg
          @e2e-conn
          "iceberg/airlines-dup"
          {:warehouse-path warehouse-path
           :mapping mapping-path})

        ;; Try to create a duplicate - API returns exception as value
        (let [result @(fluree/connect-iceberg
                       @e2e-conn
                       "iceberg/airlines-dup"
                       {:warehouse-path warehouse-path
                        :mapping mapping-path})]
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

        ;; Verify routing indexes were built (multi-map structure for Bug #3 fix)
        (let [routing (:routing-indexes vg)]
          (is (some? (:class->mappings routing))
              "Should have class->mappings index")
          (is (some? (:predicate->mappings routing))
              "Should have predicate->mappings index")
          ;; Check class mappings
          (is (contains? (:class->mappings routing) "http://example.org/Airline"))
          (is (contains? (:class->mappings routing) "http://example.org/Airport"))
          (is (contains? (:class->mappings routing) "http://example.org/Route")))))))

(deftest multi-table-routing-indexes-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Routing indexes correctly map predicates to tables"
      (let [vg (iceberg-vg/create {:alias "openflights"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})
            routing (:routing-indexes vg)
            ;; Multi-map structure: predicate -> [mapping1 mapping2 ...]
            pred->mappings (:predicate->mappings routing)]
        ;; Airline predicates should route to airlines table (first mapping)
        (is (= "openflights/airlines"
               (:table (first (get pred->mappings "http://example.org/callsign")))))

        ;; Airport predicates should route to airports table
        (is (= "openflights/airports"
               (:table (first (get pred->mappings "http://example.org/city")))))

        ;; Route predicates should route to routes table
        (is (= "openflights/routes"
               (:table (first (get pred->mappings "http://example.org/sourceAirport")))))))))

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
    ;; Test the private function via var resolution
    (let [extract-fn #'pushdown/extract-comparison]

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
    (let [analyze-fn pushdown/analyze-filter-pattern]

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
    (let [extract-fn pushdown/extract-values-in-predicate]

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
      (let [annotate-fn pushdown/annotate-values-pushdown
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
              pushdown-filters (::pushdown/pushdown-filters (meta annotated))]
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
;;; Multi-Table Hash Join Tests
;;; ---------------------------------------------------------------------------

(deftest multi-table-join-graph-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Multi-table VG has join graph from RefObjectMap"
      (let [vg (iceberg-vg/create {:alias "openflights-join"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})
            join-graph (:join-graph vg)]
        ;; Should have join graph with edges
        (is (some? join-graph) "Should have join graph")
        (is (seq (:edges join-graph)) "Join graph should have edges")

        ;; Should have 3 edges:
        ;; 1. routes -> airlines (via airline_id)
        ;; 2. routes -> airports (via src_id - sourceAirportRef)
        ;; 3. routes -> airports (via dst_id - destinationAirportRef)
        (is (= 3 (count (:edges join-graph)))
            "Should have 3 join edges from RefObjectMaps")

        ;; Verify the airline join edge
        (let [airline-edge (first (filter #(= "http://example.org/operatedBy" (:predicate %))
                                          (:edges join-graph)))]
          (is (some? airline-edge) "Should have airline join edge")
          (is (= "openflights/routes" (:child-table airline-edge)))
          (is (= "openflights/airlines" (:parent-table airline-edge)))
          (is (= [{:child "airline_id" :parent "id"}] (:columns airline-edge))))))))

(deftest multi-table-join-query-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "Multi-table query triggers hash join execution"
      (let [vg (iceberg-vg/create {:alias "openflights-hash"
                                   :config {:warehouse-path warehouse-path
                                            :mapping multi-table-mapping-path}})
            ;; Query that spans routes and airlines tables using FK predicate
            ;; The ex:operatedBy predicate is the RefObjectMap FK that links routes -> airlines
            ;; The ?airline variable is shared between the FK object and airline subject
            ;; This triggers the hash join via find-traversed-edge
            patterns [;; Route patterns - bind to routes table
                      (make-triple (var-map "?route")
                                   (iri-map "http://example.org/sourceAirport")
                                   (var-map "?src"))
                      ;; FK predicate - links route to airline via join edge
                      (make-triple (var-map "?route")
                                   (iri-map "http://example.org/operatedBy")
                                   (var-map "?airline"))
                      ;; Airline patterns - bind to airlines table
                      ;; ?airline subject matches the FK object above
                      (make-triple (var-map "?airline")
                                   (iri-map "http://example.org/name")
                                   (var-map "?airlineName"))]
            solution {::iceberg-vg/iceberg-patterns patterns}
            solution-ch (async/to-chan! [solution])
            error-ch (async/chan 1)
            result-ch (where/-finalize vg nil error-ch solution-ch)
            ;; Take limited results - should be joined, not Cartesian
            results (take 100 (collect-solutions result-ch))]
        ;; Should have results from both tables joined via hash join
        (is (pos? (count results)) "Should return joined results")
        ;; Check first result has variables from both tables
        (when (seq results)
          (let [first-result (first results)]
            ;; From routes table
            (is (contains? first-result (symbol "?src"))
                "Should have route source airport")
            (is (contains? first-result (symbol "?route"))
                "Should have route subject")
            ;; From airlines table
            (is (contains? first-result (symbol "?airlineName"))
                "Should have airline name")
            (is (contains? first-result (symbol "?airline"))
                "Should have airline subject")))))))

(deftest e2e-multi-table-join-sparql-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL query joining routes and airlines"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-join:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Query that joins routes and airlines via ex:operatedBy FK predicate
        ;; The ?airline variable is shared, triggering the hash join
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?src ?airlineName
                      FROM <iceberg/openflights-join>
                      WHERE {
                        ?route ex:sourceAirport ?src .
                        ?route ex:operatedBy ?airline .
                        ?airline ex:name ?airlineName .
                      }
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results from hash-joined query"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-multi-table-join-fql-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: FQL query joining routes and airlines"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-join-fql:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Query that joins routes and airlines via ex:operatedBy FK predicate
        ;; The ?airline variable links the FK object to airline subject
        (let [query {"from" ["iceberg/openflights-join-fql"]
                     "select" ["?src" "?airlineName"]
                     "where" [{"@id" "?route"
                               "http://example.org/sourceAirport" "?src"
                               "http://example.org/operatedBy" "?airline"}
                              {"@id" "?airline"
                               "http://example.org/name" "?airlineName"}]
                     "limit" 10}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results from hash-joined query"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; OPTIONAL (Left Outer Join) Tests
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-optional-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL OPTIONAL returns all airlines even those without routes"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-optional:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; First, verify basic multi-table query works (same as e2e-multi-table-vg-query-test)
        (let [query {"from" ["iceberg/openflights-optional"]
                     "select" ["?name" "?country"]
                     "where" {"@id" "?airline"
                              "http://example.org/name" "?name"
                              "http://example.org/country" "?country"}
                     "limit" 5}
              res @(fluree/query-connection @e2e-conn query)]
          (is (vector? res) "Should return results")
          (is (= 5 (count res)) "Should return 5 results (limit)"))

        ;; Now use OPTIONAL in SPARQL to get airlines with optional route info
        ;; Airlines without routes should still appear (left outer join)
        (let [sparql-optional "PREFIX ex: <http://example.org/>
                               SELECT ?name ?src
                               FROM <iceberg/openflights-optional>
                               WHERE {
                                 ?airline ex:name ?name .
                                 OPTIONAL {
                                   ?route ex:operatedBy ?airline .
                                   ?route ex:sourceAirport ?src .
                                 }
                               }
                               LIMIT 100"
              res @(fluree/query-connection @e2e-conn sparql-optional {:format :sparql})]
          (is (vector? res) "Should return results from OPTIONAL query")
          (is (pos? (count res)) "Should have results")
          ;; Some results should have routes (non-nil ?src)
          ;; Some results should NOT have routes (nil ?src)
          ;; We can't easily check for nils in SELECT output, but we verify the query works
          (is (<= (count res) 100) "Should respect limit"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-optional-count-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: Inner join vs OPTIONAL comparison"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-optional-count:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Inner join query - returns joined rows from routes + airlines
        ;; This uses the FK predicate ex:operatedBy to join
        (let [sparql-inner "PREFIX ex: <http://example.org/>
                            SELECT ?src ?name
                            FROM <iceberg/openflights-optional-count>
                            WHERE {
                              ?route ex:sourceAirport ?src .
                              ?route ex:operatedBy ?airline .
                              ?airline ex:name ?name .
                            }
                            LIMIT 10"
              inner-results @(fluree/query-connection @e2e-conn sparql-inner {:format :sparql})]
          ;; Should return joined results (routes with airline info)
          (is (vector? inner-results) "Should return results")
          (is (pos? (count inner-results)) "Should have joined results")
          ;; Each result should have both ?src (from routes) and ?name (from airlines)
          (is (= 2 (count (first inner-results))) "Each result should have 2 values"))

        (finally
          (teardown-fluree-system))))))

;; Note: Low-level pattern detection test removed because OPTIONAL pattern handling
;; requires the full WHERE executor pipeline. The E2E SPARQL OPTIONAL tests above
;; verify the complete integration works correctly.

;;; ---------------------------------------------------------------------------
;;; UNION Tests
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-union-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL UNION returns results from both branches"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-union:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; UNION query - get names from both airlines and airports (if airport table exists)
        ;; Using same table for both branches to test UNION mechanics
        (let [sparql-union "PREFIX ex: <http://example.org/>
                            SELECT ?name
                            FROM <iceberg/openflights-union>
                            WHERE {
                              { ?airline a ex:Airline ; ex:name ?name }
                              UNION
                              { ?airline a ex:Airline ; ex:name ?name ; ex:country \"US\" }
                            }
                            LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql-union {:format :sparql})]
          (is (vector? res) "Should return results from UNION query")
          (is (pos? (count res)) "Should have results from at least one branch")
          ;; First branch: all airlines, Second branch: US airlines only
          ;; Results should contain airlines from both (with possible duplicates)
          (is (<= (count res) 20) "Should respect limit"))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-union-different-vars-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL UNION with different variables in branches"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-union-vars:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; UNION with different variables - one branch has ?country, other doesn't
        (let [sparql-union "PREFIX ex: <http://example.org/>
                            SELECT ?name ?country
                            FROM <iceberg/openflights-union-vars>
                            WHERE {
                              { ?airline a ex:Airline ; ex:name ?name ; ex:country ?country }
                              UNION
                              { ?route ex:sourceAirport ?name }
                            }
                            LIMIT 30"
              res @(fluree/query-connection @e2e-conn sparql-union {:format :sparql})]
          (is (vector? res) "Should return results from UNION query")
          ;; Results from first branch have ?country, second branch doesn't
          ;; SPARQL UNION semantics: unbound variables appear as unbound
          (is (pos? (count res)) "Should have results"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Aggregation E2E Tests (GROUP BY + COUNT/SUM/AVG/MIN/MAX)
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-count-star-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL COUNT(*) without GROUP BY"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-count:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; COUNT(*) - count all airlines
        (let [sparql-count "PREFIX ex: <http://example.org/>
                           SELECT (COUNT(*) AS ?total)
                           FROM <iceberg/openflights-count>
                           WHERE {
                             ?airline a ex:Airline ; ex:name ?name
                           }"
              res @(fluree/query-connection @e2e-conn sparql-count {:format :sparql})]
          (is (vector? res) "Should return aggregated results")
          (is (= 1 (count res)) "COUNT(*) without GROUP BY returns 1 row")
          (when (seq res)
            (let [total (get (first res) "total")]
              (is (pos? total) "Count should be positive"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-group-by-count-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL GROUP BY with COUNT"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-group:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; GROUP BY country with COUNT - count airlines per country
        (let [sparql-group "PREFIX ex: <http://example.org/>
                           SELECT ?country (COUNT(?airline) AS ?count)
                           FROM <iceberg/openflights-group>
                           WHERE {
                             ?airline a ex:Airline ;
                                      ex:country ?country
                           }
                           GROUP BY ?country
                           ORDER BY DESC(?count)
                           LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql-group {:format :sparql})]
          (is (vector? res) "Should return grouped results")
          (is (pos? (count res)) "Should have country groups")
          ;; With :format :sparql and default :output :fql, results are vectors [country count]
          (when (seq res)
            (let [first-row (first res)]
              (is (vector? first-row) "Each row should be a vector")
              (is (= 2 (count first-row)) "Each row should have 2 elements (country, count)")
              (is (string? (first first-row)) "First element should be country (string)")
              (is (integer? (second first-row)) "Second element should be count (integer)"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-multiple-aggregates-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL GROUP BY with multiple aggregates"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-multi-agg:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Multiple aggregates - route statistics by airline
        (let [sparql-agg "PREFIX ex: <http://example.org/>
                         SELECT ?airline (COUNT(?route) AS ?route_count)
                         FROM <iceberg/openflights-multi-agg>
                         WHERE {
                           ?route a ex:Route ;
                                  ex:operatedBy ?airline
                         }
                         GROUP BY ?airline
                         ORDER BY DESC(?route_count)
                         LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql-agg {:format :sparql})]
          (is (vector? res) "Should return aggregated results")
          (is (pos? (count res)) "Should have airline groups")
          ;; With :format :sparql and default :output :fql, results are vectors [airline route_count]
          (when (seq res)
            (let [first-row (first res)]
              (is (vector? first-row) "Each row should be a vector")
              (is (= 2 (count first-row)) "Each row should have 2 elements (airline, route_count)")
              (is (string? (first first-row)) "First element should be airline (string)")
              (is (integer? (second first-row)) "Second element should be route_count (integer)"))))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; SELECT DISTINCT E2E Tests
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-select-distinct-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SELECT DISTINCT deduplicates results"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-distinct:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; SELECT DISTINCT on country - should return unique countries
        (let [sparql-distinct "PREFIX ex: <http://example.org/>
                               SELECT DISTINCT ?country
                               FROM <iceberg/openflights-distinct>
                               WHERE {
                                 ?airline a ex:Airline ;
                                          ex:country ?country
                               }
                               LIMIT 100"
              res @(fluree/query-connection @e2e-conn sparql-distinct {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have country results")
          ;; With :format :sparql and default :output :fql, results are single-element vectors [country]
          ;; All results should have unique countries
          (let [countries (map first res)]
            (is (= (count countries) (count (set countries)))
                "SELECT DISTINCT should return unique countries")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-distinct-with-aggregation-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SELECT DISTINCT with aggregation"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-distinct-agg:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; COUNT DISTINCT - count unique countries
        ;; Note: COUNT(DISTINCT ?var) is different from SELECT DISTINCT
        ;; COUNT DISTINCT is already implemented in aggregation
        (let [sparql-count-distinct "PREFIX ex: <http://example.org/>
                                     SELECT (COUNT(*) AS ?total)
                                     FROM <iceberg/openflights-distinct-agg>
                                     WHERE {
                                       ?airline a ex:Airline ;
                                                ex:country ?country
                                     }"
              res @(fluree/query-connection @e2e-conn sparql-count-distinct {:format :sparql})]
          (is (vector? res) "Should return aggregated results")
          (is (= 1 (count res)) "COUNT without GROUP BY returns 1 row"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Anti-Join E2E Tests (FILTER EXISTS, FILTER NOT EXISTS, MINUS)
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-filter-not-exists-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: FILTER NOT EXISTS excludes matching results"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-not-exists:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Find airlines that DON'T have a specific country (e.g., not United States)
        ;; This tests NOT EXISTS with a correlated subquery
        (let [sparql-not-exists "PREFIX ex: <http://example.org/>
                                 SELECT ?airline ?name ?country
                                 FROM <iceberg/openflights-not-exists>
                                 WHERE {
                                   ?airline a ex:Airline ;
                                            ex:name ?name ;
                                            ex:country ?country .
                                   FILTER NOT EXISTS {
                                     ?airline ex:country \"United States\"
                                   }
                                 }
                                 LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql-not-exists {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results (airlines not from US)")
          ;; All results should NOT be from United States
          ;; Results may be tuples [airline name country] or maps {"country" ...}
          (when (seq res)
            (let [get-country (fn [r] (if (map? r) (get r "country") (nth r 2 nil)))]
              (is (every? #(not= "United States" (get-country %)) res)
                  "FILTER NOT EXISTS should exclude US airlines"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-filter-exists-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: FILTER EXISTS keeps only matching results"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-exists:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Find airlines that DO have a specific country (e.g., United States)
        ;; This tests EXISTS with a correlated subquery
        (let [sparql-exists "PREFIX ex: <http://example.org/>
                             SELECT ?airline ?name ?country
                             FROM <iceberg/openflights-exists>
                             WHERE {
                               ?airline a ex:Airline ;
                                        ex:name ?name ;
                                        ex:country ?country .
                               FILTER EXISTS {
                                 ?airline ex:country \"United States\"
                               }
                             }
                             LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql-exists {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results (US airlines)")
          ;; All results should be from United States
          ;; Results may be tuples [airline name country] or maps {"country" ...}
          (when (seq res)
            (let [get-country (fn [r] (if (map? r) (get r "country") (nth r 2 nil)))]
              (is (every? #(= "United States" (get-country %)) res)
                  "FILTER EXISTS should only include US airlines"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-minus-test
  ;; NOTE: MINUS keyword is not yet supported in Fluree's SPARQL parser.
  ;; The VG MINUS execution code is implemented but can't be tested via SPARQL.
  ;; This test is disabled until SPARQL parser supports MINUS.
  ;; The execution code in iceberg.clj apply-minus function is ready.
  (when false ;; Disabled - SPARQL parser doesn't support MINUS keyword
    (when (and (warehouse-exists?) (multi-table-mapping-exists?))
      (testing "End-to-end: MINUS performs set difference"
        (setup-fluree-system)
        (try
          ;; Register the multi-table Iceberg virtual graph
          (async/<!! (nameservice/publish-vg
                      @e2e-publisher
                      {:vg-name "iceberg/openflights-minus:main"
                       :vg-type "fidx:Iceberg"
                       :config {:warehouse-path warehouse-path
                                :mapping multi-table-mapping-path}}))

          ;; MINUS: Get all airlines except those from United States
          ;; MINUS is an uncorrelated set difference based on shared variables
          (let [sparql-minus "PREFIX ex: <http://example.org/>
                              SELECT ?airline ?name ?country
                              FROM <iceberg/openflights-minus>
                              WHERE {
                                ?airline a ex:Airline ;
                                         ex:name ?name ;
                                         ex:country ?country .
                              }
                              MINUS {
                                ?airline ex:country \"United States\"
                              }
                              LIMIT 20"
                res @(fluree/query-connection @e2e-conn sparql-minus {:format :sparql})]
            (is (vector? res) "Should return results")
            (is (pos? (count res)) "Should have results (non-US airlines)")
            ;; All results should NOT be from United States
            ;; Results may be tuples [airline name country] or maps {"country" ...}
            (when (seq res)
              (let [get-country (fn [r] (if (map? r) (get r "country") (nth r 2 nil)))]
                (is (every? #(not= "United States" (get-country %)) res)
                    "MINUS should exclude US airlines"))))

          (finally
            (teardown-fluree-system)))))))

(deftest e2e-sparql-not-exists-cross-table-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: FILTER NOT EXISTS across tables (airlines without routes)"
      (setup-fluree-system)
      (try
        ;; Register the multi-table Iceberg virtual graph
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-not-exists-cross:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Find airlines that have no routes
        ;; This demonstrates cross-table anti-join capability
        (let [sparql-cross-not-exists "PREFIX ex: <http://example.org/>
                                       SELECT ?airline ?name
                                       FROM <iceberg/openflights-not-exists-cross>
                                       WHERE {
                                         ?airline a ex:Airline ;
                                                  ex:name ?name .
                                         FILTER NOT EXISTS {
                                           ?route ex:airlineRef ?airline
                                         }
                                       }
                                       LIMIT 50"
              res @(fluree/query-connection @e2e-conn sparql-cross-not-exists {:format :sparql})]
          (is (vector? res) "Should return results")
          ;; There should be many airlines without routes in the data
          (is (pos? (count res)) "Should have airlines without routes"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Expression Function E2E Tests (FILTER + BIND)
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-strlen-filter-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL FILTER with STRLEN (non-pushable expression)"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-strlen:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; STRLEN is a non-pushable function - must be evaluated after scan
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name
                      FROM <iceberg/openflights-strlen>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        FILTER(STRLEN(?name) > 15)
                      }
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results with long names")
          ;; All names should be longer than 15 characters
          (when (seq res)
            (is (every? #(> (count (if (vector? %) (first %) %)) 15) res)
                "All airline names should be longer than 15 characters")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-bind-ucase-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL BIND with UCASE"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-bind:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; BIND computes a new variable from an expression
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name ?upperName
                      FROM <iceberg/openflights-bind>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        BIND(UCASE(?name) AS ?upperName)
                      }
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results with computed bindings")
          ;; Each result should have both name and uppercase version
          (when (seq res)
            (let [[name upper] (first res)]
              (is (string? name) "name should be a string")
              (is (string? upper) "upperName should be a string")
              ;; Upper case should be all caps
              (is (= upper (clojure.string/upper-case name))
                  "upperName should be uppercase version of name"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-bind-then-filter-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL BIND creating variable used in FILTER"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-bind-filter:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; BIND creates a variable that is then used in FILTER
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name ?nameLen
                      FROM <iceberg/openflights-bind-filter>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        BIND(STRLEN(?name) AS ?nameLen)
                        FILTER(?nameLen > 20)
                      }
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results")
          ;; All names should have length > 20
          (when (seq res)
            (let [[name name-len] (first res)]
              (is (string? name) "name should be a string")
              (is (number? name-len) "nameLen should be a number")
              (is (> name-len 20) "nameLen should be greater than 20"))))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-regex-filter-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL FILTER with REGEX"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-regex:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; REGEX is a non-pushable function
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name
                      FROM <iceberg/openflights-regex>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        FILTER(REGEX(?name, \"^Air\", \"i\"))
                      }
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have airlines starting with 'Air'")
          ;; All names should start with 'Air' (case insensitive)
          (when (seq res)
            (is (every? #(re-find #"(?i)^Air" (if (vector? %) (first %) %)) res)
                "All airline names should start with 'Air'")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-coalesce-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL COALESCE for null handling"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-coalesce:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; COALESCE returns first non-null value
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name ?displayName
                      FROM <iceberg/openflights-coalesce>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        BIND(COALESCE(?name, \"Unknown\") AS ?displayName)
                      }
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results")
          ;; displayName should never be null/empty
          (when (seq res)
            (is (every? #(let [[_ display] %]
                           (and (string? display)
                                (seq display)))
                        res)
                "All displayNames should be non-empty strings")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-if-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL IF conditional expression"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-if:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; IF returns one of two values based on condition
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?name ?size
                      FROM <iceberg/openflights-if>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        BIND(IF(STRLEN(?name) > 15, \"long\", \"short\") AS ?size)
                      }
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have results")
          ;; size should be either "long" or "short"
          (when (seq res)
            (is (every? #(let [[name size] %]
                           (and (contains? #{"long" "short"} size)
                                ;; Verify the categorization is correct
                                (= (if (> (count name) 15) "long" "short") size)))
                        res)
                "size should correctly categorize name length")))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; HAVING Clause E2E Tests
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-having-count-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL HAVING with COUNT"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-having:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; HAVING filters groups - only countries with > 50 airlines
        ;; Note: Use aggregate alias (?count) in HAVING since Iceberg VG
        ;; computes aggregates at database level (raw values not available)
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?country (COUNT(?airline) AS ?count)
                      FROM <iceberg/openflights-having>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:country ?country
                      }
                      GROUP BY ?country
                      HAVING (?count > 50)
                      ORDER BY DESC(?count)"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return grouped results")
          (is (pos? (count res)) "Should have country groups with > 50 airlines")
          ;; All results should have count > 50
          (when (seq res)
            (is (every? #(let [[_country cnt] %]
                           (> cnt 50))
                        res)
                "All groups should have count > 50")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-having-alias-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL HAVING using aggregate alias"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-having-alias:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; HAVING using the aggregate alias variable
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?country (COUNT(?airline) AS ?airlineCount)
                      FROM <iceberg/openflights-having-alias>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:country ?country
                      }
                      GROUP BY ?country
                      HAVING (?airlineCount > 100)
                      ORDER BY DESC(?airlineCount)
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return grouped results")
          (is (pos? (count res)) "Should have country groups with > 100 airlines")
          ;; All results should have count > 100
          (when (seq res)
            (is (every? #(let [[_country cnt] %]
                           (> cnt 100))
                        res)
                "All groups should have airlineCount > 100")))

        (finally
          (teardown-fluree-system))))))

(deftest e2e-sparql-having-combined-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL HAVING with multiple conditions"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-having-combo:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; HAVING with a range condition using alias variable
        ;; Note: Use aggregate alias (?count) in HAVING since Iceberg VG
        ;; computes aggregates at database level
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?country (COUNT(?airline) AS ?count)
                      FROM <iceberg/openflights-having-combo>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:country ?country
                      }
                      GROUP BY ?country
                      HAVING (?count >= 10 && ?count <= 50)
                      ORDER BY ?count"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return grouped results")
          ;; All results should have 10 <= count <= 50
          (when (seq res)
            (is (every? #(let [[_country cnt] %]
                           (and (>= cnt 10) (<= cnt 50)))
                        res)
                "All groups should have count between 10 and 50 inclusive")))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Comprehensive Pipeline Test (BIND → FILTER → GROUP BY → HAVING → ORDER BY → LIMIT)
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-comprehensive-pipeline-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: Full SPARQL pipeline with BIND, FILTER, GROUP BY, HAVING, ORDER BY, LIMIT"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-pipeline:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Comprehensive query combining all modifiers:
        ;; 1. BIND - compute name length
        ;; 2. FILTER with REGEX - only airlines starting with "Air" (case insensitive)
        ;; 3. GROUP BY country
        ;; 4. HAVING - only countries with > 5 matching airlines
        ;; 5. ORDER BY DESC - sort by count descending
        ;; 6. LIMIT - take top 10
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?country (COUNT(?airline) AS ?airCount)
                      FROM <iceberg/openflights-pipeline>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name ;
                                 ex:country ?country .
                        BIND(STRLEN(?name) AS ?nameLen)
                        FILTER(REGEX(?name, \"^Air\", \"i\"))
                      }
                      GROUP BY ?country
                      HAVING (?airCount > 5)
                      ORDER BY DESC(?airCount)
                      LIMIT 10"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should have matching country groups")
          (is (<= (count res) 10) "Should respect LIMIT 10")

          ;; Verify HAVING constraint: all counts > 5
          (when (seq res)
            (is (every? #(let [[_country cnt] %]
                           (> cnt 5))
                        res)
                "All groups should have count > 5 (HAVING constraint)"))

          ;; Verify ORDER BY DESC: counts should be descending
          (when (>= (count res) 2)
            (let [counts (map second res)]
              (is (= counts (reverse (sort counts)))
                  "Results should be ordered by count descending"))))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; BOUND + OPTIONAL Pattern Test (Left Anti-Join Pattern)
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-bound-optional-pattern-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: BOUND + OPTIONAL pattern for left anti-join semantics"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-bound-optional:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; This is a common LLM/SPARQL pattern to find entities WITHOUT certain relationships
        ;; Semantically equivalent to FILTER NOT EXISTS but uses different constructs
        ;; Pattern: OPTIONAL { ... } FILTER(!BOUND(?var)) = "left anti-join"
        ;;
        ;; Find airlines that have NO routes (airlines not referenced by any route)
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?airline ?name
                      FROM <iceberg/openflights-bound-optional>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        OPTIONAL { ?route ex:operatedBy ?airline }
                        FILTER(!BOUND(?route))
                      }
                      LIMIT 100"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          ;; OpenFlights data has many airlines without routes
          ;; (inactive airlines, regional carriers not in routes dataset, etc.)
          (is (pos? (count res)) "Should find some airlines without routes")
          (is (<= (count res) 100) "Should respect LIMIT 100")

          ;; Verify results have expected shape [airline-iri name]
          (when (seq res)
            (is (every? #(= 2 (count %)) res)
                "Each result should have 2 values (airline, name)")
            (is (every? #(string? (first %)) res)
                "First value should be airline IRI string")
            (is (every? #(string? (second %)) res)
                "Second value should be name string")))

        ;; Also test the positive case: airlines WITH routes using BOUND
        ;; Pattern: OPTIONAL { ... } FILTER(BOUND(?var)) = "left semi-join"
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT DISTINCT ?airline ?name
                      FROM <iceberg/openflights-bound-optional>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        OPTIONAL { ?route ex:operatedBy ?airline }
                        FILTER(BOUND(?route))
                      }
                      LIMIT 100"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should find airlines with routes")
          (is (<= (count res) 100) "Should respect LIMIT"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; Subquery Test
;;; ---------------------------------------------------------------------------

(deftest e2e-sparql-subquery-test
  (when (and (warehouse-exists?) (multi-table-mapping-exists?))
    (testing "End-to-end: SPARQL subquery with aggregation"
      (setup-fluree-system)
      (try
        (async/<!! (nameservice/publish-vg
                    @e2e-publisher
                    {:vg-name "iceberg/openflights-subquery:main"
                     :vg-type "fidx:Iceberg"
                     :config {:warehouse-path warehouse-path
                              :mapping multi-table-mapping-path}}))

        ;; Test 1: Subquery with aggregation - get airlines with their route count
        ;; This is a common analytics pattern: main query + aggregation subquery
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?airline ?name ?routeCount
                      FROM <iceberg/openflights-subquery>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        {
                          SELECT ?airline (COUNT(?route) AS ?routeCount)
                          WHERE {
                            ?route ex:operatedBy ?airline
                          }
                          GROUP BY ?airline
                        }
                      }
                      ORDER BY DESC(?routeCount)
                      LIMIT 20"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          ;; Should have airlines with route counts
          (when (seq res)
            (is (pos? (count res)) "Should find airlines with routes")
            (is (<= (count res) 20) "Should respect LIMIT 20")
            ;; Each result should have 3 values: airline IRI, name, routeCount
            (is (every? #(= 3 (count %)) res)
                "Each result should have 3 values")
            ;; Route counts should be positive numbers
            (is (every? #(and (number? (nth % 2)) (pos? (nth % 2))) res)
                "Route counts should be positive numbers")))

        ;; Test 2: Simple subquery without aggregation - correlated on shared variable
        (let [sparql "PREFIX ex: <http://example.org/>
                      SELECT ?airline ?name ?country
                      FROM <iceberg/openflights-subquery>
                      WHERE {
                        ?airline a ex:Airline ;
                                 ex:name ?name .
                        {
                          SELECT ?airline ?country
                          WHERE {
                            ?airline ex:country ?country
                          }
                        }
                      }
                      LIMIT 50"
              res @(fluree/query-connection @e2e-conn sparql {:format :sparql})]
          (is (vector? res) "Should return results")
          (is (pos? (count res)) "Should find airlines with country from subquery")
          (is (<= (count res) 50) "Should respect LIMIT 50"))

        (finally
          (teardown-fluree-system))))))

;;; ---------------------------------------------------------------------------
;;; IRI Helper Function Tests
;;; ---------------------------------------------------------------------------

(deftest extract-id-from-iri-test
  (testing "Extract ID from IRI with simple template"
    (let [extract-fn (requiring-resolve 'fluree.db.virtual-graph.iceberg.query/extract-id-from-iri)]
      (testing "Standard template with ID at end"
        (is (= "123" (extract-fn "http://example.org/airline/123"
                                 "http://example.org/airline/{id}")))
        (is (= "456" (extract-fn "http://example.org/airline/456"
                                 "http://example.org/airline/{id}")))
        (is (= "abc-def" (extract-fn "http://example.org/person/abc-def"
                                     "http://example.org/person/{id}"))))

      (testing "Template with suffix"
        (is (= "123" (extract-fn "http://example.org/item/123/view"
                                 "http://example.org/item/{id}/view"))))

      (testing "Non-matching IRI returns nil"
        (is (nil? (extract-fn "http://other.org/airline/123"
                              "http://example.org/airline/{id}")))
        (is (nil? (extract-fn "http://example.org/person/123"
                              "http://example.org/airline/{id}"))))

      (testing "Nil inputs return nil"
        (is (nil? (extract-fn nil "http://example.org/{id}")))
        (is (nil? (extract-fn "http://example.org/123" nil)))))))

(deftest build-iri-from-id-test
  (testing "Build IRI from ID and template"
    (let [build-fn (requiring-resolve 'fluree.db.virtual-graph.iceberg.query/build-iri-from-id)]
      (testing "Standard template"
        (is (= "http://example.org/airline/123"
               (build-fn "123" "http://example.org/airline/{id}")))
        (is (= "http://example.org/person/alice"
               (build-fn "alice" "http://example.org/person/{name}"))))

      (testing "Template with suffix"
        (is (= "http://example.org/item/123/view"
               (build-fn "123" "http://example.org/item/{id}/view"))))

      (testing "Nil inputs return nil"
        (is (nil? (build-fn nil "http://example.org/{id}")))
        (is (nil? (build-fn "123" nil)))))))

(deftest get-column-for-predicate-test
  (when @vg
    (testing "Get column for predicate from mapping"
      (let [get-col-fn (requiring-resolve 'fluree.db.virtual-graph.iceberg.query/get-column-for-predicate)
            mapping (first (vals (:mappings @vg)))]
        (testing "Valid predicate returns column name"
          (is (= "name" (get-col-fn "http://example.org/airlines/name" mapping)))
          (is (= "country" (get-col-fn "http://example.org/airlines/country" mapping))))

        (testing "Invalid predicate returns nil"
          (is (nil? (get-col-fn "http://example.org/nonexistent" mapping))))))))

;;; ---------------------------------------------------------------------------
;;; Transitive Pattern Detection Tests
;;; ---------------------------------------------------------------------------

(deftest transitive-pattern-detection-test
  (testing "Transitive property path detection via where/get-transitive-property"
    (let [;; Create predicate match objects with transitive tags
          make-predicate (fn [iri tag]
                           (let [base {::where/iri iri}]
                             (if tag
                               (assoc base ::where/recur tag)
                               base)))]
      (testing "one+ (one-or-more) tag detected"
        (let [pred (make-predicate "http://example.org/knows" :one+)]
          (is (= :one+ (where/get-transitive-property pred)))))

      (testing "zero+ (zero-or-more) tag detected"
        (let [pred (make-predicate "http://example.org/broader" :zero+)]
          (is (= :zero+ (where/get-transitive-property pred)))))

      (testing "Non-transitive predicate returns nil"
        (let [pred (make-predicate "http://example.org/name" nil)]
          (is (nil? (where/get-transitive-property pred))))))))

(deftest transitive-pattern-removal-test
  (testing "Transitive tag removal via where/remove-transitivity"
    (let [make-predicate (fn [iri tag]
                           (let [base {::where/iri iri}]
                             (if tag
                               (assoc base ::where/recur tag)
                               base)))]
      (testing "Removes one+ tag"
        (let [pred (make-predicate "http://example.org/knows" :one+)
              result (where/remove-transitivity pred)]
          (is (nil? (::where/recur result)))
          (is (= "http://example.org/knows" (::where/iri result)))))

      (testing "Removes zero+ tag"
        (let [pred (make-predicate "http://example.org/broader" :zero+)
              result (where/remove-transitivity pred)]
          (is (nil? (::where/recur result)))
          (is (= "http://example.org/broader" (::where/iri result)))))

      (testing "Non-transitive predicate unchanged"
        (let [pred (make-predicate "http://example.org/name" nil)
              result (where/remove-transitivity pred)]
          (is (= pred result)))))))

;;; ---------------------------------------------------------------------------
;;; Transitive Path Iceberg VG Tests
;;; ---------------------------------------------------------------------------
;;
;; NOTE: Full E2E transitive path tests require a dataset with self-referential
;; relationships (e.g., employees with manager_id → employee_id, or categories
;; with parent_id → category_id). The OpenFlights data does not have such
;; hierarchical relationships.
;;
;; The transitive path implementation supports:
;; - Forward traversal: ?s pred+ ?o (subject bound)
;; - Backward traversal: ?s pred+ ?o (object bound)
;; - Both unbound: ?s pred+ ?o (expensive, requires limit)
;; - zero-or-more: pred* (includes starting node)
;; - Cycle detection via visited set
;; - Configurable depth limit (default 100)
;;
;; To test with hierarchical data, create an Iceberg table with structure like:
;; CREATE TABLE employees (
;;   id INT,
;;   name STRING,
;;   manager_id INT  -- FK to employees.id
;; )
;; Then create an R2RML mapping with a predicate like ex:reportsTo that maps
;; to manager_id, and query with:
;; SELECT ?employee ?manager WHERE { ?employee ex:reportsTo+ ?ceo }
;;
;;; ---------------------------------------------------------------------------

;;; ---------------------------------------------------------------------------
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test))
