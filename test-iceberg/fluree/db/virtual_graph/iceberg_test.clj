(ns ^:iceberg fluree.db.virtual-graph.iceberg-test
  "Integration tests for Iceberg virtual graph with R2RML mappings.

   Requires :iceberg alias for dependencies.
   Run with: clojure -M:dev:iceberg:cljtest -e \"(require '[fluree.db.virtual-graph.iceberg-test]) (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test)\""
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing use-fixtures]]
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
;;; Run from REPL
;;; ---------------------------------------------------------------------------

(defn run-tests []
  (clojure.test/run-tests 'fluree.db.virtual-graph.iceberg-test))
