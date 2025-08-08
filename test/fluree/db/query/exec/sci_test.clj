(ns fluree.db.query.exec.sci-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.json-ld :as json-ld]))

(deftest test-sci-evaluation-directly
  (testing "Direct SCI evaluation of iri function"
    ;; First check that we can call the iri-with-context function directly
    (let [raw-ctx {"ex" "http://example.org/"}
          parsed-ctx (json-ld/parse-context raw-ctx)
          ;; Test json-ld expansion directly
          expanded (json-ld/expand-iri "ex:name" parsed-ctx)]
      (is (= "http://example.org/name" expanded)
          "json-ld/expand-iri should expand the prefix"))

    (let [raw-ctx {"ex" "http://example.org/"}
          parsed-ctx (json-ld/parse-context raw-ctx)
          iri-fn (fn [input]
                   (let [value (if (map? input)
                                 (:value input)
                                 input)
                         expanded (if (= "@type" value)
                                    const/iri-rdf-type
                                    (json-ld/expand-iri value parsed-ctx))]
                     (where/->typed-val expanded const/iri-id)))
          direct-result (iri-fn "ex:name")]
      (is (= "http://example.org/name" (:value direct-result))
          "Direct iri function call should work"))

    ;; Now test through SCI - parse context before passing to eval-graalvm-with-context
    (let [raw-ctx {"ex" "http://example.org/"}
          parsed-ctx (json-ld/parse-context raw-ctx)
          ;; Use the unqualified symbol as queries would
          form '(iri "ex:name")
          result (eval/eval-graalvm-with-context form parsed-ctx)]
      (is (= "http://example.org/name" (:value result))
          "Should expand ex:name to full IRI")
      (is (= "@id" (:datatype-iri result))
          "Should have @id datatype for IRIs")))

  (testing "Direct SCI evaluation with @type"
    (let [raw-ctx {}
          parsed-ctx (json-ld/parse-context raw-ctx)
          form '(iri "@type")
          result (eval/eval-graalvm-with-context form parsed-ctx)]
      (is (= "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" (:value result))
          "Should expand @type to rdf:type IRI")))

  (testing "Direct SCI evaluation with datatype function"
    (let [raw-ctx {}
          parsed-ctx (json-ld/parse-context raw-ctx)
          typed-val {:value "test" :datatype-iri "http://www.w3.org/2001/XMLSchema#string"}
          ;; Use list instead of syntax quote to avoid namespace qualification
          form (list 'datatype typed-val)
          result (eval/eval-graalvm-with-context form parsed-ctx)]
      (is (= "http://www.w3.org/2001/XMLSchema#string" (:value result))
          "Should return the datatype IRI"))))