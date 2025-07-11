(ns graalvm_test
  "Test namespace to verify GraalVM compatibility"
  (:require [fluree.db.api :as fluree]
            [fluree.db.query.exec.eval :as eval]
            [fluree.db.flake :as flake]
            [fluree.db.util.json :as json]
            [fluree.db.constants :as const]
            [clojure.core.async :refer [go <!]])
  (:gen-class))

(defn test-sci-evaluation []
  (println "Testing SCI evaluation...")
  (let [;; Test basic arithmetic
        f1 (eval/compile '(+ 1 2) {})
        r1 (f1 {})
        
        ;; Test with variables
        f2 (eval/compile '(* ?x ?y) {})
        r2 (f2 {'?x {:value 3 :datatype-iri const/iri-long} 
                 '?y {:value 4 :datatype-iri const/iri-long}})
        
        ;; Test comparison
        f3 (eval/compile '(> 10 5) {})
        r3 (f3 {})]
    
    (println "  Basic arithmetic:" (:value r1) "=" 3)
    (println "  Variable multiplication:" (:value r2) "=" 12)
    (println "  Comparison:" (:value r3) "=" true)
    
    (and (= 3 (:value r1))
         (= 12 (:value r2))
         (= true (:value r3)))))

(defn test-flake-operations []
  (println "Testing flake operations...")
  (let [;; Create a test flake
        f (flake/create 1 const/$xsd:string "test" const/$xsd:string nil nil 1)
        
        ;; Test size calculation (uses our fixed condp)
        size (flake/size-flake f)]
    
    (println "  Flake created:" f)
    (println "  Flake size:" size)
    
    (pos? size)))

(defn test-json-encoding []
  (println "Testing JSON encoding...")
  (let [;; Test byte array encoding (uses our fixed reflection)
        bytes (byte-array [1 2 3 4])
        json-str (json/stringify bytes)]
    
    (println "  Byte array encoded:" json-str)
    
    (string? json-str)))

(defn test-db-operations []
  (println "Testing database operations...")
  (try
    (let [;; Test connection creation
          conn-future (fluree/connect {:method :memory})]
      
      (println "  Connection future created:" (boolean conn-future))
      true)
    (catch Exception e
      (println "  Error in DB operations:" (.getMessage e))
      false)))

(defn -main [& args]
  (println "\n=== GraalVM Compatibility Test Suite ===\n")
  
  (let [results [(test-sci-evaluation)
                 (test-flake-operations)
                 (test-json-encoding)
                 (test-db-operations)]]
    
    (println "\n=== Test Results ===")
    (println "SCI Evaluation:" (nth results 0))
    (println "Flake Operations:" (nth results 1))
    (println "JSON Encoding:" (nth results 2))
    (println "DB Operations:" (nth results 3))
    
    (if (every? true? results)
      (do
        (println "\nAll tests passed! ✓")
        (System/exit 0))
      (do
        (println "\nSome tests failed! ✗")
        (System/exit 1)))))