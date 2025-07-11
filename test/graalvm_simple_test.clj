(ns graalvm-simple-test
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake])
  (:gen-class))

(defn -main []
  (println "\nTesting GraalVM Compatibility...")
  
  ;; Test 1: Basic SCI compilation
  (println "\n1. Testing SCI compilation:")
  (let [f (eval/compile '(+ 1 2) {})
        result (f {})]
    (println "   Result:" result)
    (println "   Value:" (:value result))
    (assert (= 3 (:value result))))
  
  ;; Test 2: Flake with condp (no eval)
  (println "\n2. Testing flake operations:")
  (let [f (flake/create 1 2 "test" const/$xsd:string nil nil 1)
        size (flake/size-flake f)]
    (println "   Flake:" f)
    (println "   Size:" size)
    (assert (pos? size)))
  
  ;; Test 3: JSON encoding (no reflection)
  (println "\n3. Testing JSON encoding:")
  (require '[fluree.db.util.json :as json])
  (let [data {:test [1 2 3]}
        json-str (fluree.db.util.json/stringify data)]
    (println "   JSON:" json-str)
    (assert (string? json-str)))
  
  (println "\n✅ All tests passed!"))