(ns graalvm-simple-test
  "Simple test for GraalVM native compilation"
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const])
  (:gen-class))

(defn test-sci []
  (println "Testing SCI compilation...")
  (let [f (eval/compile '(+ 1 2 3) {})
        result (f {})]
    (println "  SCI result:" (:value result))
    (assert (= 6 (:value result)) "SCI test failed")))

(defn test-flake []
  (println "Testing flake operations...")
  (let [f (flake/create 1 2 "test" const/$xsd:string nil nil 1)
        size (flake/size-flake f)]
    (println "  Flake size:" size)
    (assert (pos? size) "Flake test failed")))

(defn -main []
  (println "GraalVM Native Image Test")
  (println "=========================")
  (try
    (test-sci)
    (test-flake)
    (println "\nAll tests passed!")
    (System/exit 0)
    (catch Exception e
      (println "\nTest failed:" (.getMessage e))
      (.printStackTrace e)
      (System/exit 1))))
