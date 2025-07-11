(ns fluree.db.util.graalvm
  "GraalVM compatibility utilities to replace eval-based macros"
  (:require [fluree.db.constants :as const]))

;; For GraalVM compatibility, we need to avoid using eval at compile time.
;; This namespace provides alternatives to eval-based utilities.

(defmacro case-const
  "Like case, but works with const values without eval.
   Uses a cond-based approach that will be optimized by the JIT compiler.
   For ClojureScript, uses the simpler condp = approach."
  [value & clauses]
  (if (:ns &env) ; ClojureScript
    `(condp = ~value ~@clauses)
    ;; For Clojure, generate a cond statement
    (let [clauses (partition 2 2 nil clauses)
          default (when (-> clauses last count (= 1))
                   (last clauses))
          clauses (if default (drop-last clauses) clauses)
          v-sym (gensym "v__")]
      `(let [~v-sym ~value]
         (cond
           ~@(mapcat (fn [[test-val result]]
                       `[(= ~v-sym ~test-val) ~result])
                     clauses)
           ~@(when default
               [:else (first default)])))))))