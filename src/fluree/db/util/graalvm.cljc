(ns fluree.db.util.graalvm
  "GraalVM compatibility utilities to replace eval-based macros"
  (:require [fluree.db.constants :as const]
            #?(:clj [clojure.java.io :as io])
            [clojure.string :as str]))

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
               [:else (first default)]))))))

#?(:clj
   (defmacro embed-resource
     "Embeds resource content at compile time for GraalVM compatibility.
      This ensures resources are available in native images."
     [resource-path]
     (if-let [resource-url (io/resource resource-path)]
       (slurp resource-url)
       (throw (ex-info (str "Resource not found: " resource-path)
                       {:resource resource-path})))))

#?(:clj
   (defn load-resource
     "Loads a resource, with fallback for GraalVM native images.
      In native images, io/resource may return nil, so we embed resources at compile time."
     [resource-path]
     (if-let [resource-url (io/resource resource-path)]
       (slurp resource-url)
       ;; This branch should not be reached if embed-resource is used correctly
       (throw (ex-info (str "Resource not found: " resource-path)
                       {:resource resource-path})))))