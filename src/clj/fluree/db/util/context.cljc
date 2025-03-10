(ns fluree.db.util.context
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

(defn extract-supplied-context
  "Retrieves the context from the given data"
  [jsonld]
  (or (get jsonld "@context")
      (get jsonld :context)))

(defn txn-context
  "Remove the fluree context from the supplied context."
  [txn]
  (when-let [ctx (extract-supplied-context txn)]
    (->> ctx
         json-ld/parse-context)))

(defn extract
  [jsonld]
  (-> jsonld extract-supplied-context json-ld/parse-context))

(defn stringify
  "Contexts that use clojure keywords will not translate into valid JSON for
  serialization. Here we change any keywords to strings."
  [context]
  (if (sequential? context)
    (mapv stringify context)
    (if (map? context)
      (reduce-kv
        (fn [acc k v]
          (let [k* (if (keyword? k)
                     (name k)
                     k)
                v* (if (and (map? v)
                            (not (contains? v :id)))
                     (stringify v)
                     v)]
            (assoc acc k* v*)))
        {} context)
      context)))
