(ns fluree.db.util.context
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

(defn use-fluree-context
  "Clobber the top-level context and use the fluree context. This is only intended to be
  use for the initial expansion of the top-level document, where all the keys should be
  fluree vocabulary terms."
  [txn]
  (-> txn
      (dissoc :context "@context")
      (assoc "@context" "https://ns.flur.ee")))

(defn extract-supplied-context
  "Retrieves the context from the given data"
  [jsonld]
  (cond (contains? jsonld :context) (:context jsonld)
        (contains? jsonld "@context") (get jsonld "@context")))

(defn txn-context
  "Remove the fluree context from the supplied context."
  [txn]
  (when-let [ctx (extract-supplied-context txn)]
    (->> ctx
         util/sequential
         (remove #{"https://ns.flur.ee"})
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
