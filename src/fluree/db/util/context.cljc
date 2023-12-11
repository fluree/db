(ns fluree.db.util.context
  (:require [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util]
            [fluree.db.constants :as const]))

;; handles some default context merging.

#?(:clj (set! *warn-on-reflection* true))

(defn stringify-context-key
  [k]
  (cond
    (string? k)
    k

    (keyword? k)
    (if-let [ns (namespace k)]
      (str ns ":" (name k))
      (name k))

    :else
    (throw (ex-info (str "Context key appears to be invalid: " k)
                    {:status 400
                     :error  :db/invalid-context}))))

(defn stringify-context-val
  [v]
  ;; reserved terms are not inclusive, only focused on possible map value terms (excludes e.g. @version)
  (let [reserved-terms-map {:id        "@id"
                            :type      "@type"
                            :value     "@value"
                            :list      "@list"
                            :set       "@set"
                            :context   "@context"
                            :language  "@language"
                            :reverse   "@reverse"
                            :container "@container"
                            :graph     "@graph"}]
    (cond
      (string? v)
      v

      (map? v)
      (reduce-kv (fn [acc k' v']
                   (let [k'* (or (get reserved-terms-map k')
                                 (stringify-context-key k'))
                         v'* (or (get reserved-terms-map v')
                                 (stringify-context-key v'))]
                     (assoc acc k'* v'*)))
                 {}
                 v)

      (keyword? v)
      (stringify-context-key v)

      :else
      (throw (ex-info (str "Invalid default context value provided: " v)
                      {:status 400 :error :db/invalid-context})))))

(defn stringify-context
  "Ensures mapified context that might use keyword keys is in string format."
  [context]
  (when context
    (if (map? context)
      (reduce-kv
        (fn [acc k v]
          (let [k* (stringify-context-key k)
                v* (stringify-context-val v)]
            (assoc acc k* v*)))
        {}
        context)
      (throw (ex-info (str "stringify-context called on a context that is not a map: " context)
                      {:status 400
                       :error  :db/invalid-context})))))

(defn txn-context
  "Remove the fluree context from the supplied context."
  [txn]
  (let [supplied-context (when (or (contains? txn :context)
                                   (contains? txn "@context"))
                              (->> (get txn "@context" (:context txn))
                                   (util/sequential)
                                   (remove #{"https://ns.flur.ee"})))]

    (when (seq supplied-context)
      (json-ld/parse-context supplied-context))))

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

(defn extract
  [jsonld]
  (-> jsonld extract-supplied-context json-ld/parse-context))
