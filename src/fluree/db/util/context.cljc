(ns fluree.db.util.context
  (:require [clojure.string :as str]))

;; handles some default context merging.

#?(:clj (set! *warn-on-reflection* true))

(defn mapify-context
  "An unparsed context may be a vector of maps or URLs. This merges them together into one large map.

  If a sequence context is used, and an item is an empty string, will substitute a default context."
  [context default-context]
  (when context
    (cond
      (map? context)
      context

      (sequential? context)
      (reduce (fn [acc context-item]
                (cond
                  (map? context-item)
                  (merge acc context-item)

                  (= "" context-item)
                  (if default-context
                    (merge acc default-context)
                    (throw (ex-info (str "Context uses a default context with empty string (''), "
                                         "but no default context provided.")
                                    {:status 400
                                     :error  :db/invalid-context})))

                  :else
                  (throw (ex-info (str "Only context maps are supported at the moment, provided: " context-item)
                                  {:status 400
                                   :error  :db/invalid-context}))))
              {} context)

      :else
      (throw (ex-info (str "Invalid context provided: " context)
                      {:status 400
                       :error  :db/invalid-context})))))


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


(defn keywordize-context
  "Keywordizes a mapified context. Changes all keys to a keyword, unless they start with '@'

  Throws an exception if context cannot be keywordized (some cannot!)"
  [context]
  (when context
    (if (map? context)
      (reduce-kv
        (fn [acc k v]
          (cond
            (not (string? k))
            (throw (ex-info (str "Context key expected to be a string, instead got: " k)
                            {:status 400
                             :error  :db/invalid-context}))

            (str/starts-with? k "@")
            (assoc acc k v)

            (str/includes? k ":")
            (let [parts (str/split k #":")]
              (if (not= 2 (count parts))
                (throw (ex-info (str "Context key appears to be invalid: " k)
                                {:status 400
                                 :error  :db/invalid-context}))
                (assoc acc (keyword (first parts) (second parts)) v)))

            :else
            (assoc acc (keyword k) v)))
        {}
        context)
      (throw (ex-info (str "keywordize-context called on a context that is not a map: " context)
                      {:status 400
                       :error  :db/invalid-context})))))
