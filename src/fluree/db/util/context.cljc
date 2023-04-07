(ns fluree.db.util.context)

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
                                         "but not default context provided.")
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
