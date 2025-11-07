(ns fluree.db.query.explain
  (:require [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.json-ld :as json-ld]))

(defn component->user-value
  "Convert an internal pattern component to user-readable format"
  [component compact-fn]
  (cond
    (nil? component)
    nil

    (where/unmatched-var? component)
    (str (where/get-variable component))

    (where/matched-iri? component)
    (let [iri (where/get-iri component)]
      (json-ld/compact iri compact-fn))

    (where/matched-value? component)
    (where/get-value component)

    :else
    (throw (ex-info (str "Unexpected component type: " (pr-str component))
                    {:component component}))))

(defn format-pattern
  "Convert internal pattern to user-readable triple format"
  [compact-fn pattern]
  (let [ptype (where/pattern-type pattern)
        pdata (where/pattern-data pattern)]
    (case ptype
      :class
      (let [[s _ o] pdata]
        {:subject  (component->user-value s compact-fn)
         :property const/iri-type
         :object   (component->user-value o compact-fn)})

      :triple
      (let [[s p o] pdata]
        {:subject  (component->user-value s compact-fn)
         :property (component->user-value p compact-fn)
         :object   (component->user-value o compact-fn)})

      :id
      {:subject (component->user-value pdata compact-fn)}

      ;; Other pattern types (filter, bind, etc.)
      {:type ptype
       :data (pr-str pdata)})))

(defn format-plan-pattern
  [compact-fn pattern]
  (update pattern :pattern (partial format-pattern compact-fn)))

(defn format-plan
  [compact-fn plan]
  (mapv (fn [segment]
          (update segment :patterns (partial mapv (partial format-plan-pattern compact-fn))))
        plan))

(defn query
  "Provide a user-readable summary of the query plan and any optimizations applied to a query."
  [stats {:keys [plan orig-query context] :as _planned-query}]
  (let [compact-fn     (json-ld/compact-fn context)
        formatted-plan (format-plan compact-fn plan)
        original       (mapcat :patterns formatted-plan)
        optimized      (sort-by :selectivity original)

        statistics? (not= original optimized)
        heuristics  (->> optimized
                         (map where/pattern-type)
                         (filter #{:property-join})
                         (not-empty))]
    {:query orig-query
     :plan  (cond-> {:optimizations (or (->> [(when statistics? :statistics)
                                              (when heuristics :heuristics)]
                                             (remove nil?)
                                             (vec)
                                             (not-empty))
                                        [:none])
                     :original  (vec original)
                     :optimized (vec optimized)}
              statistics? (assoc :statistics {:properties (count (:properties stats))
                                              :classes    (count (:classes stats))
                                              :flakes     (:flakes stats)
                                              :index-t    (:indexed stats)
                                              :segments   formatted-plan})
              heuristics  (assoc :heuristics (vec heuristics)))}))
