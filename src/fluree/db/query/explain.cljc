(ns fluree.db.query.explain
  (:require [fluree.db.query.optimize :as optimize]
            [fluree.db.query.exec.where :as where]
            [fluree.db.constants :as const]
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

(defn pattern->user-format
  "Convert internal pattern to user-readable triple format"
  [pattern compact-fn]
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

(defn pattern
  "Generate explain information for a single pattern"
  [db stats pattern compact-fn]
  (let [ptype       (where/pattern-type pattern)
        selectivity (optimize/calculate-selectivity db stats pattern)]
    {:type        ptype
     :pattern     (pattern->user-format pattern compact-fn)
     :selectivity selectivity
     :optimizable (when (optimize/optimizable-pattern? pattern) ptype)}))

(defn segment
  "Generate explain information for pattern segments"
  [db stats where-clause compact-fn]
  (let [segments (optimize/segment-clause where-clause)]
    (mapv (fn [segment]
            (if (= :optimizable (:type segment))
              {:type     :optimizable
               :patterns (mapv #(pattern db stats % compact-fn) (:data segment))}
              {:type    :boundary
               :pattern (pattern db stats (:data segment) compact-fn)}))
          segments)))
