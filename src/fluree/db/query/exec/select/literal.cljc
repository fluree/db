(ns fluree.db.query.exec.select.literal
  (:require [fluree.db.query.exec.where :as where]))

(def virtual-properties
  #{"@value" "@type" "@language"})

(defn get-vprop-value
  [match vprop compact-fn]
  (when (contains? virtual-properties vprop)
    (case vprop
      "@value"    (where/get-value match)
      "@type"     (-> match where/get-datatype-iri compact-fn)
      "@language" (where/get-lang match))))

(defn ensure-compact-vprop
  [cache-value vprop compact-fn]
  (if (contains? cache-value vprop)
    cache-value
    (assoc cache-value vprop {:as (compact-fn vprop)})))

(defn get-compact-vprop
  [cache compact-fn vprop]
  (-> cache
      (vswap! ensure-compact-vprop vprop compact-fn)
      (get vprop)
      :as))

(defn initial-value-node
  [match compact-fn {:keys [wildcard?] :as _select-spec} cache]
  (if wildcard?
    (reduce (fn [node vprop]
              (if-let [vprop-value (get-vprop-value match vprop compact-fn)]
                (let [compact-vprop (get-compact-vprop cache compact-fn vprop)]
                  (assoc node compact-vprop vprop-value))
                node))
            {} virtual-properties)
    {}))

(defn format-literal
  [match compact-fn select-spec cache]
  (let [initial-node (initial-value-node match compact-fn select-spec cache)
        props        (remove keyword? (keys select-spec))]
    (reduce (fn [node prop]
              (let [prop-key   (get-compact-vprop cache compact-fn prop)
                    prop-value (get-vprop-value match prop compact-fn)]
                (assoc node prop-key prop-value)))
            initial-node props)))

(defn literal-match
  [value datatype language]
  (let [mch where/unmatched]
    (if language
      (where/match-lang mch value language)
      (where/match-value mch value datatype))))
