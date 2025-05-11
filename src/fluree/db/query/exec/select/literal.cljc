(ns fluree.db.query.exec.select.literal)

(def virtual-properties
  #{"@value" "@type" "@language"})

(defn get-vprop-value
  [{::keys [value datatype language]} vprop compact-fn]
  (when (contains? virtual-properties vprop)
    (case vprop
      "@value"    value
      "@type"     (compact-fn datatype)
      "@language" language)))

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
  [attrs compact-fn {:keys [wildcard?] :as _select-spec} cache]
  (if wildcard?
    (reduce (fn [node vprop]
              (if-let [vprop-value (get-vprop-value attrs vprop compact-fn)]
                (let [compact-vprop (get-compact-vprop cache compact-fn vprop)]
                  (assoc node compact-vprop vprop-value))
                node))
            {} virtual-properties)
    {}))

(defn attribute-map
  [value datatype language]
  {::value    value
   ::datatype datatype
   ::language language})

(defn get-value
  [attrs]
  (::value attrs))

(defn format-literal
  ([value datatype language compact-fn select-spec cache]
   (let [attrs (attribute-map value datatype language)]
     (format-literal attrs compact-fn select-spec cache)))
  ([attrs compact-fn select-spec cache]
   (let [initial-node (initial-value-node attrs compact-fn select-spec cache)
         props        (remove keyword? (keys select-spec))]
     (reduce (fn [node prop]
               (let [prop-key   (get-compact-vprop cache compact-fn prop)
                     prop-value (get-vprop-value attrs prop compact-fn)]
                 (assoc node prop-key prop-value)))
             initial-node props))))
