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

(defn ensure-compact-iri
  [cache-value iri compact-fn]
  (if (contains? cache-value iri)
    cache-value
    (assoc cache-value iri {:as (compact-fn iri)})))

(defn get-compact-iri
  [cache compact-fn vprop]
  (-> cache
      (vswap! ensure-compact-iri vprop compact-fn)
      (get vprop)
      :as))

(defn attribute-map
  [value datatype language]
  {::value    value
   ::datatype datatype
   ::language language})

(defn get-value
  [attrs]
  (::value attrs))

(defn format-vprop
  [attrs compact-fn {:keys [wildcard?] :as select-spec} cache vprop]
  (if-let [k (some-> select-spec
                     (get vprop)
                     :as)]
    (let [v (get-vprop-value attrs vprop compact-fn)]
      [k v])
    (when wildcard?
      (let [k (get-compact-iri cache compact-fn vprop)]
        (when-let [v (get-vprop-value attrs vprop compact-fn)]
          [k v])))))

(defn format-literal
  ([value datatype language compact-fn select-spec cache]
   (let [attrs (attribute-map value datatype language)]
     (format-literal attrs compact-fn select-spec cache)))
  ([attrs compact-fn select-spec cache]
   (into {}
         (comp (map (partial format-vprop attrs compact-fn select-spec cache))
               (remove nil?))
         virtual-properties)))
