(ns fluree.db.query.parse)

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn variable->binding
  [sym]
  {::var sym})

(defn parse-constraint
  [cst]
  (mapv (fn [cmp]
          (cond-> cmp
            (variable? cmp) variable->binding))
        cst))

(defn parse-where
  [where]
  (mapv parse-constraint where))
