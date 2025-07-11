(ns fluree.db.util.compare)

(defn max-by
  ([_cmp x]
   x)
  ([cmp x y]
   (if (pos? (cmp x y))
     x
     y))
  ([cmp x y & more]
   (reduce (partial max-by cmp) (conj more x y))))

(defn max-key-by
  [cmp key-fn & xs]
  (transduce (map key-fn) (partial max-by cmp) xs))
