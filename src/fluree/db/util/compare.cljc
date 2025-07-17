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

(defn max-reducing-fn
  [cmp]
  (fn
    ([] nil)
    ([current-max] current-max)
    ([current-max x]
     (if current-max
       (max-by cmp current-max x)
       x))))

(defn max-key-by
  [cmp key-fn & xs]
  (transduce (map key-fn) (max-reducing-fn cmp) xs))
