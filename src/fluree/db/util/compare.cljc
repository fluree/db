(ns fluree.db.util.compare)

(defn negate-comparator
  "Returns a comparator that reverses the order of the given comparator"
  [cmp]
  (fn [a b]
    (cmp b a)))

(defn max-by
  "Returns the maximum value according to the comparator.

  The comparator `cmp` should return:
  - positive if the first argument is greater
  - negative if the second argument is greater
  - zero if they are equal

  With one argument, returns that argument.
  With two arguments, returns the greater of the two.
  With more arguments, returns the greatest among all."
  ([_cmp x]
   x)
  ([cmp x y]
   (if (pos? (cmp x y))
     x
     y))
  ([cmp x y & more]
   (reduce (partial max-by cmp) (conj more x y))))

(defn max-reducing-fn
  "Returns a reducing function that finds the maximum value according to the comparator.

  The returned function can be used with reduce, transduce, or other reducing contexts
  to find the maximum value in a collection. Returns nil for an empty collection."
  [cmp]
  (fn
    ([] nil)
    ([current-max] current-max)
    ([current-max x]
     (if current-max
       (max-by cmp current-max x)
       x))))

(defn max-key-by
  "Returns the item with the maximum key according to the comparator.

  Applies `key-fn` to each item in `xs` and returns the item whose key is
  maximum according to the comparator `cmp`. Uses transduction for efficiency."
  [cmp key-fn & xs]
  (transduce (map key-fn) (max-reducing-fn cmp) xs))

(defn min-by
  "Returns the minimum value according to the comparator."
  ([_cmp x]
   x)
  ([cmp x y]
   (max-by (negate-comparator cmp) x y))
  ([cmp x y & more]
   (apply max-by (negate-comparator cmp) x y more)))

(defn min-reducing-fn
  "Returns a reducing function that finds the minimum value."
  [cmp]
  (max-reducing-fn (negate-comparator cmp)))

(defn min-key-by
  "Returns the item with the minimum key according to the comparator."
  [cmp key-fn & xs]
  (transduce (map key-fn) (min-reducing-fn cmp) xs))
