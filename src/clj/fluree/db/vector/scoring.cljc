(ns fluree.db.vector.scoring
  #?(:clj (:require [mikera.vectorz.core :as v])))

#?(:clj (set! *warn-on-reflection* true))

(defn vectorize
  "Takes vector collection v and wraps it in a more optimized vector format
  for faster vector math."
  [v]
  #?(:clj  (v/vec v)
     :cljs v))

(defn vec?
  [x]
  #?(:clj  (v/vec? x)
     :cljs (vector? x)))

(defn- dotproduct*
  [v1 v2]
  #?(:clj  (v/dot v1 v2)
     :cljs (reduce + (map * v1 v2))))

(defn- magnitude
  [v]
  #?(:clj  (v/magnitude v)
     :cljs (Math/sqrt (dotproduct* v v))))

(defn dotproduct
  [v1 v2]
  (when (and (vec? v1) (vec? v2))
    (dotproduct* v1 v2)))

(defn cosine-similarity
  [v1 v2]
  (when (and (vec? v1) (vec? v2))
    (/ (dotproduct* v1 v2)
       (* (magnitude v1)
          (magnitude v2)))))

(defn euclidian-distance
  [v1 v2]
  (when (and (vec? v1) (vec? v2))
    #?(:clj  (v/distance v1 v2)
       :cljs (Math/sqrt (reduce + (map #(* % %) (map - v1 v2)))))))

