(ns fluree.db.sid)

(defrecord SID [namespace-code name]
  IComparable
  (-compare [_ x]
    (assert (instance? SID x) "Can't compare an SID to another type")
    (let [ns-cmp (compare namespace-code (:namespace-code x))]
      (if-not (zero? ns-cmp)
        ns-cmp
        (compare name (:name x))))))
