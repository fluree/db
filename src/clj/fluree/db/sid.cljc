(ns fluree.db.sid)

(defrecord SID [namespace-code name]
  IComparable
  (-compare [_ x]
    (when-not (instance? SID x)
      (throw (ex-info "Can't compare an SID to another type"
                      {:status 500 :error :db/unexpected-error})))
    (let [ns-cmp (compare namespace-code (:namespace-code x))]
      (if-not (zero? ns-cmp)
        ns-cmp
        (compare name (:name x))))))
