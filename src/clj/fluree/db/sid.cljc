(ns fluree.db.sid)

(declare compare-SIDs)

;; TODO - verify sort order is same!!
;;
(defrecord SID [namespace-code name]
  #?@(:clj  [java.lang.Comparable
             (compareTo [sid1 sid2] (compare-SIDs sid1 sid2))]

      :cljs [IComparable
             (-compare [sid1 sid2] (compare-SIDs sid1 sid2))]))


(defn compare-SIDs
  [sid1 sid2]
  (when-not (instance? SID sid2)
    (throw (ex-info "Can't compare an SID to another type"
                    {:status 500 :error :db/unexpected-error})))
  (let [ns-cmp (compare (:namespace-code sid1) (:namespace-code sid2))]
    (if-not (zero? ns-cmp)
      ns-cmp
      (compare (:name sid1) (:name sid2)))))

(defn sid?
  [x]
  (instance? SID x))


(comment

 #fluree.db.sid.SID{:namespace-code 101, :name "alice"} #fluree.db.sid.SID{:namespace-code 101, :name "brian"}

 (def x-a (->SID 101 "alice"))
 (def x-b (->SID 101 "brian"))
 (compare x-a x-b)


 )