(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async]))

(defn assign-clause
  [clause solution]
  (map (fn [triple]
         (where/assign-matched-values triple solution))
       clause))

(defn retract-xf
  [t]
  (comp cat
        (map (fn [f]
               (flake/flip-flake f t)))))

(defn retract-matches
  [db t fuel-tracker error-ch matched-ch]
  (let [retract-ch (async/chan 2 (retract-xf t))]
    (async/pipeline-async 2
                          retract-ch
                          (fn [matched-triple ch]
                            (-> db
                                (where/resolve-flake-range fuel-tracker error-ch matched-triple)
                                (async/pipe ch)))
                          matched-ch)
    retract-ch))

(defn retract
  [db txn {:keys [t] :as _tx-state} fuel-tracker error-ch solution-ch]
  (let [clause     (:delete txn)
        matched-ch (async/pipe solution-ch
                               (async/chan 2 (comp (mapcat (partial assign-clause clause))
                                                   (filter where/all-matched?)
                                                   (map (partial where/compute-sids db)))))]
    (retract-matches db t fuel-tracker error-ch matched-ch)))

(defn insert-flake-xf
  [db t]
  (fn [rf]
    (let [namespaces (volatile! (:namespaces db))]
      (fn
        ([]
         (rf))

        ([result [s-mch p-mch o-mch]]
         (let [m                (where/get-meta o-mch)
               [sid new-sid-ns] (-> s-mch
                                    where/get-iri
                                    (iri/iri->sid-with-namespace @namespaces))]
           (when new-sid-ns
             (vswap! namespaces conj new-sid-ns))
           (let [p-iri (where/get-iri p-mch)
                 [pid new-pid-ns] (iri/iri->sid-with-namespace p-iri @namespaces)]
             (when new-pid-ns
               (vswap! namespaces conj new-pid-ns))
             (if (where/matched-iri? o-mch)
               (let [dt const/$xsd:anyURI
                     [oid new-oid-ns] (-> o-mch
                                          where/get-iri
                                          (iri/iri->sid-with-namespace @namespaces))]
                 (when new-oid-ns
                   (vswap! namespaces conj new-oid-ns))
                 (let [f (flake/create sid pid oid dt t true m)]
                   (rf result [f @namespaces])))
               (let [v (where/get-value o-mch)
                     [dt new-dt-ns] (or (dbproto/-p-prop db :datatype p-iri)
                                        (-> o-mch
                                            where/get-datatype
                                            (iri/iri->sid-with-namespace @namespaces)))]
                 (when new-dt-ns
                   (vswap! namespaces conj new-dt-ns))
                 (let [f (flake/create sid pid v dt t true m)]
                   (rf result [f @namespaces])))))))

        ([result]
         (rf result))))))

(defn insert
  [db txn {:keys [t]} solution-ch]
  (let [clause    (:insert txn)
        insert-ch (async/chan 2 (comp (mapcat (partial assign-clause clause))
                                      (filter where/all-matched?)
                                      (insert-flake-xf db t)))]
    (async/pipe solution-ch insert-ch)))

(defn insert-retract
  [db mdfn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2)  ; create an extra channel to multiply so
                                        ; solutions don't get dropped before we
                                        ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert db mdfn tx-state insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract db mdfn tx-state fuel-tracker error-ch retract-soln-ch)]
    (async/pipe solution-ch solution-ch*) ; now hook up the solution input
                                          ; after everything is wired
    (async/merge [insert-ch retract-ch])))

(defn insert?
  [txn]
  (contains? txn :insert))

(defn retract?
  [txn]
  (contains? txn :delete))

(defn modify
  [db parsed-txn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch* (async/pipe solution-ch
                                 (async/chan 2 (comp (where/with-default where/blank-solution))))]
    (cond
      (and (insert? parsed-txn)
           (retract? parsed-txn))
      (insert-retract db parsed-txn tx-state fuel-tracker error-ch solution-ch*)

      (insert? parsed-txn)
      (insert db parsed-txn tx-state solution-ch*)

      (retract? parsed-txn)
      (retract db parsed-txn tx-state fuel-tracker error-ch solution-ch*))))
