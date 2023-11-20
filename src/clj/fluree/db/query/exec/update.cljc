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

(defn retract
  [db txn {:keys [t] :as _tx-state} fuel-tracker error-ch solution-ch]
  (let [clause           (:delete txn)
        matched-ch       (async/chan 2 (comp (mapcat (partial assign-clause clause))
                                             (filter where/all-matched?)
                                             (map (partial where/compute-sids db))))
        retract-ch       (async/chan 2 (comp cat
                                             (map (fn [f]
                                                    (flake/flip-flake f t)))))]
    (async/pipe solution-ch matched-ch)

    (async/pipeline-async 2
                          retract-ch
                          (fn [triple ch]
                            (-> db
                                (where/resolve-flake-range fuel-tracker error-ch triple)
                                (async/pipe ch)))
                          matched-ch)
    retract-ch))

(defn matched-triple->flake
  [db t [s-mch p-mch o-mch]]
  (let [nses  (:namespaces db)
        sid   (-> s-mch where/get-iri (iri/iri->sid nses))
        p-iri (where/get-iri p-mch)
        pid   (iri/iri->sid p-iri nses)
        m     (where/get-meta o-mch)]
    (if-let [oid (some-> o-mch where/get-iri (iri/iri->sid nses))]
      (flake/create sid pid oid const/$xsd:anyURI t true m)
      (let [v  (where/get-value o-mch)
            dt (or (dbproto/-p-prop db :datatype p-iri)
                   (-> o-mch where/get-datatype (iri/iri->sid nses)))]
        (flake/create sid pid v dt t true m)))))

(defn insert
  [db txn {:keys [t]} solution-ch]
  (let [clause    (:insert txn)
        insert-ch (async/chan 2 (comp (mapcat (partial assign-clause clause))
                                      (filter where/all-matched?)
                                      (map (partial matched-triple->flake db t))))]
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
                                 (async/chan 2 (where/with-default where/blank-solution)))]
    (cond
      (and (insert? parsed-txn)
           (retract? parsed-txn))
      (insert-retract db parsed-txn tx-state fuel-tracker error-ch solution-ch*)

      (insert? parsed-txn)
      (insert db parsed-txn tx-state solution-ch*)

      (retract? parsed-txn)
      (retract db parsed-txn tx-state fuel-tracker error-ch solution-ch*))))
