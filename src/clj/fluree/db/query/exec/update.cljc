(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async]))

(defn assign-clause
  [clause solution]
  (map (fn [triple]
         (where/assign-matched-values triple solution))
       clause))

(defn retract-triple-matches
  [db t fuel-tracker error-ch matched-ch]
  (let [retract-ch (async/chan 2 (comp cat
                                       (map (fn [f]
                                              (flake/flip-flake f t)))))]
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
                                                   (map (partial where/compute-sids db))
                                                   (remove nil?))))]
    (retract-triple-matches db t fuel-tracker error-ch matched-ch)))


(defn generate-sids
  [db t sid-gen [s-mch p-mch o-mch]]
  (let [m     (where/get-meta o-mch)
        s-iri (where/get-iri s-mch)
        sid   (iri/generate-sid sid-gen s-iri)
        p-iri (where/get-iri p-mch)
        pid   (iri/generate-sid sid-gen p-iri)]
    (if (where/matched-iri? o-mch)
      (let [o-iri (where/get-iri o-mch)
            oid   (iri/generate-sid sid-gen o-iri)
            dt    const/$xsd:anyURI]
        (flake/create sid pid oid dt t true m))
      (let [v  (where/get-value o-mch)
            dt (or (dbproto/-p-prop db :datatype p-iri)
                   (some-> o-mch
                           where/get-datatype-iri
                           (as-> dt-iri (iri/generate-sid sid-gen dt-iri)))
                   (datatype/infer v))
            v* (datatype/coerce-value v dt)]
        (flake/create sid pid v* dt t true m)))))


(defn insert
  [db txn {:keys [t]} sid-gen solution-ch]
  (let [clause    (:insert txn)
        insert-ch (async/chan 2 (comp (mapcat (partial assign-clause clause))
                                      (filter where/all-matched?)
                                      (map (partial generate-sids db t sid-gen))))]
    (async/pipe solution-ch insert-ch)))

(defn insert-retract
  [db mdfn tx-state sid-gen fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2)  ; create an extra channel to multiply so
                                        ; solutions don't get dropped before we
                                        ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert db mdfn tx-state sid-gen insert-soln-ch)
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
  [db parsed-txn tx-state sid-gen fuel-tracker error-ch solution-ch]
  (let [solution-ch* (async/pipe solution-ch
                                 (async/chan 2 (comp (where/with-default where/blank-solution))))]
    (cond
      (and (insert? parsed-txn)
           (retract? parsed-txn))
      (insert-retract db parsed-txn tx-state sid-gen fuel-tracker error-ch solution-ch*)

      (insert? parsed-txn)
      (insert db parsed-txn tx-state sid-gen solution-ch*)

      (retract? parsed-txn)
      (retract db parsed-txn tx-state fuel-tracker error-ch solution-ch*))))
