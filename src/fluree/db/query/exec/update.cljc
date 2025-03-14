(ns fluree.db.query.exec.update
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.log :as log]))

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

(defn build-sid
  [{:keys [namespaces] :as _db} ns nme]
  (let [ns-code (get namespaces ns)]
    (iri/->sid ns-code nme)))

(defn ensure-namespace
  [db ns]
  (let [nses     (:namespaces db)
        ns-codes (:namespace-codes db)]
    (if (contains? nses ns)
      db
      (let [new-ns-code (iri/next-namespace-code ns-codes)]
        (-> db
            (update :namespaces assoc ns new-ns-code)
            (update :namespace-codes assoc new-ns-code ns))))))

(defn generate-sid!
  [db-vol iri]
  (let [[ns nme] (iri/decompose iri)]
    (-> db-vol
        (vswap! ensure-namespace ns)
        (build-sid ns nme))))

(defn create-iri-reference-flake
  [db-vol sid pid o-mch t m]
  (let [o-iri (where/get-iri o-mch)
        oid   (generate-sid! db-vol o-iri)
        dt    const/$id]
    (flake/create sid pid oid dt t true m)))

(defn create-scalar-flake
  [db-vol p-iri sid pid o-mch t m]
  (let [v  (where/get-value o-mch)
        dt (or (some-> o-mch
                       where/get-datatype-iri
                       (->> (generate-sid! db-vol)))
               (datatype/infer v (:lang m)))
        v* (datatype/coerce-value v dt)]
    (flake/create sid pid v* dt t true m)))

(defn build-flake
  ([db-vol t matched-triple]
   (build-flake db-vol t nil matched-triple))
  ([db-vol t reasoned [s-mch p-mch o-mch]]
   (let [m     (where/get-meta o-mch)
         s-iri (where/get-iri s-mch)
         sid   (generate-sid! db-vol s-iri)
         p-iri (where/get-iri p-mch)
         pid   (generate-sid! db-vol p-iri)]
     (cond-> (if (where/matched-iri? o-mch)
               (create-iri-reference-flake db-vol sid pid o-mch t m)
               (create-scalar-flake db-vol p-iri sid pid o-mch t m))
       reasoned (with-meta {:reasoned reasoned})))))

(defn insert
  [db-vol txn {:keys [t reasoned]} solution-ch]
  (let [clause    (:insert txn)
        insert-xf (comp (mapcat (partial assign-clause clause))
                        (filter where/all-matched?)
                        (map (partial build-flake db-vol t reasoned)))
        insert-ch (async/chan 2 insert-xf identity)]
    (async/pipe solution-ch insert-ch)))

(defn insert-retract
  [db-vol mdfn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2)  ; create an extra channel to multiply so
                                        ; solutions don't get dropped before we
                                        ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert db-vol mdfn tx-state insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract @db-vol mdfn tx-state fuel-tracker error-ch retract-soln-ch)]
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
  [db-vol parsed-txn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch* (async/pipe solution-ch
                                 (async/chan 2 (comp (where/with-default where/blank-solution))))]
    (cond
      (and (insert? parsed-txn)
           (retract? parsed-txn))
      (insert-retract db-vol parsed-txn tx-state fuel-tracker error-ch solution-ch*)

      (insert? parsed-txn)
      (insert db-vol parsed-txn tx-state solution-ch*)

      (retract? parsed-txn)
      (retract @db-vol parsed-txn tx-state fuel-tracker error-ch solution-ch*))))
