(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.datatype :as datatype]))

(defn iri-mapping?
  [flake]
  (= const/$xsd:anyURI (flake/p flake)))

(defn retract-triple
  [db triple t solution fuel-tracker error-ch]
  (let [retract-xf (keep (fn [f]
                           ;;do not retract the flakes which map subject ids to iris.
                           ;;they are an internal optimization, which downstream code
                           ;;(eg the commit pipeline) relies on
                           (when-not (iri-mapping? f)
                             (flake/flip-flake f t))))
        matched    (where/assign-matched-values triple solution nil)]
    (where/resolve-flake-range db fuel-tracker retract-xf error-ch matched)))

(defn retract-clause
  [db clause t solution fuel-tracker error-ch out-ch]
  (let [clause-ch  (async/to-chan! clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [triple ch]
                            (go (let [triple* (<! (where/resolve-sids db error-ch triple))]
                                  (-> db
                                      (retract-triple triple* t solution fuel-tracker error-ch)
                                      (async/pipe ch)))))
                          clause-ch)
    out-ch))

(defn retract
  [db mdfn t fuel-tracker error-ch solution-ch]
  (let [{:keys [delete]} mdfn
        retract-ch       (async/chan 2)]
    (async/pipeline-async 2
                          retract-ch
                          (fn [solution ch]
                            (retract-clause db delete t solution fuel-tracker error-ch ch))
                          solution-ch)
    retract-ch))

(defn insert-triple
  [db triple t solution error-ch]
  (go
    (try* (let [alias               (:alias db)
                [s-mch p-mch o-mch] (where/assign-matched-values triple solution nil)
                s                   (where/get-sid s-mch alias)
                p                   (where/get-sid p-mch alias)
                o                   (or (where/get-sid o-mch alias)
                                        (where/get-value o-mch))
                dt                  (where/get-datatype o-mch)]
            (when (and (some? s) (some? p) (some? o) (some? dt))
              ;; wrap created flake in a vector so the output of this function has the
              ;; same shape as the retract functions
              [(flake/create s p o dt t true nil)]))
          (catch* e
                  (log/error e "Error inserting new triple")
                  (>! error-ch e)))))

(defn insert-clause
  [db clause t solution error-ch out-ch]
  (async/pipeline-async 2
                        out-ch
                        (fn [triple ch]
                           (go (let [triple* (<! (where/resolve-sids db error-ch triple))]
                                (-> db
                                    (insert-triple triple* t solution error-ch)
                                    (async/pipe ch)))))
                        (async/to-chan! clause))
  out-ch)

(defn insert
  [db mdfn t error-ch solution-ch]
  (let [clause    (:insert mdfn)
        insert-ch (async/chan 2)]
    (async/pipeline-async 2
                          insert-ch
                          (fn [solution ch]
                            (insert-clause db clause t solution error-ch ch))
                          solution-ch)
    insert-ch))

(defn insert-retract
  [db mdfn t fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2) ; create an extra channel to multiply so
                                       ; solutions don't get dropped before we
                                       ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert db mdfn t error-ch insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract db mdfn t fuel-tracker error-ch retract-soln-ch)]
    (async/pipe solution-ch solution-ch*) ; now hook up the solution input
                                          ; after everything is wired
    (async/merge [insert-ch retract-ch])))

(defn insert?
  [mdfn]
  (or (contains? mdfn :insert)
      (contains? mdfn "insert")))

(defn retract?
  [mdfn]
  (or (contains? mdfn :delete)
      (contains? mdfn "delete")))

(defn modify
  [db mdfn t fuel-tracker error-ch solution-ch]
  (cond
    (and (insert? mdfn)
         (retract? mdfn))
    (insert-retract db mdfn t fuel-tracker error-ch solution-ch)

    (insert? mdfn)
    (insert db mdfn t error-ch solution-ch)

    (retract? mdfn)
    (retract db mdfn t fuel-tracker error-ch solution-ch)))

(defn retract-triple2
  [db triple {:keys [t]} solution fuel-tracker error-ch]
  (let [retract-flakes-ch (async/chan)]
    (go
      (try*
        (let [retract-xf (keep (fn [f]
                                 ;;do not retract the flakes which map subject ids to iris.
                                 ;;they are an internal optimization, which downstream code
                                 ;;(eg the commit pipeline) relies on
                                 (when-not (iri-mapping? f)
                                   (flake/flip-flake f t))))

              components  (->> (where/assign-matched-values triple solution nil)
                               (where/resolve-sids db error-ch)
                               (<!))]
          ;; we need to match an individual flake, so if we are missing s p or o we want to close the ch
          (if components
            (async/pipe (where/resolve-flake-range db fuel-tracker retract-xf error-ch components)
                        retract-flakes-ch)
            (async/close! retract-flakes-ch)))
        (catch* e
                (log/error e "Error retracting triple")
                (>! error-ch e))))
    retract-flakes-ch))

(defn retract-clause2
  [db clause tx-state solution fuel-tracker error-ch out-ch]
  (let [clause-ch  (async/to-chan! clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [triple ch]
                            (-> db
                                (retract-triple2 triple tx-state solution fuel-tracker error-ch)
                                (async/pipe ch)))
                          clause-ch)
    out-ch))

(defn retract2
  [db txn tx-state fuel-tracker error-ch solution-ch]
  (let [{:keys [delete]} txn
        retract-ch       (async/chan 2)]
    (async/pipeline-async 2
                          retract-ch
                          (fn [solution ch]
                            (retract-clause2 db delete tx-state solution fuel-tracker error-ch ch))
                          solution-ch)
    retract-ch))

(defn create-id-flake
  [sid iri t]
  (flake/create sid const/$xsd:anyURI iri const/$xsd:string t true nil))

(defn insert-triple2
  [db triple {:keys [t next-sid next-pid]} solution error-ch]
  (go
    (try*
      (let [[s-mch p-mch o-mch] (where/assign-matched-values triple solution nil)
            db-alias            (:alias db)

            s-iri          (where/get-iri s-mch)
            existing-sid   (or (where/get-sid s-mch db-alias)
                               (<? (dbproto/-subid db s-iri {:expand? false})))
            sid            (or existing-sid (get jld-ledger/predefined-properties s-iri) (next-sid s-iri))
            new-subj-flake (when-not existing-sid (create-id-flake sid s-iri t))

            p-iri          (where/get-iri p-mch)
            existing-pid   (or (where/get-sid p-mch db-alias) (<? (dbproto/-subid db p-iri {:expand? false})))
            pid            (or existing-pid (get jld-ledger/predefined-properties p-iri) (next-pid p-iri))
            new-pred-flake (when-not existing-pid (create-id-flake pid p-iri t))

            o-val        (where/get-value o-mch)
            ref-iri      (where/get-iri o-mch)
            m            (where/get-meta o-mch)
            dt           (where/get-datatype o-mch)
            sh-dt        (dbproto/-p-prop db :datatype p-iri)
            existing-dt  (when dt (<? (dbproto/-subid db dt {:expand? false})))
            dt-sid       (cond ref-iri      const/$xsd:anyURI
                               existing-dt  existing-dt
                               (string? dt) (or (get jld-ledger/predefined-properties dt) (next-pid dt))
                               sh-dt        sh-dt
                               :else        (datatype/infer o-val (:lang m)))
            new-dt-flake (when (and (not existing-dt) (string? dt)) (create-id-flake dt-sid dt t))

            ref?             (boolean ref-iri)
            existing-ref-sid (when ref? (or (where/get-sid o-mch db-alias)
                                            (<? (dbproto/-subid db ref-iri {:expand? false}))))
            ref-sid          (when ref? (or existing-ref-sid
                                            (get jld-ledger/predefined-properties ref-iri)
                                            (next-sid ref-iri)))
            new-ref-flake    (when (and ref? (not existing-ref-sid))
                               (create-id-flake ref-sid ref-iri t))

            ;; o needs to be a sid if it's a ref, otherwise the literal o
            o*        (if ref?
                        ref-sid
                        (datatype/coerce-value o-val dt-sid))
            obj-flake (flake/create sid pid o* dt-sid t true m)]
        (into [] (remove nil?) [new-subj-flake new-pred-flake new-dt-flake new-ref-flake obj-flake]))
      (catch* e
              (log/error e "Error inserting new triple")
              (>! error-ch e)))))

(defn insert-clause2
  [db clause tx-state solution error-ch out-ch]
  (async/pipeline-async 2
                        out-ch
                        (fn [triple ch]
                          (-> db
                              (insert-triple2 triple tx-state solution error-ch)
                              (async/pipe ch)))
                        (async/to-chan! clause))
  out-ch)

(defn insert2
  [db txn tx-state error-ch solution-ch]
  (let [clause    (:insert txn)
        insert-ch (async/chan 2)]
    (async/pipeline-async 2
                          insert-ch
                          (fn [solution ch]
                            (insert-clause2 db clause tx-state solution error-ch ch))
                          solution-ch)
    insert-ch))

(defn insert-retract2
  [db mdfn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2)  ; create an extra channel to multiply so
                                        ; solutions don't get dropped before we
                                        ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert2 db mdfn tx-state error-ch insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract2 db mdfn tx-state fuel-tracker error-ch retract-soln-ch)]
    (async/pipe solution-ch solution-ch*) ; now hook up the solution input
                                        ; after everything is wired
    (async/merge [insert-ch retract-ch])))

(defn modify2
  [db parsed-txn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch* (async/pipe solution-ch
                                 (async/chan 2 (where/with-default where/blank-solution)))]
    (cond
      (and (insert? parsed-txn)
           (retract? parsed-txn))
      (insert-retract2 db parsed-txn tx-state fuel-tracker error-ch solution-ch*)

      (insert? parsed-txn)
      (insert2 db parsed-txn tx-state error-ch solution-ch*)

      (retract? parsed-txn)
      (retract2 db parsed-txn tx-state fuel-tracker error-ch solution-ch*))))
