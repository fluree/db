(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async :refer [>! go]]
            [clojure.string :as str]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.datatype :as datatype]))

(defn match-component
  [c solution]
  (if (some? (::where/val c))
    c
    (get solution (::where/var c))))

(defn match-solution
  [pattern solution]
  (mapv (fn [c]
          (match-component c solution))
        pattern))

(defn iri-mapping?
  [flake]
  (= const/$xsd:anyURI (flake/p flake)))

(defn retract-triple
  [db triple {:keys [t]} solution fuel-tracker error-ch]
  (let [retract-xf (keep (fn [f]
                           ;;do not retract the flakes which map subject ids to iris.
                           ;;they are an internal optimization, which downstream code
                           ;;(eg the commit pipeline) relies on
                           (when-not (iri-mapping? f)
                             (flake/flip-flake f t))))
        matched    (match-solution triple solution)]
    (where/resolve-flake-range db fuel-tracker retract-xf error-ch matched)))

(defn retract-clause
  [db clause tx-state solution fuel-tracker error-ch out-ch]
  (let [clause-ch  (async/to-chan! clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [triple ch]
                            (-> db
                                (retract-triple triple tx-state solution fuel-tracker error-ch)
                                (async/pipe ch)))
                          clause-ch)
    out-ch))

(defn retract
  [db mdfn tx-state fuel-tracker error-ch solution-ch]
  (let [{:keys [delete]} mdfn
        retract-ch       (async/chan 2)]
    (async/pipeline-async 2
                          retract-ch
                          (fn [solution ch]
                            (retract-clause db delete tx-state solution fuel-tracker error-ch ch))
                          solution-ch)
    retract-ch))

(defn insert-triple
  [db triple {:keys [t next-sid next-pid]} solution error-ch]
  (go
    (try* (let [[s-mch p-mch o-mch] (match-solution triple solution)
                s                   (::where/val s-mch)
                p                   (::where/val p-mch)
                o                   (::where/val o-mch)
                dt                  (::where/datatype o-mch)]
            (when (and (some? s) (some? p) (some? o) (some? dt))
              (let [s* (if-not (number? s)
                         (<? (dbproto/-subid db s true))
                         s)]
                ;; wrap created flake in a vector so the output of this function has the
                ;; same shape as the retract functions
                [(flake/create s* p o dt t true nil)])))
          (catch* e
                  (log/error e "Error inserting new triple")
                  (>! error-ch e)))))

(defn insert-clause
  [db clause tx-state solution error-ch out-ch]
  (async/pipeline-async 2
                        out-ch
                        (fn [triple ch]
                          (-> db
                              (insert-triple triple tx-state solution error-ch)
                              (async/pipe ch)))
                        (async/to-chan! clause))
  out-ch)

(defn insert
  [db mdfn tx-state error-ch solution-ch]
  (let [clause    (:insert mdfn)
        insert-ch (async/chan 2)]
    (async/pipeline-async 2
                          insert-ch
                          (fn [solution ch]
                            (insert-clause db clause tx-state solution error-ch ch))
                          solution-ch)
    insert-ch))

(defn insert-retract
  [db mdfn tx-state fuel-tracker error-ch solution-ch]
  (let [solution-ch*    (async/chan 2) ; create an extra channel to multiply so
                                       ; solutions don't get dropped before we
                                       ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert db mdfn tx-state error-ch insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract db mdfn tx-state fuel-tracker error-ch retract-soln-ch)]
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
  [db mdfn tx-state fuel-tracker error-ch solution-ch]
  (cond
    (and (insert? mdfn)
         (retract? mdfn))
    (insert-retract db mdfn tx-state fuel-tracker error-ch solution-ch)

    (insert? mdfn)
    (insert db mdfn tx-state error-ch solution-ch)

    (retract? mdfn)
    (retract db mdfn tx-state fuel-tracker error-ch solution-ch)))

(defn retract-triple2
  [db triple {:keys [t]} solution fuel-tracker error-ch]
  (go
    (try*
      (let [retract-xf (keep (fn [f]
                               ;;do not retract the flakes which map subject ids to iris.
                               ;;they are an internal optimization, which downstream code
                               ;;(eg the commit pipeline) relies on
                               (when-not (iri-mapping? f)
                                 (flake/flip-flake f t))))

            [s-mch p-mch o-mch] (match-solution triple solution)

            s-cmp  (if (string? (::where/val s-mch))
                     (assoc s-mch ::where/val (<? (dbproto/-subid db (::where/val s-mch))))
                     s-mch)

            p-cmp  (if (string? (::where/val p-mch))
                     (assoc p-mch ::where/val (<? (dbproto/-subid db (::where/val p-mch))))
                     p-mch)
            o-cmp  (if (and (= const/$xsd:anyURI (::where/val o-mch))
                            (string? (::where/val o-mch)))
                     (assoc o-mch ::where/val (<? (dbproto/-subid db (::where/val o-mch))))
                     o-mch)]
        (<? (where/resolve-flake-range db fuel-tracker retract-xf error-ch [s-cmp p-cmp o-cmp])))
      (catch* e
              (log/error e "Error retracting triple")
              (>! error-ch e)))))

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

(defn fdb-bnode?
  "Is the iri a fluree-generated temporary bnode?"
  [iri]
  (str/starts-with? iri "_:fdb"))

(defn bnode-id
  "A stable bnode."
  [sid]
  (str "_:" sid))

(defn create-id-flake
  [sid iri t]
  (flake/create sid const/$xsd:anyURI iri const/$xsd:string t true nil))

(defn insert-triple2
  [db triple {:keys [t next-sid next-pid]} solution error-ch]
  (go
    (try* (let [[s-mch p-mch o-mch] (match-solution triple solution)
                s                   (::where/val s-mch)
                p                   (::where/val p-mch)
                o                   (::where/val o-mch)
                dt                  (::where/datatype o-mch)
                m                   (::where/m o-mch)]

            (when (and (some? s) (some? p) (some? o) (some? dt))
              (let [existing-sid   (<? (dbproto/-subid db s))
                    [sid s-iri]    (if (fdb-bnode? s)
                                     (let [bnode-sid (next-sid s)]
                                       [bnode-sid (bnode-id bnode-sid)])
                                     [(or existing-sid (get jld-ledger/predefined-properties s) (next-sid s)) s])
                    new-subj-flake (when-not existing-sid (create-id-flake sid s-iri t))

                    existing-pid   (<? (dbproto/-subid db p))
                    pid            (or existing-pid (get jld-ledger/predefined-properties p) (next-pid p))
                    new-pred-flake (when-not existing-pid (create-id-flake pid p t))

                    ;; subid works for sids
                    existing-dt   (<? (dbproto/-subid db dt))
                    dt-sid        (cond existing-dt existing-dt
                                        (string? dt) (next-sid dt)
                                        :else (datatype/infer o (:lang m)))
                    new-dt-flake  (when (and (not existing-dt) (string? dt)) (create-id-flake dt-sid dt t))

                    ref?             (= const/$xsd:anyURI dt)
                    existing-ref-sid (when ref? (<? (dbproto/-subid db o)))
                    ref-sid          (when (and (= const/$xsd:anyURI dt)
                                                (not existing-ref-sid))
                                       (next-sid o))
                    new-ref-flake    (when (and (not existing-ref-sid) (= const/$xsd:anyURI dt))
                                       (create-id-flake (next-sid o) o t))

                    ;; o needs to be a sid if it's a ref, otherwise the literal o
                    obj-flake  (flake/create sid pid (if ref? ref-sid o) dt-sid t true m)]

                (into [] (remove nil?) [new-subj-flake new-pred-flake new-dt-flake new-ref-flake obj-flake]))))
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
                            (println "DEP insert solution" (pr-str solution))
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
  (cond
    (and (insert? parsed-txn)
         (retract? parsed-txn))
    (insert-retract2 db parsed-txn tx-state fuel-tracker error-ch solution-ch)

    (insert? parsed-txn)
    (insert2 db parsed-txn tx-state error-ch solution-ch)

    (retract? parsed-txn)
    (retract db parsed-txn tx-state fuel-tracker error-ch solution-ch)))
