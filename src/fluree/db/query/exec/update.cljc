(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]
            [clojure.core.async :as async :refer [>! go]]))

(defn match-component
  [c solution]
  (if (::where/val c)
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
  [db triple t solution fuel-tracker error-ch]
  (let [retract-xf (keep (fn [f]
                           ;;do not retract the flakes which map subject ids to iris.
                           ;;they are an internal optimization, which downstream code
                           ;;(eg the commit pipeline) relies on
                           (when-not (iri-mapping? f)
                             (flake/flip-flake f t))))
        matched    (match-solution triple solution)]
    (where/resolve-flake-range db fuel-tracker retract-xf error-ch matched)))

(defn retract-clause
  [db clause t solution fuel-tracker error-ch out-ch]
  (let [clause-ch  (async/to-chan! clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [triple ch]
                            (-> db
                                (retract-triple triple t solution fuel-tracker error-ch)
                                (async/pipe ch)))
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
    (try* (let [[s-mch p-mch o-mch] (match-solution triple solution)
                s                   (::where/val s-mch)
                p                   (::where/val p-mch)
                o                   (::where/val o-mch)
                dt                  (::where/datatype o-mch)]
            (when (and s p o dt)
              (let [s* (if-not (number? s)
                         (<? (dbproto/-subid db s true))
                         s)]
                [(flake/create s* p o dt t true nil)]))) ; wrap created flake in
                                                         ; a vector so the
                                                         ; output of this
                                                         ; function has the same
                                                         ; shape as the retract
                                                         ; functions
          (catch* e
                  (log/error e "Error inserting new triple")
                  (>! error-ch e)))))

(defn insert-clause
  [db clause t solution error-ch out-ch]
  (async/pipeline-async 2
                        out-ch
                        (fn [triple ch]
                          (-> db
                              (insert-triple triple t solution error-ch)
                              (async/pipe ch)))
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
