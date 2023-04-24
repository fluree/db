(ns fluree.db.query.exec.update
  (:require [fluree.db.flake :as flake]
            [fluree.db.query.exec.where :as where]
            [clojure.core.async :as async]))

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

(defn retract-triple
  [db triple t solution error-ch]
  (let [retract-xf (map (fn [f]
                          (flake/flip-flake f t)))
        matched    (match-solution triple solution)]
    (where/resolve-flake-range db retract-xf error-ch matched)))

(defn retract-clause
  [db clause t solution out-ch error-ch]
  (let [clause-ch  (async/to-chan! clause)]
    (async/pipeline-async 2
                          out-ch
                          (fn [triple ch]
                            (-> db
                                (retract-triple triple t solution error-ch)
                                (async/pipe ch)))
                          clause-ch)))

(defn retract
  [db mdfn t error-ch solution-ch]
  (let [{:keys [delete]} mdfn
        retract-ch       (async/chan 2)]
    (async/pipeline-async 2
                          retract-ch
                          (fn [solution ch]
                            (retract-clause db delete t solution ch error-ch))
                          solution-ch)
    retract-ch))

(defn insert-triple
  [triple t solution]
  (let [[s-mch p-mch o-mch] (match-solution triple solution)
        s                   (::where/val s-mch)
        p                   (::where/val p-mch)
        o                   (::where/val o-mch)
        dt                  (::where/datatype o-mch)]
    (when (and s p o dt)
      (flake/create s p o dt t true nil))))

(defn insert
  [mdfn t solution-ch]
  (let [{:keys [insert]} mdfn
        insert-xf        (mapcat (fn [soln]
                                   (->> insert
                                        (map (fn [triple]
                                               (insert-triple triple t soln)))
                                        (remove nil?))))]
    (async/pipe solution-ch
                (async/chan 2 insert-xf))))

(defn insert-retract
  [db mdfn t error-ch solution-ch]
  (let [solution-ch*    (async/chan 2) ; create an extra channel to multiply so
                                       ; solutions don't get dropped before we
                                       ; can add taps to process them.
        solution-mult   (async/mult solution-ch*)
        insert-soln-ch  (->> (async/chan 2)
                             (async/tap solution-mult))
        insert-ch       (insert mdfn t insert-soln-ch)
        retract-soln-ch (->> (async/chan 2)
                             (async/tap solution-mult))
        retract-ch      (retract db mdfn t error-ch retract-soln-ch)]
    (async/pipe solution-ch solution-ch*) ; now hook up the solution input
                                          ; after everything is wired
    (async/merge insert-ch retract-ch)))

(defn insert?
  [mdfn]
  (contains? mdfn :insert))

(defn retract?
  [mdfn]
  (contains? mdfn :delete))

(defn modify
  [db mdfn t error-ch solution-ch]
  (cond
    (and (insert? mdfn)
         (retract? mdfn))
    (insert-retract db mdfn t error-ch solution-ch)

    (insert? mdfn)
    (insert mdfn t solution-ch)

    (retract? mdfn)
    (retract db mdfn t error-ch solution-ch)))
