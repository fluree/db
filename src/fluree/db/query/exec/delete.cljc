(ns fluree.db.query.exec.delete
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
