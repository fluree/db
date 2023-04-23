(ns fluree.db.query.exec.delete
  (:require [fluree.db.flake :as flake]
            [fluree.db.query.exec.where :as where]
            [clojure.core.async :as async]))

(defn retract
  [db q t error-ch solution-ch]
  (let [{:keys [delete]} q
        [s p o]          delete
        retract-xf       (map (fn [f]
                                (flake/flip-flake f t)))
        retract-ch       (async/chan 2)]
    (async/pipeline-async 1
                          retract-ch
                          (fn [solution ch]
                            (let [s* (if (::where/val s)
                                       s
                                       (get solution (::where/var s)))
                                  p* (if (::where/val p)
                                       p
                                       (get solution (::where/var p)))
                                  o* (if (::where/val o)
                                       o
                                       (get solution (::where/var o)))]
                              (-> db
                                  (where/resolve-flake-range retract-xf error-ch [s* p* o*])
                                  (async/pipe ch))))
                          solution-ch)
    retract-ch))
