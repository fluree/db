(ns fluree.db.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]))

#?(:clj (set! *warn-on-reflection* true))

(defn tracker
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  ([] (tracker nil))
  ([limit]
   {:limit    (or limit 0)
    :counters (atom [])}))

(defn tally
  [trkr]
  (reduce (fn [total ctr]
            (+ total @ctr))
          0 @(:counters trkr)))

(defn track
  [trkr error-ch]
  (fn [rf]
    (let [counter (volatile! 0)]
      (swap! (:counters trkr) conj counter)
      (fn
        ([]
         (rf))

        ([result next]
         (if (flake/flake? next)
           (vswap! counter inc)
           (vswap! counter + (count (remove result next))))
         (let [t     (tally trkr)
               limit (:limit trkr)]
           (when (and (> limit 0) (> t limit))
             (log/trace "Fuel limit of" limit "exceeded:" t)
             (put! error-ch
                   (ex-info "Fuel limit exceeded" {:used t, :limit limit})))
           (rf result next)))

        ([result]
         (rf result))))))
