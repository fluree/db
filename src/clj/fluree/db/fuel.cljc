(ns fluree.db.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]))

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
         (vswap! counter inc)
         (let [tly   (tally trkr)
               limit (:limit trkr)]
           (when (< 0 limit tly)
             (log/error "Fuel limit of" limit "exceeded:" tly)
             (put! error-ch
                   (ex-info "Fuel limit exceeded" {:used tly, :limit limit})))
           (rf result next)))

        ([result]
         (rf result))))))

(defn track?
  [opts]
  (or (:max-fuel opts)
      (:meta opts)))
