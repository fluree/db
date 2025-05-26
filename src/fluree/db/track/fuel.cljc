(ns fluree.db.track.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn init
  [max-fuel]
  {:limit    (or max-fuel 0)
   :counters (atom [])})

(defn tally
  [{:keys [counters]}]
  (reduce (fn [total ctr]
            (+ total @ctr))
          0 @counters))

(defn track!
  [fuel error-ch]
  (fn [rf]
    (let [counter (volatile! 0)]
      (swap! (:counters fuel) conj counter)
      (fn
        ([]
         (rf))

        ([result next]
         (vswap! counter inc)
         (when-let [limit (:limit fuel)]
           (let [tly (tally fuel)]
             (when (< 0 limit tly)
               (log/error "Fuel limit of" limit "exceeded:" tly)
               (put! error-ch
                     (ex-info "Fuel limit exceeded" {:used tly, :limit limit})))))
         (rf result next))

        ([result]
         (rf result))))))
