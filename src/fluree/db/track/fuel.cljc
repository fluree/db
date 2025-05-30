(ns fluree.db.track.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn init
  [max-fuel]
  (atom {:limit    (or max-fuel 0)
         :counters []}))

(defn tally
  [fuel]
  (let [{:keys [counters]} @fuel]
    (reduce (fn [total ctr]
              (+ total @ctr))
            0 counters)))

(defn with-counter
  [fuel-state counter]
  (update fuel-state :counters conj counter))

(defn track!
  [fuel error-ch]
  (fn [rf]
    (let [limit   (:limit @fuel)
          counter (volatile! 0)]
      (swap! fuel with-counter counter)
      (fn
        ([]
         (rf))

        ([result next]
         (vswap! counter inc)
         (when limit
           (let [tly (tally fuel)]
             (when (< 0 limit tly)
               (log/error "Fuel limit of" limit "exceeded:" tly)
               (put! error-ch
                     (ex-info "Fuel limit exceeded" {:used tly, :limit limit})))))
         (rf result next))

        ([result]
         (rf result))))))
