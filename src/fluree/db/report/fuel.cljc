(ns fluree.db.report.fuel
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

(defn track?
  [{:keys [max-fuel meta] :as _opts}]
  (or max-fuel
      (true? meta)
      (:fuel meta)))

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
         (when-let [limit (:limit trkr)]
           (let [tly (tally trkr)]
             (when (< 0 limit tly)
               (log/error "Fuel limit of" limit "exceeded:" tly)
               (put! error-ch
                     (ex-info "Fuel limit exceeded" {:used tly, :limit limit})))))
         (rf result next))

        ([result]
         (rf result))))))
