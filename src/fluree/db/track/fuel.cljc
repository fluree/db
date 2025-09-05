(ns fluree.db.track.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn init
  [max-fuel]
  (atom {:limit (or max-fuel 0)
         :total 0}))

(defn tally
  [fuel]
  (:total @fuel))

(defn track!
  [fuel error-ch]
  (fn [rf]
    (fn
      ([]
       (rf))

      ([result next]
       (let [{:keys [limit total]} (swap! fuel update :total inc)]
         (when (and (pos? limit) (= (inc limit) total))
           (log/error "Fuel limit of" limit "exceeded")
           (put! error-ch
                 (ex-info "Fuel limit exceeded" {:used total, :limit limit}))))
       (rf result next))

      ([result]
       (rf result)))))
