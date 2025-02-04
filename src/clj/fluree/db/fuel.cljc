(ns fluree.db.fuel
  (:require [clojure.core.async :as async :refer [put!]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn tracker
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  ([] (tracker nil))
  ([limit]
   {:ranges  (atom {})
    :limit    (or limit 0)}))

(defn tally
  [trkr]
  (reduce (fn [total ctr]
            (+ total @ctr))
          0 (vals @(:ranges trkr))))

(defn track
  ([trkr error-ch]
   (track trkr nil nil nil error-ch))
  ([trkr idx start-flake end-flake error-ch]
   (fn [rf]
     (let [counter (volatile! 0)]
       (swap! (:ranges trkr) assoc [idx start-flake end-flake] counter)
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
          (rf result)))))))
