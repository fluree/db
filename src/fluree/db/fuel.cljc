(ns fluree.db.fuel)

#?(:clj (set! *warn-on-reflection* true))

(defn tracker
  []
  (atom []))

(defn track
  [trkr]
  (fn [rf]
    (let [counter (volatile! 0)]
      (swap! trkr conj counter)
      (fn
        ([]
         (rf))

        ([result next]
         (vswap! counter inc)
         (rf result next))

        ([result]
         (rf result))))))

(defn tally
  [trkr result]
  (let [total (reduce (fn [total ctr]
                        (+ total @ctr))
                      0 @trkr)]
    (assoc result ::total total)))
