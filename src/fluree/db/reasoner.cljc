(ns fluree.db.reasoner
  (:require [fluree.db.track :as track]
            [fluree.db.util :as util]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Reasoner
  (-reason [reasoner methods rules-graph tracker reasoner-max])
  (-reasoned-facts [reasoner]))

(defn reason
  [db methods rule-sources {:keys [max-fuel reasoner-max]
                            :or   {reasoner-max 10} :as _opts}]
  (let [methods* (set (util/sequential methods))
        tracker  (track/init {:max-fuel max-fuel})]
    (-reason db methods* rule-sources tracker reasoner-max)))

(defn reasoned-facts
  ([db]
   (-reasoned-facts db))
  ([db grouping]
   (let [result   (reasoned-facts db)
         group-fn (case grouping
                    nil nil
                    :subject (fn [p] (nth p 0))
                    :property (fn [p] (nth p 1))
                    :rule (fn [p] (nth p 3)))]
     (if group-fn
       (group-by group-fn result)
       result))))

(defn reasoned-count
  "Returns a count of reasoned facts in the provided db."
  [db]
  (-> db reasoned-facts count))
