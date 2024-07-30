(ns fluree.db.reasoner
  (:require [fluree.db.util.core :as util]
            [fluree.db.fuel :as fuel]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol Reasoner
  (-reason [reasoner methods rules-graph fuel-tracker reasoner-max])
  (-reasoned-facts [reasoner]))

(defn deduplicate-raw-rules
  [raw-rules]
  (let [rule-ids (map first raw-rules)
        duplicate-ids (filter #(< 1 (last %)) (frequencies rule-ids))]
    (reduce (fn [rules [duplicate-id occurances]]
              (let [grouped-rules (group-by #(= duplicate-id (first %)) rules)]
                (loop [suffix occurances
                       rules-to-update (get grouped-rules true)
                       updated-rules-list (get grouped-rules false)]
                  (if (empty? rules-to-update)
                    updated-rules-list
                    (let [updated-rule [(str duplicate-id suffix) (last (first rules-to-update))]]
                      (recur (dec suffix) (rest rules-to-update) (conj updated-rules-list updated-rule)))))))
            raw-rules duplicate-ids)))

(defn reason
  [db methods rule-sources {:keys [max-fuel reasoner-max]
                            :or   {reasoner-max 10} :as _opts}]
  (let [methods*        (set (util/sequential methods))
        fuel-tracker    (fuel/tracker max-fuel)]
    (-reason db methods* rule-sources fuel-tracker reasoner-max)))


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
