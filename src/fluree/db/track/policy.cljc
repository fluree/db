(ns fluree.db.track.policy
  (:require [clojure.walk :as walk]))

(defn register-policies!
  [tracker policy-db]
  (reset! tracker (reduce (fn [state policy-id]
                            (assoc state policy-id {:executed 0 :allowed 0}))
                          {}
                          (concat (->> policy-db :policy :view :class (vals) (mapv :id))
                                  (->> policy-db :policy :view :property (vals) (mapv :id))
                                  (->> policy-db :policy :view :default (mapv :id))

                                  (->> policy-db :policy :modify :class (vals) (mapv :id))
                                  (->> policy-db :policy :modify :property (vals) (mapv :id))
                                  (->> policy-db :policy :modify :default (mapv :id))))))



(defn init
  "Map of `<policy-id>->{:executed <count> :allowed <count>}`, where `:executed` is the
  number of times a policy is executed on a flake and `:allowed` is the number of times
  it grants access to a flake."
  []
  (atom {}))

(defn track-exec!
  [tracker policy-id]
  (swap! tracker update-in [policy-id :executed] inc))

(defn track-allow!
  [tracker policy-id]
  (swap! tracker update-in [policy-id :allowed] inc))

(defn tally
  [tracker]
  @tracker)
