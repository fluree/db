(ns fluree.db.query.dataset
  (:refer-clojure :exclude [alias])
  (:require [fluree.db.util.core :as util]
            [fluree.db.query.exec.where :as where]
            [clojure.core.async :as async]))


(defrecord DataSet [named default active])

(defn dataset?
  [ds]
  (instance? DataSet ds))

(defn active-graph
  [ds]
  (let [active-graph (:active ds)]
    (if (#{::default} active-graph)
      (:default ds)
      (-> ds :named (get active-graph)))))

(defn activate
  [ds alias]
  (when (-> ds :named (contains? alias))
    (assoc ds :active alias)))

(defn names
  [ds]
  (-> ds :named keys))

(defn all
  [ds]
  (if (dataset? ds)
    (->> (:default ds)
         (concat (-> ds :named vals))
         (into [] (distinct)))
    [ds]))

(extend-type DataSet
  where/Matcher
  (-match-id [ds fuel-tracker solution s-mch error-ch]
    (if-let [graph (active-graph ds)]
      (if (sequential? graph)
        (->> graph
             (map (fn [db]
                    (where/-match-id db fuel-tracker solution s-mch error-ch)))
             async/merge)
        (where/-match-id graph fuel-tracker solution s-mch error-ch))
      where/nil-channel))

  (-match-triple [ds fuel-tracker solution triple error-ch]
    (if-let [graph (active-graph ds)]
      (if (sequential? graph)
        (->> graph
             (map (fn [db]
                    (where/-match-triple db fuel-tracker solution triple error-ch)))
             async/merge)
        (where/-match-triple graph fuel-tracker solution triple error-ch))
      where/nil-channel))

  (-match-class [ds fuel-tracker solution triple error-ch]
    (if-let [graph (active-graph ds)]
      (if (sequential? graph)
        (->> graph
             (map (fn [db]
                    (where/-match-class db fuel-tracker solution triple error-ch)))
             async/merge)
        (where/-match-class graph fuel-tracker solution triple error-ch))
      where/nil-channel))

  (-activate-alias [ds alias]
    (activate ds alias))

  (-aliases [ds]
    (names ds)))

(defn combine
  [named-map defaults]
  (let [default-graph (some->> defaults util/sequential)]
    (->DataSet named-map default-graph ::default)))
