(ns fluree.db.query.dataset
  (:refer-clojure :exclude [alias])
  (:require [fluree.db.util.core :as util]))


(defrecord DataSet [db-map active])

(defn combine
  [named-map defaults]
  (let [default-graph (util/sequential defaults)]
    (-> named-map
        (assoc ::default default-graph)
        (->DataSet ::default))))

(defn dataset?
  [ds]
  (instance? DataSet ds))

(defn activate
  [ds alias]
  (when (-> ds :names (contains? alias))
    (assoc ds :active alias)))

(defn activate-default
  [ds]
  (assoc ds :active ::default))

(defn active
  [ds]
  (let [active-graph (:active ds)]
    (-> ds :db-map (get active-graph))))
