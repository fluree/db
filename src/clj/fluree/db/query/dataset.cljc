(ns fluree.db.query.dataset
  (:refer-clojure :exclude [alias])
  (:require [fluree.db.util.core :as util]))


(defrecord DataSet [named default active])

(defn combine
  [named-map defaults]
  (let [default-graph (util/sequential defaults)]
    (->DataSet named-map default-graph ::default)))

(defn dataset?
  [ds]
  (instance? DataSet ds))

(defn activate
  [ds alias]
  (when (-> ds :named (contains? alias))
    (assoc ds :active alias)))

(defn activate-default
  [ds]
  (assoc ds :active ::default))

(defn active
  [ds]
  (if (dataset? ds)
    (let [active-graph (:active ds)]
      (if (#{::default} active-graph)
        (:default ds)
        (-> ds :named (get active-graph))))
    ds))

(defn all
  [ds]
  (if (dataset? ds)
    (-> ds
        :default
        (concat (-> ds :named vals)))
    [ds]))

(defn names
  [ds]
  (-> ds :named keys))
