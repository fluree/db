(ns fluree.db.query.exec
  (:require [clojure.spec.alpha :as spec]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.query.parse :as parse]))

#?(:clj (set! *warn-on-reflection* true))

(def rdf-type-preds #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                      "a"
                      :a
                      "rdf:type"
                      :rdf/type
                      "@type"})

(defn rdf-type-pred?
  [p]
  (contains? rdf-type-preds p))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn variable->binding
  [sym]
  {::var sym})

(defn parse-constraint
  [cst]
  (mapv (fn [cmp]
          (cond-> cmp
            (variable? cmp) variable->binding))
        cst))

(defn parse-where
  [where]
  (mapv parse-constraint where))
