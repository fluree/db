(ns fluree.db.query.fql.serialize
  (:require [fluree.db.query.exec.eval :as eval]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.select :as select]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.fql.syntax :as syntax]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.walk :refer [postwalk]]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.validation :as v]
            [fluree.db.constants :as const]
            #?(:cljs [cljs.reader :refer [read-string]])))

#?(:clj (set! *warn-on-reflection* true))

(defn safe-compare
  [x y]
  (let [cmp (compare x y)]
    (when-not (= 0 cmp)
      cmp)))

(defn- serialize-record
  [record]
  (let [c #?(:clj (.getName (class record))
             :cljs (type->str record))]
    (tagged-literal `SortedRecord [c (into (sorted-map) record)])))

(defn- serialize-query
  [parsed-query]
  (binding [*print-length* nil
            *print-level* nil
            *print-namespace-maps* nil]
    (pr-str (postwalk (fn [x]
                        (cond
                          (record? x) (serialize-record x)
                          (map? x) (into (sorted-map) x)
                          :else x))
                      parsed-query))))


(defn compare-wheres
  [where1 where2]
  (let [w1-pats (::where/patterns where1)
        w2-pats (::where/patterns where2)]
    (or (safe-compare (count w1-pats) (count w2-pats))
        (let [w1-filters (::where/filters where1)
              w2-filters (::where/filters where2)]
          (or (safe-compare (count w1-filters) (count w2-filters))
              (let [w1-by-type (group-by where/pattern-type w1-pats)
                    w2-by-type (group-by where/pattern-type w2-pats)]
                (or (safe-compare (count (:tuple w1-by-type))
                                  (count (:tuple w2-by-type)))
                    (safe-compare (count (:class w1-by-type))
                                  (count (:class w2-by-type)))
                    (safe-compare (count (:optional w1-by-type))
                                  (count (:optional w2-by-type)))
                    (safe-compare (count (:union w1-by-type))
                                  (count (:union w2-by-type))))))))))

(defn get-select-key
  [q]
  (cond
    (contains? q :select) :select
    (contains? q :select-one) :select-one
    (contains? q :select-distinct) :select-distinct))

(defn score-select-key
  [select-key]
  (case select-key
    :select 0
    :select-one 1
    :select-distinct 2))

(defn compare-by-existence
  [k1 k2]
  (let [k1-exists? (some? k1)
        k2-exists? (some? k2)]
    (cond
      (and k1-exists?
           (not k2-exists?)) -1
      (and k2-exists?
           (not k1-exists?)) 1)))

(defrecord SerializedQuery [select select-one select-distinct
                            where order-by group-by having
                            context t values]
  #?(:clj  java.lang.Comparable
     :cljs IComparable)
  (#?(:clj compareTo
      :cljs -compare)
    [q1 q2]
    (if (= q1 q2)
      0
      (or (safe-compare (count q1) (count q2))
          (safe-compare t (:t q2))
          (compare-by-existence having (:having q2))
          (compare-by-existence group-by (:group-by q2))
          (compare-by-existence order-by (:order-by q2))
          (compare-by-existence values (:values q2))
          (let [q1-select-key (get-select-key q1)
                q2-select-key (get-select-key q2)]
            (or (safe-compare (score-select-key q1-select-key) (score-select-key q2-select-key))
                (let [q1-select (get q1 q1-select-key)
                      q2-select (get q2 q2-select-key)]
                  (or (safe-compare (count q1-select) (count q2-select))
                      (compare-wheres where (:where q2))
                      (compare (serialize-query q1) (serialize-query q2))))))))))
