(ns fluree.common.model
  (:require [malli.core :as m]
            [malli.error :as me]))

(defn valid?
  [model x]
  (m/validate model x))

(defn explain
  [model x]
  (m/explain model x))

(defn report
  [explanation]
  (me/humanize explanation))
