(ns fluree.db.track.time
  (:require [fluree.db.util.core :as util]))

(defn init
  []
  {:start #?(:clj (System/nanoTime)
             :cljs (util/current-time-millis))})

(defn tally
  [{:keys [start]}]
  (util/response-time-formatted start))
