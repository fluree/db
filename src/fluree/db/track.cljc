(ns fluree.db.track
  (:require [fluree.db.track.fuel :as fuel]
            [fluree.db.track.solutions :as solutions]
            [fluree.db.track.time :as time]))

#?(:clj (set! *warn-on-reflection* true))

(defn track-all?
  [{:keys [meta] :as _opts}]
  (true? meta))

(defn track-time?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (-> meta :time true?)))

(defn track-fuel?
  [{:keys [max-fuel meta] :as opts}]
  (or max-fuel
      (track-all? opts)
      (-> meta :fuel true?)))

(defn track-file?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (-> meta :file true?)))

(defn track-policy?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (-> meta :policy true?)))

(defn track-solutions?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (:explain meta)))

(defn track-query?
  [opts]
  (or (track-time? opts)
      (track-fuel? opts)
      (track-policy? opts)))

(defn track-txn?
  [opts]
  (or (track-time? opts)
      (track-fuel? opts)
      (track-file? opts)
      (track-policy? opts)))

(defn init-time
  [tracker]
  (assoc tracker :time (time/init)))

(defn init-fuel
  [tracker max-fuel]
  (assoc tracker :fuel (fuel/init max-fuel)))

(defn init-explain
  [tracker]
  (assoc tracker :explain (solutions/init)))

(defn init
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  ([]
   (init {}))
  ([{:keys [max-fuel] :as opts}]
   (cond-> {}
     (track-time? opts) init-time
     (track-fuel? opts) (init-fuel max-fuel)
     (track-solutions? opts) init-explain)))

(defn pattern-in!
  [tracker pattern]
  (when-let [solution-tracker (:explain tracker)]
    (solutions/pattern-in! solution-tracker pattern)))

(defn pattern-out!
  [tracker pattern]
  (when-let [solution-tracker (:explain tracker)]
    (solutions/pattern-out! solution-tracker pattern)))

(defn track-fuel!
  [tracker error-ch]
  (when-let [fuel-tracker (:fuel tracker)]
    (fuel/track! fuel-tracker error-ch)))

(defn tally
  [tracker]
  (cond-> tracker
    (contains? tracker :time) (update :time time/tally)
    (contains? tracker :fuel) (update :fuel fuel/tally)
    (contains? tracker :explain) (update :explain solutions/tally)))
