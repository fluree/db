(ns fluree.db.track
  (:require [fluree.db.track.fuel :as fuel]
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

(defn init
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  ([]
   (init {}))
  ([{:keys [max-fuel]}]
   (-> {}
       init-time
       (init-fuel max-fuel))))

(defn track-fuel!
  [tracker error-ch]
  (when-let [fuel-tracker (:fuel tracker)]
    (fuel/track! fuel-tracker error-ch)))

(defn tally
  [tracker]
  (cond-> tracker
    (contains? tracker :time) (update :time time/tally)
    (contains? tracker :fuel) (update :fuel fuel/tally)))
