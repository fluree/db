(ns fluree.db.track
  (:require [fluree.db.track.fuel :as fuel]))

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

(defn tracker
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  ([]
   (tracker {:fuel {:limit 0}}))
  ([{:keys [fuel]}]
   (let [{:keys [limit]} fuel]
     {:fuel {:limit    limit
             :counters (atom [])}})))

(defn tally
  [trkr]
  (update trkr :fuel fuel/tally))
