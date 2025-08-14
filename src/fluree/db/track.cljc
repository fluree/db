(ns fluree.db.track
  (:require [fluree.db.track.fuel :as fuel]
            [fluree.db.track.policy :as policy]
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
      (track-policy? opts)
      (track-solutions? opts)))

(defn track-txn?
  [opts]
  (or (track-time? opts)
      (track-fuel? opts)
      (track-file? opts)
      (track-policy? opts)
      (track-solutions? opts)))

(defn init-time
  [tracker]
  (assoc tracker :time (time/init)))

(defn init-fuel
  [tracker max-fuel]
  (assoc tracker :fuel (fuel/init max-fuel)))

(defn init-policy
  [tracker]
  (assoc tracker :policy (policy/init)))

(defn init-explain
  [tracker]
  (assoc tracker :explain (solutions/init)))

(defn init
  "Creates a new fuel tracker w/ optional fuel limit (0 means unlimited)."
  [{:keys [max-fuel] :as opts}]
  (cond-> {}
    (track-time? opts) init-time
    (track-fuel? opts) (init-fuel max-fuel)
    (track-policy? opts) init-policy
    (track-solutions? opts) init-explain))

(defn pattern-in!
  [tracker pattern solution]
  (when-let [solution-tracker (:explain tracker)]
    (solutions/pattern-in! solution-tracker pattern solution)))

(defn pattern-out!
  [tracker pattern solution]
  (when-let [solution-tracker (:explain tracker)]
    (solutions/pattern-out! solution-tracker pattern solution)))

(defn track-fuel!
  [tracker error-ch]
  (when-let [fuel-tracker (:fuel tracker)]
    (fuel/track! fuel-tracker error-ch)))

(defn register-policies!
  [tracker policy-db]
  (when-let [policy-tracker (:policy tracker)]
    (policy/register-policies! policy-tracker policy-db)))

(defn policy-exec!
  [tracker policy-id]
  (when-let [policy-tracker (:policy tracker)]
    (policy/track-exec! policy-tracker policy-id)))

(defn policy-allow!
  [tracker policy-id]
  (when-let [policy-tracker (:policy tracker)]
    (policy/track-allow! policy-tracker policy-id)))

(defn tally
  [tracker]
  (cond-> tracker
    (contains? tracker :time) (update :time time/tally)
    (contains? tracker :fuel) (update :fuel fuel/tally)
    (contains? tracker :policy) (update :policy policy/tally)
    (contains? tracker :explain) (update :explain solutions/tally)))
