(ns fluree.db.track)

(defn track-all?
  [{:keys [meta] :as _opts}]
  (true? meta))

(defn track-fuel?
  [{:keys [max-fuel meta] :as opts}]
  (or max-fuel
      (track-all? opts)
      (:fuel meta)))

(defn track-file?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (:file meta)))

(defn track-policy?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (:policy meta)))

(defn track?
  [opts]
  (or (track-fuel? opts)
      (track-file? opts)
      (track-policy? opts)))
