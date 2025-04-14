(ns fluree.db.track)

(defn track-all?
  [{:keys [meta] :as _opts}]
  (true? meta))

(defn track-file?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (:file meta)))

(defn track-fuel?
  [{:keys [max-fuel meta] :as _opts}]
  (or max-fuel
      (true? meta)
      (:fuel meta)))

(defn track-policy?
  [{:keys [meta] :as opts}]
  (or (track-all? opts)
      (:policy meta)))
