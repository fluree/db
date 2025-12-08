(ns fluree.db.track.policy)

(defn init
  "Map of `<policy-id>->{:executed <count> :allowed <count>}`, where `:executed` is the
  number of times a policy is executed on a flake and `:allowed` is the number of times
  it grants access to a flake."
  []
  (atom {}))

(defn track-exec!
  [tracker policy-id]
  (when policy-id
    (swap! tracker update-in [policy-id :executed] (fnil inc 0))))

(defn track-allow!
  [tracker policy-id]
  (when policy-id
    (swap! tracker update-in [policy-id :allowed] (fnil inc 0))))

(defn tally
  [tracker]
  @tracker)
