(ns fluree.db.merge.flake
  "Flake manipulation and spot-based operations for merge operations."
  (:require [clojure.set]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.merge.commit :as merge-commit]
            [fluree.db.merge.db :as merge-db]
            [fluree.db.query.range :as query-range]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn query-existing-spot-values
  "Queries the database for existing values at the given spots."
  [db spots]
  (go-try
    (let [root-db (policy/root db)]
      (try*
        (loop [existing {}
               remaining spots]
          (if-let [[s p dt] (first remaining)]
            (let [current (try*
                            (<? (query-range/index-range root-db nil :spot = [s p]
                                                         {:flake-xf (comp
                                                                     (filter #(= (flake/dt %) dt))
                                                                     (map flake/o))}))
                            (catch* e
                              (log/warn "Failed to query existing values for spot" [s p dt]
                                        "- assuming no existing values" (ex-message e))
                              []))]
              (recur (if (seq current)
                       (assoc existing [s p dt] (set current))
                       existing)
                     (rest remaining)))
            existing))
        (catch* e
          (log/warn "Failed to query existing values, assuming empty" (ex-message e))
          {})))))

(defn- apply-flakes-to-spot-map-with-commits
  "Applies a set of flakes to the spot->values accumulator map.
  Retractions remove values, assertions add values."
  [spot->values {:keys [asserted retracted]}]
  (let [spot-key (fn [f] [(flake/s f) (flake/p f) (flake/dt f)])
        ;; Apply retractions - remove values from spots
        after-retractions (reduce (fn [m f]
                                    (let [spot (spot-key f)
                                          val (flake/o f)]
                                      (update m spot (fnil disj #{}) val)))
                                  spot->values
                                  (or retracted []))
        ;; Apply additions - add values to spots
        after-assertions (reduce (fn [m f]
                                   (let [spot (spot-key f)
                                         val (flake/o f)]
                                     (update m spot (fnil conj #{}) val)))
                                 after-retractions
                                 (or asserted []))]
    after-assertions))

(defn- compute-spot-values-from-commits
  "Processes commits sequentially to compute the net spot->values map."
  [conn commits target-db]
  (go-try
    (loop [spot->values {}
           remaining commits]
      (if-let [commit (first remaining)]
        (let [flakes (<? (merge-commit/read-commit-data conn commit target-db))
              updated-spots (apply-flakes-to-spot-map-with-commits spot->values flakes)]
          (recur updated-spots (rest remaining)))
        spot->values))))

(defn- generate-flakes-from-spot-values
  "Generates flakes from spot->values map, including retractions for removed values."
  [spot->values existing-values]
  (reduce-kv
   (fn [flakes spot final-values]
     (let [existing-vals (get existing-values spot #{})
           final-vals-set (set final-values)
           to-retract (clojure.set/difference existing-vals final-vals-set)
           to-add final-vals-set
           [s p dt] spot]
       (into flakes
             (concat
              ;; Retractions for values that exist but shouldn't
              (map (fn [o] (flake/create s p o dt 0 false nil))
                   to-retract)
              ;; Assertions for final values
              (map (fn [o] (flake/create s p o dt 0 true nil))
                   to-add)))))
   []
   spot->values))

(defn compute-net-flakes
  "Computes net effect of all source commits.
  Returns [flakes updated-target-db] where updated-target-db has the necessary namespace mappings."
  [conn target-db source-commits]
  (go-try
    ;; Step 1: Ensure synchronous db
    (let [target-db-sync (<? (merge-commit/ensure-sync-db target-db))
          ;; Step 2: Collect all namespaces from source commits
          all-namespaces (<? (merge-commit/collect-commit-namespaces conn source-commits))
          ;; Step 3: Prepare target-db with namespaces
          target-db-with-ns (merge-db/prepare-target-db-namespaces target-db-sync all-namespaces)
          ;; Step 4: Process commits to compute net spot values
          spot->values (<? (compute-spot-values-from-commits conn source-commits target-db-with-ns))]

      ;; Step 5: Generate final flakes based on net changes
      (if (empty? spot->values)
        [(flake/sorted-set-by flake/cmp-flakes-spot) target-db-with-ns]
        (let [;; Query existing values for affected spots
              existing-values (<? (query-existing-spot-values target-db-with-ns (keys spot->values)))
              ;; Generate flakes including retractions and assertions
              all-flakes (generate-flakes-from-spot-values spot->values existing-values)]
          [(into (flake/sorted-set-by flake/cmp-flakes-spot) all-flakes) target-db-with-ns])))))

(defn get-changed-spots
  "Returns the set of [s p dt] coordinates that have changed in the given flakes."
  [flakes]
  (set (map (fn [f] [(flake/s f) (flake/p f) (flake/dt f)]) flakes)))

(defn reverse-commit-flakes
  "Process a single commit and return its flakes with flipped operations."
  [conn commit db-with-ns]
  (go-try
    (let [commit-flakes (<? (merge-commit/read-commit-data conn commit db-with-ns))
          all-flakes (concat (:asserted commit-flakes)
                             (:retracted commit-flakes))]
      (map flake/flip-flake all-flakes))))

(defn process-commits-to-reverse
  "Process multiple commits and collect all their reversed flakes."
  [conn commits-reversed db-with-ns]
  (go-try
    (loop [acc []
           commits commits-reversed]
      (if-let [commit (first commits)]
        (let [flipped (<? (reverse-commit-flakes conn commit db-with-ns))]
          (recur (into acc flipped) (rest commits)))
        acc))))