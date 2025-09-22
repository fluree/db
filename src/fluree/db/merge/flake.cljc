(ns fluree.db.merge.flake
  "Flake manipulation and spot-based operations for merge operations."
  (:require [fluree.db.flake :as flake]
            [fluree.db.merge.commit :as merge-commit]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn- cancel-opposite-operations
  "Processes flakes in order to determine net effect.
  For each [s p o dt m], replays operations to see if value ends up asserted or retracted.
  Returns a collection of flakes representing the net changes."
  [all-flakes]
  (let [;; Group flakes by [s p o dt m] - include meta for ordered lists
        flake-key (fn [f] [(flake/s f) (flake/p f) (flake/o f) (flake/dt f) (flake/m f)])
        grouped (group-by flake-key all-flakes)]
    (reduce-kv
     (fn [acc spot-key flakes-at-spot]
       (let [;; Count assertions and retractions
             ops (map flake/op flakes-at-spot)
             assert-count (count (filter true? ops))
             retract-count (count (filter false? ops))
             ;; Net effect: assertions minus retractions
             net-effect (- assert-count retract-count)]
         (cond
           (pos? net-effect) ; More assertions than retractions
           (let [[s p o dt m] spot-key]
             (conj acc (flake/create s p o dt 0 true m)))

           (neg? net-effect) ; More retractions than assertions
           (let [[s p o dt m] spot-key]
             (conj acc (flake/create s p o dt 0 false m)))

           ;; Zero net effect - cancelled out, don't add anything
           :else acc)))
     []
     grouped)))

(defn- collect-all-flakes
  "Collects all flakes from source commits, threading updated namespace state."
  [conn source-commits target-db]
  (go-try
    (loop [all-flakes []
           cur-db     target-db
           remaining  source-commits]
      (if-let [commit (first remaining)]
        (let [{:keys [asserted retracted db]} (<? (merge-commit/read-commit-data conn commit cur-db))
              commit-flakes (concat asserted retracted)
              ;; Debug: Check for null subjects
              null-flakes (filter #(nil? (flake/s %)) commit-flakes)]
          (when (seq null-flakes)
            (log/error "collect-all-flakes: Found" (count null-flakes) "flakes with null subjects"
                       "at t=" (get-in commit [:data :t])
                       "first null flake:" (first null-flakes)))
          (recur (into all-flakes commit-flakes) db (rest remaining)))
        [all-flakes cur-db]))))

(defn compute-net-flakes
  "Computes net effect of all source commits by collecting flakes and cancelling opposites.
  Returns [flakes updated-target-db] where updated-target-db has the necessary namespace mappings."
  [conn target-db source-commits]
  (go-try
    ;; Step 1: Ensure synchronous db
    (let [target-db-sync (<? (merge-commit/ensure-sync-db target-db))
          ;; Collect all flakes from commits, threading updated db via dynamic codec
          [all-flakes target-db-ready] (<? (collect-all-flakes conn source-commits target-db-sync))
          ;; Step 5: Cancel out opposite operations (assert/retract pairs)
          net-flakes (cancel-opposite-operations all-flakes)]

      (log/info "compute-net-flakes: collected" (count all-flakes) "flakes, net" (count net-flakes) "after cancellation")

      [(into (flake/sorted-set-by flake/cmp-flakes-spot) net-flakes) target-db-ready])))

(defn get-changed-spots
  "Returns the set of [s p dt] coordinates that have changed in the given flakes."
  [flakes]
  (set (map (fn [f] [(flake/s f) (flake/p f) (flake/dt f)]) flakes)))

(defn reverse-commit-flakes
  "Process a single commit and return its flakes with flipped operations."
  [conn commit db-with-ns]
  (go-try
    (let [{:keys [asserted retracted]} (<? (merge-commit/read-commit-data conn commit db-with-ns))
          all-flakes (concat asserted retracted)]
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