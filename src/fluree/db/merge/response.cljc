(ns fluree.db.merge.response
  "Response generation utilities for merge operations."
  (:require [fluree.db.util.log :as log]))

(defn conflict-response
  "Creates a conflict response for a failed merge/rebase operation."
  [from to failed-commit opts]
  {:status :conflict
   :operation :merge
   :from from
   :to to
   :error :db/merge-conflict
   :strategy (cond
               (:ff-mode opts) "fast-forward"
               (:squash? opts) "squash"
               :else "replay")
   :commits {:failed failed-commit
             :conflicts [{:commit failed-commit
                          :message "Conflict detected during rebase"}]}})

(defn success-response
  "Creates a success response for a completed merge/rebase operation."
  [from to replay-result new-commit-sha opts]
  {:status :success
   :operation :merge
   :from from
   :to to
   :strategy (cond
               (:ff-mode opts) "fast-forward"
               (:squash? opts) "squash"
               :else "replay")
   :commits {:replayed (:replayed replay-result)
             :new (when new-commit-sha
                    {:id new-commit-sha})}})

(defn generate-reset-message
  "Generate a descriptive message for the reset operation."
  [state-spec opts]
  (or (:message opts)
      (str "Reset branch to "
           (if (:sha state-spec)
             (str "commit " (:sha state-spec))
             (str "t=" (:t state-spec))))))

(defn filter-commits-to-undo
  "Filter commits that need to be undone (those after target-t)."
  [all-commits target-t]
  (let [commits-to-undo (filter #(> (get-in % [:data :t]) target-t) all-commits)]
    (log/info "filter-commits-to-undo: target-t=" target-t
              "total-commits=" (count all-commits)
              "commits-to-undo=" (count commits-to-undo)
              "undo-ts=" (map #(get-in % [:data :t]) commits-to-undo))
    commits-to-undo))

(defn create-reset-result
  "Create the result map for a reset operation."
  [branch-spec state-spec current-t target-t commits-undone new-commit-sha]
  {:status :success
   :operation :reset
   :branch branch-spec
   :mode :safe
   :reset-to (or (:sha state-spec) (str "t=" (:t state-spec)))
   :from-t current-t
   :to-t target-t
   :commits-undone commits-undone
   :new-commit new-commit-sha})