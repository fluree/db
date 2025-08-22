(ns fluree.db.merge
  "Public API for merge, rebase, and reset operations on Fluree database branches."
  (:require [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.merge.branch :as merge-branch]
            [fluree.db.merge.graph :as merge-graph]
            [fluree.db.merge.operations :as ops]
            [fluree.db.util.async :refer [<? go-try]]))

(defn merge!
  "Merges commits from source branch into target branch.
  
  Updates the target branch with changes from source branch.
  Supports fast-forward, squash, and regular merge modes.
  
  Parameters:
    conn - Connection object
    from - Source branch spec (e.g., 'ledger:feature')
    to - Target branch spec (e.g., 'ledger:main')
    opts - Merge options:
      :ff - Fast-forward behavior (default :auto)
        :auto - Fast-forward when possible
        :only - Only allow fast-forward
        :never - Never fast-forward
      :squash? - Combine all commits into one (default false)
      :preview? - Dry run without changes (default false)
      
  Returns promise resolving to merge result with anomalies report."
  [conn from to opts]
  (go-try
    (merge-branch/validate-same-ledger! from to)
    (let [{:keys [ff squash? preview?]
           :or {ff :auto
                squash? false}} opts]

      ;; Load branches and check for fast-forward
      (let [{:keys [source-db target-db source-branch-info target-branch-info]}
            (<? (merge-branch/load-branches conn from to))

            can-ff? (<? (merge-branch/can-fast-forward? conn source-db target-db
                                                        source-branch-info target-branch-info))]

        (when preview?
          (reduced
           {:status :preview
            :operation :merge
            :from from
            :to to
            :can-fast-forward can-ff?
            :strategy (cond
                        (and can-ff? (not= ff :never)) "fast-forward"
                        squash? "squash"
                        :else "merge")}))

        (cond
          ;; Fast-forward only mode - fail if not possible
          (and (= ff :only) (not can-ff?))
          {:status :error
           :operation :merge
           :from from
           :to to
           :error :db/cannot-fast-forward
           :message "Fast-forward not possible - branches have diverged"}

          ;; Fast-forward when possible (unless explicitly disabled)
          (and can-ff? (not= ff :never))
          (<? (ops/fast-forward! conn from to))

          ;; Squash mode
          squash?
          (<? (ops/squash! conn from to opts))

          ;; Regular merge mode (not yet implemented)
          :else
          (throw (ex-info "Regular merge not yet implemented. Use :squash? true or :ff :auto"
                          {:status 501 :error :not-implemented})))))))

(defn rebase!
  "Rebases source branch onto target branch (updates source branch).
  
  Note: This is currently a stub. The original implementation has been
  moved to merge! as it was updating the target branch instead of source.
  
  TODO: Implement true rebase that updates the source branch by
  replaying its commits on top of the target branch."
  [conn from to opts]
  (go-try
    ;; True rebase would:
    ;; 1. Find where 'from' diverged from 'to'
    ;; 2. Take all commits from 'from' since divergence
    ;; 3. Replay them on top of latest 'to'
    ;; 4. Update 'from' branch with new commits
    ;; 5. Leave 'to' branch unchanged

    (throw (ex-info "True rebase not yet implemented. Use merge! to apply changes from one branch to another."
                    {:status 501
                     :error :not-implemented
                     :note "Current 'rebase' functionality has been moved to merge!"
                     :suggestion "Use merge! with :squash? or :ff options"}))))

(defn reset-branch!
  "Resets a branch to a previous state."
  [conn branch to opts]
  (go-try
    (let [{:keys [mode preview?]
           :or {mode :safe}} opts]

      (when (= mode :hard)
        (throw (ex-info "Hard reset not yet implemented"
                        {:status 501 :error :not-implemented})))

      (when preview?
        (reduced
         {:status :preview
          :operation :reset
          :branch branch
          :mode mode
          :reset-to to
          :message "Would reset branch to specified state"}))

      (<? (ops/safe-reset! conn branch to opts)))))

(defn branch-divergence
  "Analyzes divergence between two branches.
  
  Parameters:
    conn - Connection object
    branch1-spec - First branch spec
    branch2-spec - Second branch spec
    
  Returns promise resolving to divergence analysis including:
    :common-ancestor - Commit ID of common ancestor
    :can-fast-forward - Boolean if one can fast-forward to the other
    :fast-forward-direction - Direction of fast-forward if possible"
  [conn branch1-spec branch2-spec]
  (go-try
    (let [ledger1 (<? (connection/load-ledger conn branch1-spec))
          ledger2 (<? (connection/load-ledger conn branch2-spec))
          db1 (ledger/current-db ledger1)
          db2 (ledger/current-db ledger2)
          ;; Get branch metadata for common ancestor detection
          branch1-info (<? (ledger/branch-info ledger1))
          branch2-info (<? (ledger/branch-info ledger2))
          common-ancestor (<? (merge-branch/find-lca conn db1 db2 branch1-info branch2-info))
          ff-1-to-2 (<? (merge-branch/can-fast-forward? conn db1 db2 branch1-info branch2-info))
          ff-2-to-1 (<? (merge-branch/can-fast-forward? conn db2 db1 branch2-info branch1-info))]
      {:common-ancestor common-ancestor
       :can-fast-forward (or ff-1-to-2 ff-2-to-1)
       :fast-forward-direction (cond
                                 ff-1-to-2 :branch1-to-branch2
                                 ff-2-to-1 :branch2-to-branch1
                                 :else nil)})))

(defn branch-graph
  "Returns a graph representation of branches and their relationships.
  
  Parameters:
    conn - Connection object
    ledger-spec - Ledger specification (e.g., 'myledger')
    opts - Options map:
      :format - :json (default) or :ascii
      :depth - Number of commits to show (default 20, :all for everything)
      :branches - Specific branches to include (default: all)
  
  Returns promise resolving to graph data in requested format."
  [conn ledger-spec opts]
  (merge-graph/branch-graph conn ledger-spec opts))