(ns fluree.db.merge
  "Public API for merge, rebase, and reset operations on Fluree database branches."
  (:require [fluree.db.api.branch :as api.branch]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.merge.branch :as merge-branch]
            [fluree.db.merge.commit :as merge-commit]
            [fluree.db.merge.db :as merge-db]
            [fluree.db.merge.flake :as merge-flake]
            [fluree.db.merge.graph :as merge-graph]
            [fluree.db.merge.operations :as ops]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]))

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
                squash? false}} opts

          {:keys [source-db target-db source-branch-info target-branch-info]}
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
                        {:status 501 :error :not-implemented}))))))

(defn- recreate-branch
  "Deletes and recreates a branch pointing to a new commit."
  [conn branch-spec temp-branch new-commit-id branch-metadata]
  (go-try
    ;; Delete the existing branch
    (<? (api.branch/delete-branch! conn branch-spec))
    ;; Recreate it pointing to the new commit
    (<? (api.branch/create-branch!
         conn branch-spec temp-branch
         (assoc branch-metadata :from-commit new-commit-id)))
    ;; Ensure subsequent loads see fresh state
    (ns-subscribe/release-ledger conn branch-spec)))

(defn rebase!
  "Rebases source branch onto target branch (updates source branch).
  
  Takes the source branch and replays its commits on top of the target branch.
  The source branch is updated with new commits, target branch remains unchanged.
  
  Parameters:
    conn - Connection object
    from - Source branch to rebase (will be updated)
    to - Target branch to rebase onto (unchanged)
    opts - Rebase options:
      :squash? - Combine all commits into one (default false)
      :preview? - Dry run without changes (default false)
      
  Returns promise resolving to rebase result."
  [conn from to opts]
  (go-try
    (merge-branch/validate-same-ledger! from to)
    (let [{:keys [squash? preview?]
           :or {squash? false}} opts

          ;; Load branches and find LCA
          {:keys [source-db target-db source-branch-info target-branch-info]}
          (<? (merge-branch/load-branches conn from to))

          lca (<? (merge-branch/find-lca conn source-db target-db
                                         source-branch-info target-branch-info))]

      (when preview?
        (reduced
         {:status :preview
          :operation :rebase
          :from from
          :to to
          :lca lca
          :strategy (if squash? "squash" "replay")}))

      ;; Get commits from source since LCA
      (let [source-commits (<? (merge-commit/extract-commits-since conn source-db lca))]

        ;; Check if there's anything to rebase
        (when (empty? source-commits)
          (throw (ex-info "Nothing to rebase - source branch is already up to date"
                          {:status 400 :error :db/no-op})))

        ;; Compute the net flakes for rebase
        (let [[net-flakes _] (if squash?
                               (<? (merge-flake/compute-net-flakes conn target-db source-commits))
                               (throw (ex-info "Commit-by-commit replay not yet implemented for rebase"
                                               {:status 501 :error :not-implemented})))

              message (or (:message opts)
                          (if squash?
                            (str "Rebase " from " onto " to " (squashed)")
                            (str "Rebase " from " onto " to)))

              [ledger-id _] (util.ledger/ledger-parts from)

              ;; Create temporary branch manually (can't use with-temp-branch as we need it for recreate)
              temp-branch-name (str ledger-id ":__rebase_temp_" (random-uuid))
              _ (<? (api.branch/create-branch! conn temp-branch-name to nil))

              ;; Create the rebased commit on temp branch
              new-commit-id (try
                              (<? (go-try
                                    ;; Load temp branch and stage flakes
                                    (let [temp-ledger (<? (connection/load-ledger conn temp-branch-name))
                                          temp-db (ledger/current-db temp-ledger)
                                          staged-db (<? (merge-db/stage-flakes temp-db net-flakes nil))
                                          ;; Commit the rebased state
                                          result (<? (transact/commit! temp-ledger staged-db
                                                                       {:message message
                                                                        :author (or (:author opts) "system/rebase")}))]
                                      (:id (:commit result)))))
                              (catch #?(:clj Exception :cljs js/Error) e
                                ;; Clean up temp branch on error
                                (try
                                  (<? (api.branch/delete-branch! conn temp-branch-name))
                                  (catch #?(:clj Exception :cljs js/Error) _ nil))
                                (throw e)))

              ;; Recreate source branch pointing to new commit
              _ (<? (recreate-branch conn from
                                     temp-branch-name
                                     new-commit-id
                                     (select-keys source-branch-info [:protected :description])))

              ;; Clean up temp branch
              _ (try
                  (<? (api.branch/delete-branch! conn temp-branch-name))
                  (catch #?(:clj Exception :cljs js/Error) _ nil))]

          {:status :success
           :operation :rebase
           :from from
           :to to
           :strategy (if squash? "squash" "replay")
           :commits {:rebased (count source-commits)}
           :new-commit new-commit-id})))))

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