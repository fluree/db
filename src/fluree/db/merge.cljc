(ns fluree.db.merge
  "Public API for merge, rebase, and reset operations on Fluree database branches."
  (:require [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.merge.branch :as merge-branch]
            [fluree.db.merge.operations :as ops]
            [fluree.db.util.async :refer [<? go-try]]))

(defn merge!
  "Three-way delta merge with conflict resolution.
  Phase 2 - Not yet implemented."
  [_conn from to opts]
  (go-try
    (if (:preview? opts)
      {:status :preview
       :operation :merge
       :from from
       :to to
       :message "Three-way merge preview not yet implemented"}

      (throw (ex-info "Three-way merge not yet implemented. Use rebase! instead."
                      {:status 501
                       :error :not-implemented
                       :suggestion "Use rebase! with :squash? true or :ff :auto"})))))

(defn rebase!
  "Strictly replays commits from source branch onto target branch."
  [conn from to opts]
  (go-try
    (merge-branch/validate-same-ledger! from to)
    (let [{:keys [ff squash? atomic? selector preview?]
           :or {ff :auto
                squash? false
                atomic? true}} opts]

      ;; Validate unsupported options
      (when selector
        (throw (ex-info "Cherry-pick selector not yet implemented"
                        {:status 501 :error :not-implemented})))

      (when (false? atomic?)
        (throw (ex-info "Non-atomic mode not yet implemented"
                        {:status 501 :error :not-implemented})))

      ;; Load branches and check for fast-forward
      (let [{:keys [source-db target-db source-branch-info target-branch-info]}
            (<? (merge-branch/load-branches conn from to))

            can-ff? (<? (merge-branch/can-fast-forward? conn source-db target-db
                                                        source-branch-info target-branch-info))]

        (when preview?
          (reduced
           {:status :preview
            :operation :rebase
            :from from
            :to to
            :can-fast-forward can-ff?
            :strategy (cond
                        (and can-ff? (not= ff :never)) "fast-forward"
                        squash? "squash"
                        :else "replay")}))

        (cond
          ;; Fast-forward only mode - fail if not possible
          (and (= ff :only) (not can-ff?))
          {:status :error
           :operation :rebase
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

          ;; Regular replay mode (not yet implemented)
          :else
          (throw (ex-info "Commit-by-commit replay not yet implemented. Use :squash? true"
                          {:status 501 :error :not-implemented})))))))

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