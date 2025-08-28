(ns fluree.db.merge.operations
  "Core merge operation implementations."
  (:require [clojure.set :as set]
            [fluree.db.connection :as connection]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.ledger :as ledger]
            [fluree.db.merge.branch :as merge-branch]
            [fluree.db.merge.commit :as merge-commit]
            [fluree.db.merge.db :as merge-db]
            [fluree.db.merge.flake :as merge-flake]
            [fluree.db.merge.response :as merge-response]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn fast-forward!
  "Performs a fast-forward merge by updating the target branch reference."
  [conn from to]
  (go-try
    (let [{:keys [source-db target-branch-info]} (<? (merge-branch/load-branches conn from to))
          source-commit (:commit source-db)
          [_ target-branch] (util.ledger/ledger-parts to)
          compact (-> source-commit
                      commit-data/->json-ld
                      (assoc "alias" to
                             "branch" target-branch
                             "branchMetadata" (select-keys target-branch-info
                                                           [:created-at :created-from :protected :description])))
          publisher (:primary-publisher conn)]
      (when-not publisher
        (throw (ex-info "No nameservice available for fast-forward"
                        {:status 400 :error :db/no-nameservice})))
      (<? (nameservice/publish publisher compact))
      ;; Ensure subsequent loads see fresh state
      (ns-subscribe/release-ledger conn to)
      {:status :success
       :operation :rebase
       :from from
       :to to
       :strategy "fast-forward"
       :commits {:applied [] :skipped [] :conflicts []}
       :new-commit nil})))

(defn- load-and-validate-branches
  "Loads branches and validates there are commits to process."
  [conn source-db lca]
  (go-try
    (let [source-commits (<? (merge-commit/extract-commits-since conn source-db lca))]
      (log/info "squash!: source-commits=" (count source-commits))
      (when (empty? source-commits)
        (throw (ex-info "No commits to rebase"
                        {:status 400 :error :db/no-commits})))
      source-commits)))

(defn- detect-conflicts
  "Detects conflicts between source and target branches."
  [source-novelty target-novelty]
  (let [source-spots (merge-flake/get-changed-spots source-novelty)
        target-spots (merge-flake/get-changed-spots target-novelty)]
    (set/intersection source-spots target-spots)))

(defn- apply-squashed-changes
  "Applies squashed changes to the target database."
  [target-ledger db-after-source source-novelty opts from]
  (go-try
    (let [net-flakes (seq source-novelty)
          final-db (if net-flakes
                     (<? (merge-db/stage-flakes db-after-source net-flakes
                                                {:message (:message opts)
                                                 :author "system/merge"}))
                     db-after-source)]
      (log/info "squash!: staging-net-flakes count=" (count (or net-flakes [])))
      (if net-flakes
        (let [result (<? (transact/commit! target-ledger final-db
                                           (assoc opts
                                                  :message (or (:message opts)
                                                               (str "Squash merge from " from))
                                                  :author (or (:author opts) "system/merge"))))]
          {:final-db final-db
           :new-commit-sha (or (get-in result [:commit :id])
                               (get-in final-db [:commit :id]))})
        {:final-db final-db
         :new-commit-sha nil}))))

(defn- handle-diverged-branches
  "Handles squash when target branch has diverged from source."
  [conn updated-target-db source-commits target-commits from to opts target-ledger]
  (go-try
    ;; Compute net changes from both branches
    (let [[source-novelty updated-db-after-source] (<? (merge-flake/compute-net-flakes conn updated-target-db source-commits))
          _ (log/info "squash!: source-novelty count=" (count source-novelty))
          [target-novelty _] (<? (merge-flake/compute-net-flakes conn updated-target-db target-commits))
          _ (log/info "squash!: target-novelty count=" (count target-novelty))
          conflicting-spots (detect-conflicts source-novelty target-novelty)]

      (if (seq conflicting-spots)
        ;; Conflict detected
        (merge-response/conflict-response from to {:id (first (map :id source-commits))}
                                          (assoc opts :ff-mode false))
        ;; No conflicts - apply the changes
        (let [{:keys [final-db new-commit-sha]}
              (<? (apply-squashed-changes target-ledger updated-db-after-source source-novelty opts from))
              replay-result {:success true
                             :final-db final-db
                             :replayed (map (fn [c] {:commit c :flakes nil :success true})
                                            source-commits)}]
          (merge-response/success-response from to replay-result new-commit-sha opts))))))

(defn- handle-fast-path
  "Handles squash when target branch hasn't diverged."
  [conn updated-target-db source-commits from to opts target-ledger]
  (go-try
    (let [[source-novelty updated-db-after-source] (<? (merge-flake/compute-net-flakes conn updated-target-db source-commits))
          {:keys [final-db new-commit-sha]}
          (<? (apply-squashed-changes target-ledger updated-db-after-source source-novelty opts from))
          replay-result {:success true
                         :final-db final-db
                         :replayed (map (fn [c] {:commit c :flakes nil :success true})
                                        source-commits)}]
      (merge-response/success-response from to replay-result new-commit-sha opts))))

(defn squash!
  "Performs a squash rebase by combining all source commits into one."
  [conn from to opts]
  (go-try
    (let [{:keys [_source-ledger target-ledger source-db target-db
                  source-branch-info target-branch-info]}
          (<? (merge-branch/load-branches conn from to))

          ;; Find the Last Common Ancestor
          lca (<? (merge-branch/find-lca conn source-db target-db
                                         source-branch-info target-branch-info))
          _ (log/debug "squash!: LCA=" lca)

          ;; Load and validate source commits
          source-commits (<? (load-and-validate-branches conn source-db lca))

          ;; Get target commits since LCA to check for conflicts
          target-commits (<? (merge-commit/extract-commits-since conn target-db lca))]

      (log/info "squash!: target-commits=" (count target-commits))

      (if (seq target-commits)
        ;; Target has diverged - check for conflicts and apply if safe
        (<? (handle-diverged-branches conn target-db source-commits target-commits
                                      from to opts target-ledger))
        ;; Fast path - target hasn't diverged, just apply source commits
        (<? (handle-fast-path conn target-db source-commits from to opts target-ledger))))))

(defn- validate-reset-state
  "Validates that the branch is not already at the target state."
  [current-t target-t]
  (when (= current-t target-t)
    (throw (ex-info "Branch is already at the specified state"
                    {:status 400 :error :db/no-op
                     :current-t current-t}))))

(defn- apply-reset-changes
  "Stages and commits the reversed changes to reset the branch."
  [ledger current-db reversed-flakes state-spec opts]
  (go-try
    (let [reset-message (merge-response/generate-reset-message state-spec opts)
          reset-author (or (:author opts) "system/reset")
          final-db (if (seq reversed-flakes)
                     (<? (merge-db/stage-flakes current-db reversed-flakes
                                                {:message reset-message
                                                 :author reset-author}))
                     current-db)]
      (if (seq reversed-flakes)
        (let [result (<? (transact/commit! ledger final-db
                                           (assoc opts
                                                  :message reset-message
                                                  :author reset-author)))]
          (or (get-in result [:commit :id])
              (get-in final-db [:commit :id])))
        nil))))

(defn safe-reset!
  "Creates a new commit that reverts the branch to a previous state.
  This is non-destructive - adds a new commit rather than rewriting history."
  [conn branch-spec state-spec opts]
  (go-try
    (let [ledger (<? (connection/load-ledger conn branch-spec))
          current-db (ledger/current-db ledger)
          current-t (:t current-db)
          target-db (<? (merge-db/get-db-at-state conn branch-spec state-spec))
          target-t (:t target-db)]

      ;; Validate we're not already at target state
      (validate-reset-state current-t target-t)
      (log/info "safe-reset!: current-t=" current-t "target-t=" target-t)

      ;; Get all commits and filter to those we need to undo
      (let [all-commits (<? (merge-commit/extract-commits-since conn current-db nil))
            commits-to-undo (merge-response/filter-commits-to-undo all-commits target-t)]

        (if (empty? commits-to-undo)
          ;; No commits to undo - already at target state
          (merge-response/create-reset-result branch-spec state-spec current-t target-t 0 nil)

          ;; Process commits to create reversal
          (let [commits-reversed (reverse commits-to-undo)
                ;; pass current-db; namespaces will be created dynamically as needed
                all-reversed-flakes (<? (merge-flake/process-commits-to-reverse conn commits-reversed current-db))
                _ (log/info "safe-reset!: total-reversed-flakes=" (count all-reversed-flakes))
                new-commit-sha (<? (apply-reset-changes ledger current-db all-reversed-flakes state-spec opts))]

            (merge-response/create-reset-result branch-spec state-spec current-t target-t
                                                (count commits-to-undo) new-commit-sha)))))))