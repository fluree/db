(ns fluree.db.api.merge
  "Branch merge operations for Fluree DB.
  
  Provides three main operations:
  1. merge! - Three-way delta merge with conflict resolution (Phase 2)
  2. rebase! - Strict replay of commits (Phase 1)
  3. reset-branch! - Safe rollback or hard reset (Phase 1 - safe mode only)"
  (:require [clojure.core.async :as async]
            [clojure.set]
            [clojure.string :as str]
            [fluree.db.async-db :as async-db]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.connection :as connection]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.query.range :as query-range]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

;; ============================================================================
;; Common Ancestor Detection
;; ============================================================================

(defn- same-commit?
  "Check if two databases are at the same commit."
  [source-db target-db]
  (= (get-in source-db [:commit :id])
     (get-in target-db [:commit :id])))

(defn- branch-origin
  "Get the commit ID a branch was created from."
  [branch-info]
  (or (get-in branch-info [:created-from "f:commit" "@id"])      ; nameservice expanded
      (get-in branch-info [:created-from :commit])                ; internal map
      (get-in branch-info ["f:createdFrom" "f:commit" "@id"])     ; raw nameservice
      (get-in branch-info ["created-from" "f:commit" "@id"])))

(defn- branch-created-from?
  "Check if source branch was created from target commit."
  [source-branch-info target-commit-id]
  (= (branch-origin source-branch-info) target-commit-id))

(defn- branches-share-origin?
  "Check if two branches were created from the same commit."
  [source-branch-info target-branch-info]
  (when-let [source-origin (branch-origin source-branch-info)]
    (= source-origin (branch-origin target-branch-info))))

(defn- expand-latest-commit
  [conn db]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          commit-map (:commit db)
          latest-address (:address commit-map)]
      (if (and latest-address (string? latest-address) (not (str/blank? latest-address)))
        (first (<? (commit-storage/read-verified-commit commit-catalog latest-address)))
        ;; Fallback: expand from in-memory commit map
        (let [compact (commit-data/->json-ld commit-map)
              commit-id (commit-data/commit-json->commit-id compact)
              compact* (assoc compact "id" commit-id)]
          (json-ld/expand compact*))))))

(defn- get-commit-chain
  "Returns vector of commit maps from genesis to head for a db."
  [conn db]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          latest-expanded (<? (expand-latest-commit conn db))
          error-ch (async/chan)
          tuples (commit-storage/trace-commits commit-catalog latest-expanded 0 error-ch)]
      (loop [acc []]
        (if-let [[commit-expanded _] (<? tuples)]
          (recur (conj acc (commit-data/json-ld->map commit-expanded nil)))
          acc)))))

(defn find-lca
  "Finds the last common ancestor commit between two branches.
  Returns commit id string."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [source-commit-id (get-in source-db [:commit :id])
          target-commit-id (get-in target-db [:commit :id])]
      (cond
        (same-commit? source-db target-db) source-commit-id
        (branch-created-from? source-branch-info target-commit-id) target-commit-id
        (branch-created-from? target-branch-info source-commit-id) source-commit-id
        (branches-share-origin? source-branch-info target-branch-info) (branch-origin source-branch-info)
        :else
        (let [source-chain (<? (get-commit-chain conn source-db))
              target-chain (<? (get-commit-chain conn target-db))
              source-id-set (into #{} (keep :id) source-chain)
              ;; Try id-based match first
              lca-id (some (fn [commit]
                             (let [cid (:id commit)]
                               (when (and cid (contains? source-id-set cid)) cid)))
                           (reverse target-chain))
              ;; Fallback: t-based match (find matching :data :t) and return that commit's id
              lca-by-t (when (nil? lca-id)
                         (let [source-t-set (into #{} (map #(get-in % [:data :t])) source-chain)]
                           (some (fn [commit]
                                   (let [ct (get-in commit [:data :t])]
                                     (when (and ct (contains? source-t-set ct))
                                       (:id commit))))
                                 (reverse target-chain))))]
          (or lca-id lca-by-t))))))

(defn can-fast-forward?
  "Checks if merge from source to target is a fast-forward merge.
  A fast-forward is possible when target branch's HEAD is an ancestor of source branch's HEAD."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [target-head (get-in target-db [:commit :id])
          common-ancestor (<? (find-lca conn source-db target-db
                                        source-branch-info
                                        target-branch-info))]
      (log/debug "Fast-forward check:"
                 "target-head:" target-head
                 "common:" common-ancestor
                 "is-ff?" (= target-head common-ancestor))
      (= target-head common-ancestor))))

;; ============================================================================
;; Utility Functions
;; ============================================================================

(defn- ensure-sync-db
  "Ensures we have a synchronous database, dereferencing async if needed."
  [db]
  (go-try
    (if (async-db/db? db)
      (<? (async-db/deref-async db))
      db)))

;; ============================================================================
;; Commit Extraction and Processing
;; ============================================================================

(defn- read-commit-data
  "Reads the actual data from a commit.
  Returns map with :asserted and :retracted flakes.
  Note: When used from compute-spot-values, db-context should already have all necessary namespaces."
  [conn commit db-context]
  (go-try
    (let [db-context* (<? (ensure-sync-db db-context))]
      (when-let [data-address (get-in commit [:data :address])]
        (let [commit-catalog (:commit-catalog conn)
              data-jsonld (<? (commit-storage/read-data-jsonld commit-catalog data-address))
              ;; Note: We don't add namespaces here when called from compute-spot-values
              ;; because the db-context should already have all necessary namespaces.
              ;; This function is currently only used by compute-spot-values where
              ;; namespaces are pre-loaded in compute-net-flakes.
              assert-data (get data-jsonld const/iri-assert)
              retract-data (get data-jsonld const/iri-retract)
              t (get-in commit [:data :t])
              asserted-flakes (when assert-data
                                (flake-db/create-flakes true db-context* t assert-data))
              retracted-flakes (when retract-data
                                 (flake-db/create-flakes false db-context* t retract-data))]
          (log/debug "read-commit-data: t=" t
                     "assert-count=" (count assert-data)
                     "retract-count=" (count retract-data))
          {:asserted asserted-flakes
           :retracted retracted-flakes
           :all (concat (or asserted-flakes [])
                        (or retracted-flakes []))})))))

;; ============================================================================
;; Flake Operations
;; ============================================================================

(defn- stage-flakes
  "Stages flakes directly into a database.
  Handles retractions by finding and removing matching assertions from the database."
  [db flakes opts]
  (go-try
    (if (empty? flakes)
      db
      (let [db* (<? (ensure-sync-db db))
            next-t (flake/next-t (:t db*))
            ;; retime all flakes to the new t
            retimed (into [] (map (fn [f]
                                    (flake/create (flake/s f)
                                                  (flake/p f)
                                                  (flake/o f)
                                                  (flake/dt f)
                                                  next-t
                                                  (flake/op f)
                                                  (flake/m f)))) flakes)
            {adds true rems false} (group-by flake/op retimed)
            ;; For retractions, we need to find the actual flakes to remove
            ;; Look in both novelty AND the indexed data
            root-db (policy/root db*)
            flakes-to-remove (when rems
                               (<? (async/go
                                     (loop [to-remove []
                                            remaining rems]
                                       (if-let [retraction (first remaining)]
                                         (let [s (flake/s retraction)
                                               p (flake/p retraction)
                                               o (flake/o retraction)
                                               dt (flake/dt retraction)
                                              ;; Find matching flakes in the database
                                               existing (<? (query-range/index-range root-db nil :spot = [s p]
                                                                                     {:flake-xf (filter #(and (= (flake/o %) o)
                                                                                                              (= (flake/dt %) dt)
                                                                                                              (true? (flake/op %))))}))
                                               to-remove* (into to-remove existing)]
                                           (recur to-remove* (rest remaining)))
                                         to-remove)))))
            db-after (-> db*
                         (assoc :t next-t
                                :staged {:txn (:message opts "Rebase merge")
                                         :author (:author opts "system/merge")
                                         :annotation (:annotation opts)})
                         (commit-data/update-novelty (or adds []) flakes-to-remove))]
        db-after))))

;; ============================================================================
;; Transaction Replay
;; ============================================================================

(defn- collect-commit-namespaces
  "Collects all unique namespace IRIs from a sequence of commits."
  [conn commits]
  (go-try
    (loop [namespaces #{}
           remaining commits]
      (if-let [commit (first remaining)]
        (let [commit-catalog (:commit-catalog conn)
              data-address (get-in commit [:data :address])
              data-jsonld (when data-address
                            (<? (commit-storage/read-data-jsonld commit-catalog data-address)))
              commit-nses-raw (get data-jsonld const/iri-namespaces)
              commit-nses (when (seq commit-nses-raw)
                            (mapv :value commit-nses-raw))]
          (recur (into namespaces commit-nses) (rest remaining)))
        namespaces))))

(defn- prepare-target-db-namespaces
  "Prepares target database with proper namespace configuration and adds source namespaces."
  [target-db source-namespaces]
  (let [;; Ensure target-db has proper namespace configuration
        target-db-prepared (cond-> target-db
                             (not (map? (:namespaces target-db)))
                             (assoc :namespaces {})

                             (not (:max-namespace-code target-db))
                             (assoc :max-namespace-code
                                    (or (and (:namespace-codes target-db)
                                             (iri/get-max-namespace-code (:namespace-codes target-db)))
                                        100)))]
    ;; Add all source namespaces to target-db
    (if (seq source-namespaces)
      (flake-db/with-namespaces target-db-prepared source-namespaces)
      target-db-prepared)))

(defn- apply-flakes-to-spot-map
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

(defn- compute-spot-values
  "Processes commits sequentially to compute the net spot->values map."
  [conn commits target-db]
  (go-try
    (loop [spot->values {}
           remaining commits]
      (if-let [commit (first remaining)]
        (let [flakes (<? (read-commit-data conn commit target-db))
              updated-spots (apply-flakes-to-spot-map spot->values flakes)]
          (recur updated-spots (rest remaining)))
        spot->values))))

(defn- query-existing-spot-values
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

(defn- generate-flakes-from-spots
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

(defn- compute-net-flakes
  "Computes net effect of all source commits.
  Returns [flakes updated-target-db] where updated-target-db has the necessary namespace mappings."
  [conn target-db source-commits]
  (go-try
    ;; Step 1: Ensure synchronous db
    (let [target-db-sync (<? (ensure-sync-db target-db))
          ;; Step 2: Collect all namespaces from source commits
          all-namespaces (<? (collect-commit-namespaces conn source-commits))
          ;; Step 3: Prepare target-db with namespaces
          target-db-with-ns (prepare-target-db-namespaces target-db-sync all-namespaces)
          ;; Step 4: Process commits to compute net spot values
          spot->values (<? (compute-spot-values conn source-commits target-db-with-ns))]

      ;; Step 5: Generate final flakes based on net changes
      (if (empty? spot->values)
        [(flake/sorted-set-by flake/cmp-flakes-spot) target-db-with-ns]
        (let [;; Query existing values for affected spots
              existing-values (<? (query-existing-spot-values target-db-with-ns (keys spot->values)))
              ;; Generate flakes including retractions and assertions
              all-flakes (generate-flakes-from-spots spot->values existing-values)]
          [(into (flake/sorted-set-by flake/cmp-flakes-spot) all-flakes) target-db-with-ns])))))

(defn- get-changed-spots
  "Returns set of [s p dt] tuples for changed flakes."
  [flakes]
  (into #{} (map (fn [f] [(flake/s f) (flake/p f) (flake/dt f)])) flakes))

;; ============================================================================
;; Result Building
;; ============================================================================

(defn- conflict-response
  "Build conflict response."
  [from-spec to-spec replay-result opts]
  (log/warn "build-conflict-response: from=" from-spec "to=" to-spec
            "failed-commit=" (get-in replay-result [:failed-commit :id])
            "error" :db/rebase-conflict)
  {:status :conflict
   :operation :rebase
   :from from-spec
   :to to-spec
   :strategy (if (:ff-mode opts) "fast-forward" "squash")
   :commits {:applied []
             :skipped []
             :conflicts [{:commit (get-in replay-result [:failed-commit :id])
                          :original-count (count (:original-flakes replay-result))
                          :replay-count (count (:replayed-flakes replay-result))}]}
   :error :db/rebase-conflict
   :message (str "Rebase conflict: Transaction produces different results on target branch. "
                 "Failed at commit: " (get-in replay-result [:failed-commit :id]))})

(defn- success-response
  "Build success response."
  [from-spec to-spec replay-result new-commit-sha opts]
  (log/info "build-success-response: from=" from-spec "to=" to-spec
            "applied-count=" (count (:replayed replay-result))
            "new-commit=" new-commit-sha
            "strategy=" (if (:ff-mode opts) "fast-forward" "squash"))
  {:status :success
   :operation :rebase
   :from from-spec
   :to to-spec
   :strategy (if (:ff-mode opts) "fast-forward" "squash")
   :commits {:applied (mapv #(get-in % [:commit :id]) (:replayed replay-result))
             :skipped []
             :conflicts []}
   :new-commit new-commit-sha})

(defn- commit-message
  "Generate commit message."
  [from-spec opts]
  (or (:message opts)
      (if (:ff-mode opts)
        (str "Fast-forward from " from-spec)
        (str "Squash rebase from " from-spec))))

(defn- commit-if-changed
  "Commit the final database if it has changed. Returns new commit id (or nil)."
  [target-ledger target-db final-db from-spec opts]
  (go-try
    (when (not= (:t target-db) (:t final-db))
      (let [commit-result (<? (transact/commit! target-ledger final-db
                                                (assoc opts
                                                       :message (commit-message from-spec opts)
                                                       :author "system/merge")))]
        (or (get-in commit-result [:commit :id])
            (get-in final-db [:commit :id]))))))

;; ============================================================================
;; Squash Rebase Implementation  
;; ============================================================================

(defn- load-branches
  "Load source and target branches with their metadata."
  [conn from-spec to-spec]
  (go-try
    (let [source-ledger (<? (connection/load-ledger conn from-spec))
          target-ledger (<? (connection/load-ledger conn to-spec))]
      {:source-ledger source-ledger
       :target-ledger target-ledger
       :source-db (ledger/current-db source-ledger)
       :target-db (ledger/current-db target-ledger)
       :source-branch-info (<? (ledger/branch-info source-ledger))
       :target-branch-info (<? (ledger/branch-info target-ledger))})))

(defn- extract-commits-since
  "Extracts commits after LCA. Returns vector of commit maps
  in chronological order (oldest first)."
  [conn source-db lca-commit-id]
  (go-try
    ;; If LCA is the current commit, there are no commits since then
    (if (= lca-commit-id (get-in source-db [:commit :id]))
      []
      (let [commit-catalog (:commit-catalog conn)
            latest-expanded (<? (expand-latest-commit conn source-db))
            error-ch (async/chan)
            ;; include genesis (t=0) so LCA at genesis can be located
            tuples (commit-storage/trace-commits commit-catalog latest-expanded 0 error-ch)
            traced (loop [acc []]
                     (if-let [[exp _] (<? tuples)]
                       (recur (conj acc (commit-data/json-ld->map exp nil)))
                       acc))
            vtr traced
            head-id (get-in source-db [:commit :id])
            normalize-id (fn [cid]
                           (when cid
                             (let [s (str cid)]
                               (if (clojure.string/ends-with? s ".json")
                                 (subs s 0 (- (count s) 5))
                                 s))))]
        (log/debug "extract-commits-since-storage: traced-count=" (count vtr)
                   "head-id=" head-id "lca-id=" lca-commit-id)
        (if (seq vtr)
          (let [ids (mapv :id vtr)
                ids-norm (mapv normalize-id ids)
                lca-norm (normalize-id lca-commit-id)
                idx (when lca-norm
                      (let [found-idx (first (keep-indexed
                                              (fn [i id] (when (= id lca-norm) i))
                                              ids-norm))]
                        (or found-idx -1)))
                idx* (if (or (nil? idx) (= -1 idx))
                       ;; Don't try SHA lookup if we can't find the commit
                       -1
                       idx)
                after-lca (cond
                            (nil? idx*) vtr
                            (= -1 idx*) vtr
                            :else (subvec vtr (inc idx*)))]
            (log/debug "extract-commits-since-storage: after-lca-count=" (count after-lca)
                       "after-lca-ids=" (mapv :id after-lca))
            (vec after-lca))
          (let [walked (loop [acc [] cur (:commit source-db)]
                         (if (and cur (not= (:id cur) lca-commit-id))
                           (recur (conj acc cur) (:previous cur))
                           acc))
                walked* (vec (reverse walked))]
            (log/debug "extract-commits-since-storage: in-memory-walk-count=" (count walked*)
                       "walk-ids=" (mapv :id walked*))
            walked*))))))

(defn- validate-same-ledger!
  "Ensure from/to are within same ledger."
  [from-spec to-spec]
  (let [[from-ledger _] (util.ledger/ledger-parts from-spec)
        [to-ledger _] (util.ledger/ledger-parts to-spec)]
    (when (not= from-ledger to-ledger)
      (throw (ex-info "Cannot operate across different ledgers"
                      {:status 400 :error :db/invalid-branch-operation
                       :from from-ledger :to to-ledger})))))

(defn- fast-forward!
  "Updates target branch pointer to source head."
  [conn from-spec to-spec]
  (go-try
    (let [{:keys [source-db target-branch-info]} (<? (load-branches conn from-spec to-spec))
          source-commit (:commit source-db)
          [_ target-branch] (util.ledger/ledger-parts to-spec)
          compact (-> source-commit
                      commit-data/->json-ld
                      (assoc "alias" to-spec
                             "branch" target-branch
                             "branchMetadata" (select-keys target-branch-info
                                                           [:created-at :created-from :protected :description])))
          publisher (:primary-publisher conn)]
      (when-not publisher
        (throw (ex-info "No nameservice available for fast-forward"
                        {:status 400 :error :db/no-nameservice})))
      (<? (nameservice/publish publisher compact))
      ;; Ensure subsequent loads see fresh state
      (ns-subscribe/release-ledger conn to-spec)
      {:status :success
       :operation :rebase
       :from from-spec
       :to to-spec
       :strategy "fast-forward"
       :commits {:applied [] :skipped [] :conflicts []}
       :new-commit nil})))

(defn- squash!
  "Squash rebase - combines all changes into single commit."
  [conn from-spec to-spec opts]
  (go-try
    (let [{:keys [source-db target-db source-branch-info target-branch-info
                  target-ledger]} (<? (load-branches conn from-spec to-spec))
          common-ancestor (<? (find-lca conn source-db target-db
                                        source-branch-info
                                        target-branch-info))
          ;; Use storage traversal to get all commits after LCA. If no LCA, start from t=1.
          source-commits (<? (extract-commits-since conn source-db common-ancestor))
          target-commits (<? (extract-commits-since conn target-db common-ancestor))
          _ (log/info "squash-rebase!: LCA=" common-ancestor
                      "source-commits=" (map :id source-commits)
                      "target-commits=" (map :id target-commits))
          ;; Check if we need to look for conflicts
          check-conflicts? (seq target-commits)
          ;; Compute net flakes across all commits (oldest -> newest)
          [source-novelty updated-target-db] (<? (compute-net-flakes conn target-db source-commits))
          ;; Only compute target novelty if there are target commits
          [target-novelty _] (when check-conflicts?
                               (<? (compute-net-flakes conn updated-target-db target-commits)))
          s-spots (get-changed-spots source-novelty)
          t-spots (when target-novelty (get-changed-spots target-novelty))
          conflict-spots (when (and s-spots t-spots)
                           (seq (clojure.set/intersection s-spots t-spots)))]
      (log/debug "squash-rebase!: source-novelty-count=" (count (or source-novelty []))
                 "target-novelty-count=" (count (or target-novelty []))
                 "conflict-spots?=" (boolean conflict-spots))
      (if conflict-spots
        (conflict-response from-spec to-spec {:failed-commit {:id (first (map :id source-commits))}}
                           (assoc opts :ff-mode false))
        (let [net-flakes (seq source-novelty)
              final-db (if net-flakes
                         (<? (stage-flakes updated-target-db net-flakes {:message (:message opts)
                                                                         :author "system/merge"}))
                         updated-target-db)
              _ (log/info "squash-rebase!: staging-net-flakes count=" (count (or net-flakes [])))
              new-commit-sha (when net-flakes
                               (<? (commit-if-changed target-ledger target-db final-db from-spec opts)))
              replay-result {:success true
                             :final-db final-db
                             :replayed (map (fn [c] {:commit c :flakes nil :success true}) source-commits)}]
          (success-response from-spec to-spec replay-result new-commit-sha opts))))))

;; ============================================================================
;; Safe Reset Implementation
;; ============================================================================

(defn- get-db-at-state
  "Gets database at a specific state (t-value or SHA)."
  [conn branch-spec state-spec]
  (go-try
    (let [ledger (<? (connection/load-ledger conn branch-spec))
          current-db (ledger/current-db ledger)]
      (cond
        (:t state-spec)
        (assoc current-db :t (:t state-spec))

        (:sha state-spec)
        (let [target-t (<? ((:sha->t current-db) current-db (:sha state-spec)))]
          (assoc current-db :t target-t))

        :else
        (throw (ex-info "Invalid state specification. Must provide :t or :sha"
                        {:status 400 :error :db/invalid-state-spec}))))))

(defn- safe-reset!
  "Creates a new commit that reverts the branch to a previous state.
  This is non-destructive - adds a new commit rather than rewriting history."
  [conn branch-spec state-spec _opts]
  (go-try
    (let [ledger (<? (connection/load-ledger conn branch-spec))
          current-db (ledger/current-db ledger)
          current-t (:t current-db)

          target-db (<? (get-db-at-state conn branch-spec state-spec))
          target-t (:t target-db)]

      (when (= current-t target-t)
        (throw (ex-info "Branch is already at the specified state"
                        {:status 400 :error :db/no-op
                         :current-t current-t})))

      ;; TODO: In a full implementation, we would:
      ;; 1. Query the target state to get all data
      ;; 2. Query the current state to get all data
      ;; 3. Compute the delta (what to add and what to remove)
      ;; 4. Create a transaction that applies these changes
      ;; 5. Commit the transaction with appropriate message

      ;; For now, throw not-implemented
      (throw (ex-info "Safe reset not yet fully implemented"
                      {:status 501 :error :not-implemented
                       :message "Safe reset requires delta computation between states"})))))

;; ============================================================================
;; Public API Functions
;; ============================================================================

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
    (validate-same-ledger! from to)
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
            (<? (load-branches conn from to))

            can-ff? (<? (can-fast-forward? conn source-db target-db
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
          (<? (fast-forward! conn from to))

          ;; Squash mode
          squash?
          (<? (squash! conn from to opts))

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

      (<? (safe-reset! conn branch to opts)))))
