(ns fluree.db.api.merge
  "Branch merge operations for Fluree DB.
  
  Provides three main operations:
  1. merge! - Three-way delta merge with conflict resolution (Phase 2)
  2. rebase! - Strict replay of commits (Phase 1)
  3. reset-branch! - Safe rollback or hard reset (Phase 1 - safe mode only)"
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.async-db :as async-db]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.connection :as connection]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.storage :as storage]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
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

(defn- created-from-commit
  "Get the commit ID a branch was created from.
  Supports multiple shapes depending on source."
  [branch-info]
  (or (get-in branch-info [:created-from "f:commit" "@id"])         ;; nameservice expanded
      (get-in branch-info [:created-from :commit])                      ;; internal map
      (get-in branch-info ["f:createdFrom" "f:commit" "@id"])      ;; raw nameservice
      (get-in branch-info ["created-from" "f:commit" "@id"])))

(defn- branch-created-from?
  "Check if source branch was created from target commit."
  [source-branch-info target-commit-id]
  (= (created-from-commit source-branch-info) target-commit-id))

(defn- branches-share-origin?
  "Check if two branches were created from the same commit."
  [source-branch-info target-branch-info]
  (when-let [source-origin (created-from-commit source-branch-info)]
    (= source-origin (created-from-commit target-branch-info))))

(defn- collect-commit-chain
  "Collect all commit IDs in a branch's history."
  [initial-commit]
  (loop [current-commit initial-commit
         commit-ids #{}]
    (if-not current-commit
      commit-ids
      (let [commit-id (:id current-commit)
            updated-ids (if commit-id
                          (conj commit-ids commit-id)
                          commit-ids)
            prev-commit (when-let [prev-id (get-in current-commit [:previous :id])]
                          {:id prev-id
                           :previous (get-in current-commit [:previous :previous])})]
        (recur prev-commit updated-ids)))))

(defn- find-first-common-commit
  "Find the first commit in target that exists in source's history."
  [source-commits target-commit]
  (loop [current-commit target-commit]
    (when current-commit
      (let [commit-id (:id current-commit)]
        (if (and commit-id (contains? source-commits commit-id))
          commit-id
          (when-let [prev-id (get-in current-commit [:previous :id])]
            (recur {:id prev-id
                    :previous (get-in current-commit [:previous :previous])})))))))

(defn- latest-expanded-commit
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

(defn- all-commit-maps
  "Returns vector of commit maps from genesis to head for a db."
  [conn db]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          latest-expanded (<? (latest-expanded-commit conn db))
          error-ch (async/chan)
          tuples (commit-storage/trace-commits commit-catalog latest-expanded 0 error-ch)]
      (loop [acc []]
        (if-let [[commit-expanded _] (<? tuples)]
          (recur (conj acc (commit-data/json-ld->map commit-expanded nil)))
          acc)))))

(defn find-common-ancestor
  "Finds the common ancestor commit between two branches using storage traversal, with
  metadata heuristics as quick paths. Returns commit id string."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [source-commit-id (get-in source-db [:commit :id])
          target-commit-id (get-in target-db [:commit :id])]
      (cond
        (same-commit? source-db target-db) source-commit-id
        (branch-created-from? source-branch-info target-commit-id) target-commit-id
        (branch-created-from? target-branch-info source-commit-id) source-commit-id
        (branches-share-origin? source-branch-info target-branch-info) (created-from-commit source-branch-info)
        :else
        (let [source-chain (<? (all-commit-maps conn source-db))
              target-chain (<? (all-commit-maps conn target-db))
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

(defn is-fast-forward?
  "Checks if merge from source to target is a fast-forward merge.
  A fast-forward is possible when target branch's HEAD is an ancestor of source branch's HEAD."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [target-head (get-in target-db [:commit :id])
          common-ancestor (<? (find-common-ancestor conn source-db target-db
                                                    source-branch-info
                                                    target-branch-info))]
      (log/debug "Fast-forward check:"
                 "target-head:" target-head
                 "common:" common-ancestor
                 "is-ff?" (= target-head common-ancestor))
      (= target-head common-ancestor))))

;; ============================================================================
;; Commit Extraction and Processing
;; ============================================================================

(defn- extract-commits-since
  "Extracts all commits from a branch since a given commit ID.
  Returns a sequence of commit maps in chronological order (oldest first)."
  [db since-commit-id]
  (go-try
    (loop [current-commit (:commit db)
           commits []]
      (if (or (nil? current-commit)
              (= (:id current-commit) since-commit-id))
        (reverse commits)
        (recur (:previous current-commit)
               (conj commits current-commit))))))

(defn- read-commit-data
  "Reads the actual data from a commit.
  Returns map with :asserted and :retracted flakes."
  [conn commit db-context]
  (go-try
    (let [db-context* (if (and db-context (async-db/db? db-context))
                        (<? (async-db/deref-async db-context))
                        db-context)]
      (when-let [data-address (get-in commit [:data :address])]
        (let [commit-catalog (:commit-catalog conn)
              data-jsonld (<? (commit-storage/read-data-jsonld commit-catalog data-address))
              nses (map :value (get data-jsonld const/iri-namespaces))
              db-for-decode (if (seq nses)
                              (flake-db/with-namespaces db-context* nses)
                              db-context*)
              assert-data (get data-jsonld const/iri-assert)
              retract-data (get data-jsonld const/iri-retract)
              t (get-in commit [:data :t])
              asserted-flakes (when assert-data
                                (flake-db/create-flakes true db-for-decode t assert-data))
              retracted-flakes (when retract-data
                                 (flake-db/create-flakes false db-for-decode t retract-data))]
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

(defn- normalize-flake
  "Normalize a flake for semantic comparison, ignoring t values and metadata."
  [f]
  [(flake/s f) (flake/p f) (flake/o f) (flake/dt f) (flake/op f)])

(defn- compare-flakes-semantically
  "Compares two sets of flakes ignoring t values and metadata.
  Returns true if they represent the same semantic changes."
  [flakes1 flakes2]
  (= (set (map normalize-flake flakes1))
     (set (map normalize-flake flakes2))))

(defn- stage-flakes
  "Stages flakes directly into a database.
  Bypasses transaction parsing for rebase operations."
  [db flakes opts]
  (go-try
    (if (empty? flakes)
      db
      (let [db* (if (and db (async-db/db? db))
                  (<? (async-db/deref-async db))
                  db)
            next-t (flake/next-t (:t db*))
            ;; retime all flakes to the new t so they are captured in novelty for this commit
            retimed (into [] (map (fn [f]
                                    (flake/create (flake/s f)
                                                  (flake/p f)
                                                  (flake/o f)
                                                  (flake/dt f)
                                                  next-t
                                                  (flake/op f)
                                                  (flake/m f)))) flakes)
            {adds true rems false} (group-by flake/op retimed)
            db-after (-> db*
                         (assoc :t next-t
                                :staged {:txn (:message opts "Rebase merge")
                                         :author (:author opts "system/merge")
                                         :annotation (:annotation opts)})
                         (commit-data/update-novelty (or adds []) (or rems [])))]
        db-after))))

;; ============================================================================
;; Transaction Replay
;; ============================================================================

(defn- parse-transaction
  "Parse a transaction JSON string into Clojure data."
  [txn-str]
  (try*
    (json/parse txn-str)
    (catch* _e
      nil)))

(defn- read-txn
  "Reads a stored transaction document from commit storage by address."
  [conn txn-address]
  (go-try
    (when (and txn-address (string? txn-address))
      (<? (storage/read-json (:commit-catalog conn) txn-address)))))

(defn- staged-flakes
  "Extracts novelty flakes for the most recent stage at db's :t."
  [db]
  (some-> db :novelty :tspo (flake/match-tspo (:t db)) not-empty))

(defn- net-flakes-for-squash
  "Computes the net set of flakes across source commits since LCA.
  Uses last-write-wins per (s,p,dt) spot, independent of original t/value.
  Returns a collection of flakes with t normalized (they will be retimed at staging)."
  [conn target-db source-commits]
  (go-try
    (loop [spot->flake {}
           commits source-commits]
      (if-let [commit (first commits)]
        (let [data (<? (read-commit-data conn commit target-db))
              adds (:asserted data)
              rems (:retracted data)
              build (fn [f opval]
                      (flake/create (flake/s f)
                                    (flake/p f)
                                    (flake/o f)
                                    (flake/dt f)
                                    0
                                    opval
                                    (flake/m f)))
              spot-key (fn [f] [(flake/s f) (flake/p f) (flake/dt f)])
              spot->flake* (reduce (fn [m f]
                                     (assoc m (spot-key f) (build f false)))
                                   spot->flake
                                   (or rems []))
              spot->flake** (reduce (fn [m f]
                                      (assoc m (spot-key f) (build f true)))
                                    spot->flake*
                                    (or adds []))]
          (recur spot->flake** (rest commits)))
        (let [novelty (->> spot->flake vals (into (flake/sorted-set-by flake/cmp-flakes-spot)))]
          novelty)))))

(defn- spots-changed
  "Returns a set of [s p dt] spots changed by the provided novelty flakes."
  [flakes]
  (into #{} (map (fn [f] [(flake/s f) (flake/p f) (flake/dt f)])) flakes))

(defn- replay-transaction
  "Replays a transaction on a database and returns {:db <staged-db> :flakes <novelty-flakes>}.
  This stages the transaction but doesn't commit it."
  [conn db txn-ref]
  (go-try
    (when-let [txn (or (<? (read-txn conn txn-ref))
                       (parse-transaction txn-ref))]
      (let [context (or (get txn "@context") {})
            parsed-txn (if (vector? txn) txn [txn])
            staged-db (<? (flake.transact/stage db nil context nil nil nil
                                                txn parsed-txn))]
        {:db staged-db
         :flakes (staged-flakes staged-db)}))))

;; ============================================================================
;; Replay Loop Functions
;; ============================================================================

(defn- process-single-commit
  "Process a single commit during squash: apply commit's flakes directly (admin path)."
  [conn current-db commit]
  (go-try
    (let [original-data (<? (read-commit-data conn commit current-db))
          commit-flakes (:all original-data)
          txn-ref (:txn commit)
          replay (when txn-ref (<? (replay-transaction conn current-db txn-ref)))
          replay-flakes (:flakes replay)]
      (log/debug "process-single-commit: commit-id=" (:id commit)
                 "txn?=" (boolean txn-ref)
                 "original-count=" (count (or commit-flakes []))
                 "replay-count=" (count (or replay-flakes [])))
      (if (not (compare-flakes-semantically (or commit-flakes []) (or replay-flakes [])))
        (do (log/warn "process-single-commit: semantic mismatch detected at commit" (:id commit))
            {:conflict true
             :commit commit
             :original-flakes commit-flakes
             :replayed-flakes replay-flakes})
        (let [new-db (<? (stage-flakes current-db commit-flakes
                                       {:message (str "Squash: " (:message commit))
                                        :author (:author commit)}))]
          (log/debug "process-single-commit: staged successfully commit-id=" (:id commit))
          {:success true
           :new-db new-db
           :commit commit
           :flakes commit-flakes})))))

(defn- replay-commits*
  "Replay a sequence of commits onto a target database."
  [conn target-db source-commits]
  (go-try
    (loop [current-db target-db
           commits source-commits
           replayed []]
      (if-let [commit (first commits)]
        (let [_ (log/info "replay-commits*: applying commit" (:id commit)
                          "remaining=" (dec (count commits)))
              result (<? (process-single-commit conn current-db commit))]
          (if (:conflict result)
            ;; Return conflict information
            (do (log/warn "replay-commits*: conflict at commit" (get-in result [:commit :id]))
                {:conflict true
                 :failed-commit (:commit result)
                 :original-flakes (:original-flakes result)
                 :replayed-flakes (:replayed-flakes result)
                 :replayed-so-far replayed})
            ;; Continue with next commit
            (recur (:new-db result)
                   (rest commits)
                   (conj replayed {:commit (:commit result)
                                   :flakes (:flakes result)
                                   :success true}))))
        ;; All commits replayed successfully
        (do (log/info "replay-commits*: all commits applied successfully"
                      "applied-count=" (count replayed))
            {:success true
             :final-db current-db
             :replayed replayed})))))

;; ============================================================================
;; Result Building
;; ============================================================================

(defn- build-conflict-response
  "Build a conflict response map."
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

(defn- build-success-response
  "Build a success response map."
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

(defn- generate-commit-message
  "Generate an appropriate commit message."
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
                                                       :message (generate-commit-message from-spec opts)
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

(defn- lca-t
  "Resolves the t value for a commit id on a given db, or 0 if nil."
  [db commit-id]
  (go-try
    (if (and (string? commit-id)
             (not (str/blank? commit-id)))
      (<? (time-travel/sha->t db commit-id))
      0)))

(defn- extract-commits-since-storage
  "Extracts commits after the LCA using commit storage traversal. Returns a vector
  of commit maps in chronological order (oldest first). Uses commit id to locate LCA,
  avoids sha->t lookups."
  [conn source-db lca-commit-id]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          latest-expanded (<? (latest-expanded-commit conn source-db))
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
              idx (when lca-norm (.indexOf ids-norm lca-norm))
              idx* (if (or (nil? idx) (= -1 idx))
                     (let [lca-t* (<? (lca-t source-db lca-commit-id))
                           by-t (some (fn [[i c]] (when (= (get-in c [:data :t]) lca-t*) i))
                                      (map-indexed vector vtr))]
                       (if (nil? by-t) -1 by-t))
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
          walked*)))))

(defn- enforce-same-ledger!
  "Throws if from/to are not within the same ledger base."
  [from-spec to-spec]
  (let [[from-ledger _] (util.ledger/ledger-parts from-spec)
        [to-ledger _] (util.ledger/ledger-parts to-spec)]
    (when (not= from-ledger to-ledger)
      (throw (ex-info "Cannot operate across different ledgers"
                      {:status 400 :error :db/invalid-branch-operation
                       :from from-ledger :to to-ledger})))))

(defn- fast-forward-pointer!
  "Updates the target branch pointer to the source head by publishing the source commit under the target alias."
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

(defn- squash-rebase!
  "Performs a squash rebase by replaying transactions and verifying they produce
  the same semantic results. All changes are combined into a single commit."
  [conn from-spec to-spec opts]
  (go-try
    (let [{:keys [source-db target-db source-branch-info target-branch-info
                  target-ledger]} (<? (load-branches conn from-spec to-spec))
          common-ancestor (<? (find-common-ancestor conn source-db target-db
                                                    source-branch-info
                                                    target-branch-info))
          ;; Use storage traversal to get all commits after LCA. If no LCA, start from t=1.
          source-commits (<? (extract-commits-since-storage conn source-db common-ancestor))
          target-commits (<? (extract-commits-since-storage conn target-db common-ancestor))
          _ (log/info "squash-rebase!: LCA=" common-ancestor
                      "source-commits=" (map :id source-commits)
                      "target-commits=" (map :id target-commits))]
      (let [;; Compute net flakes across all commits (oldest -> newest)
            source-novelty (<? (net-flakes-for-squash conn target-db source-commits))
            target-novelty (<? (net-flakes-for-squash conn target-db target-commits))
            s-spots (spots-changed source-novelty)
            t-spots (spots-changed target-novelty)
            conflict-spots (seq (clojure.set/intersection s-spots t-spots))]
        (log/debug "squash-rebase!: source-novelty-count=" (count (or source-novelty []))
                   "target-novelty-count=" (count (or target-novelty []))
                   "conflict-spots?=" (boolean conflict-spots))
        (if conflict-spots
          (build-conflict-response from-spec to-spec {:failed-commit {:id (first (map :id source-commits))}}
                                   (assoc opts :ff-mode false))
          (let [net-flakes (seq source-novelty)
                final-db (if net-flakes
                           (<? (stage-flakes target-db net-flakes {:message (:message opts)
                                                                   :author "system/merge"}))
                           target-db)
                _ (log/info "squash-rebase!: staging-net-flakes count=" (count (or net-flakes [])))
                new-commit-sha (when net-flakes
                                 (<? (commit-if-changed target-ledger target-db final-db from-spec opts)))
                replay-result {:success true
                               :final-db final-db
                               :replayed (map (fn [c] {:commit c :flakes nil :success true}) source-commits)}]
            (build-success-response from-spec to-spec replay-result new-commit-sha opts)))))))

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
    (enforce-same-ledger! from to)
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

            can-ff? (<? (is-fast-forward? conn source-db target-db
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
          (<? (fast-forward-pointer! conn from to))

          ;; Squash mode
          squash?
          (<? (squash-rebase! conn from to opts))

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
