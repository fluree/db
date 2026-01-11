(ns fluree.db.ledger
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.branch :as branch]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.did :as did]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.indexer.garbage :as garbage]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.track :as track]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as context]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn get-branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

(defn indexing-enabled?
  [ledger branch]
  (-> ledger (get-branch-meta branch) branch/indexing-enabled?))

(defn available-branches
  [{:keys [state] :as _ledger}]
  (-> @state :branches keys))

;; TODO - no time travel, only latest db on a branch thus far
(defn current-db
  ([ledger]
   (current-db ledger nil))
  ([ledger branch]
   (if-let [branch-meta (get-branch-meta ledger branch)] ; if branch is nil, will return default
     (branch/current-db branch-meta)
     (throw (ex-info (str "Invalid branch: " branch " is not one of:"
                          (available-branches ledger))
                     {:status 400, :error :db/invalid-branch})))))

(defn update-commit!
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  ([ledger branch-name db]
   (update-commit! ledger branch-name db nil))
  ([{:keys [state] :as ledger} branch-name db index-files-ch]
   (log/debug "Attempting to update ledger:" (:alias ledger)
              "and branch:" branch-name "with new commit to t" (:t db))
   (if-let [branch-meta (get-branch-meta ledger branch-name)]
     (-> branch-meta
         (branch/update-commit! db index-files-ch)
         branch/current-db)
     (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                          "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                     {:status 400, :error :db/invalid-branch})))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  ([ledger]
   (status ledger const/default-branch-name))
  ([{:keys [address alias] :as ledger} requested-branch]
   (let [branch-data (get-branch-meta ledger requested-branch)
         current-db  (branch/current-db branch-data)
         {:keys [commit stats t]} current-db
         {:keys [size flakes]} stats
         branch (or requested-branch (:branch @(:state ledger)))]
     {:address address
      :alias   alias
      :branch  branch
      :t       t
      :size    size
      :flakes  flakes
      :commit  commit})))

(defn ledger-info
  "Returns comprehensive ledger information including statistics for specified branch (or default branch if nil).
   Computes current property and class statistics by replaying novelty on top of indexed stats.
   Uses connection's LRU cache to share stats computation with f:onClass policy optimization.
   Returns stats in native SID format - use fluree.db.api/ledger-info for IRI-decoded version."
  ([ledger]
   (ledger-info ledger const/default-branch-name))
  ([{:keys [address alias primary-publisher] :as ledger} requested-branch]
   (go-try
     (let [branch-data (get-branch-meta ledger requested-branch)
           current-db  (branch/current-db branch-data)
           {:keys [stats current-stats namespace-codes commit schema index]} (<? (dbproto/-ledger-info current-db))
           commit-jsonld (commit-data/->json-ld commit)
           nameservice (when primary-publisher
                         (try*
                           (<? (nameservice/lookup primary-publisher address))
                           (catch* e
                             (log/debug "Unable to fetch nameservice record" {:alias alias :error (ex-message e)})
                             nil)))]
       {:commit          commit-jsonld
        :nameservice     nameservice
        :namespace-codes namespace-codes
        :stats           (merge stats current-stats)
        :schema          schema
        :index           index}))))

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [ledger expanded-commit expanded-data]
  (go-try
    (let [branch    (get-first-value expanded-commit const/iri-branch)
          commit-t  (-> expanded-commit
                        (get-first const/iri-data)
                        (get-first-value const/iri-fluree-t))
          db        (current-db ledger branch)
          current-t (:t db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger)
                 "at t value:" commit-t "where current cached db t value is:"
                 current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond
        (= commit-t (flake/next-t current-t))
        (let [updated-db (<? (transact/-merge-commit db expanded-commit expanded-data))]
          (try*
            (update-commit! ledger branch updated-db)
            ::updated
            (catch* e
              (log/warn e "notify commit sequencing conflict; marking ledger stale to reload"
                        {:alias (:alias ledger)
                         :branch branch
                         :current-t current-t
                         :commit-t commit-t})
              ::stale)))

        ;; missing some updates, dump in-memory ledger forcing a reload
        (flake/t-after? commit-t (flake/next-t current-t))
        (do
          (log/warn "Received commit update that is more than 1 ahead of current ledger state. "
                    "Will dump in-memory ledger and force a reload: " (:alias ledger))
          ::stale)

        (= commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger)
                    " at t value: " commit-t " however we already have this commit so not applying: "
                    current-t)
          ::current)

        (flake/t-before? commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger)
                    " at t value: " commit-t " however, latest t is more current: "
                    current-t)
          ::newer)))))

(defn expand-and-extract-ns
  "Expands a nameservice record and extracts key fields via IRIs.

  Returns map {:ledger-alias :branch :ns-t :commit-address :index-address}."
  [ns-record]
  (let [expanded (json-ld/expand ns-record)
        ;; The @id field contains the full ledger:branch alias
        ledger-alias (get-first-value expanded const/iri-id)]
    {:ledger-alias   ledger-alias
     :branch         (or (get-first-value expanded const/iri-branch)
                         (second (util.ledger/ledger-parts ledger-alias)))
     :ns-t           (get-first-value expanded const/iri-fluree-t)
     :commit-address (-> (get-first expanded const/iri-commit)
                         (get-first-value const/iri-id))
     :index-address  (-> (get-first expanded const/iri-index)
                         (get-first-value const/iri-id))}))

(defn idx-address->idx-id
  "Extracts the hash from a content-addressed index address and returns the index ID.
  Uses storage/get-hash to properly extract the hash from any storage backend.
  Returns: 'fluree:index:sha256:abc123def'"
  [index-catalog index-address]
  (go-try
    (let [hash (<? (storage/get-hash (:storage index-catalog) index-address))]
      (str "fluree:index:sha256:" hash))))

(defn- apply-index-to-db
  "Applies an index to a database and returns the updated db.
  Returns a channel with the updated db."
  [db index-catalog root index-address]
  (go-try
    (let [data          (-> db :commit :data)
          index-id      (<? (idx-address->idx-id index-catalog index-address))
          index-version (:v root)
          index-map     (commit-data/new-index data index-id index-address index-version
                                               (select-keys root [:spot :post :opst :tspo :stats]))]
      (dbproto/-index-update db index-map))))

(defn- update-branch-with-index
  "Updates branch state with new index. If use-update-commit? is true,
  uses update-commit! which enforces commit sequencing. Otherwise,
  directly updates the branch state for index-only updates."
  [ledger branch updated-db use-update-commit?]
  (if use-update-commit?
    (do (update-commit! ledger branch updated-db)
        ::index-updated)
    (let [branch-meta (get-branch-meta ledger branch)]
      (swap! (:state branch-meta) branch/update-index updated-db)
      ::index-updated)))

(defn- compare-index-by-hash
  "Tie-breaker for indexes at the same t value. Returns a channel that resolves to true
  if new-idx should replace cur-idx. Uses lexicographic comparison of content hashes
  for deterministic selection."
  [index-catalog new-idx-addr cur-idx-addr]
  (go-try
    (let [new-hash (<? (idx-address->idx-id index-catalog new-idx-addr))
          cur-hash (<? (idx-address->idx-id index-catalog cur-idx-addr))]
      (pos? (compare new-hash cur-hash)))))

(defn- handle-index-behind-commit
  "Handles case where new index t is less than current commit t.
  Compares against current index (if exists) to decide whether to apply."
  [ledger branch db index-catalog new-root new-idx-t new-idx-addr cur-t cur-idx-addr]
  (go-try
    (if cur-idx-addr
      (let [cur-root  (<? (index-storage/read-db-root index-catalog cur-idx-addr))
            cur-idx-t (:t cur-root)]
        (cond
          (flake/t-after? new-idx-t cur-idx-t)
          (do (log/debug "notify-index: applying newer index (behind commit but ahead of current)"
                         {:new-idx-t new-idx-t :cur-idx-t cur-idx-t :cur-t cur-t})
              (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
                (update-branch-with-index ledger branch updated-db false)))

          (= new-idx-t cur-idx-t)
          (if (<? (compare-index-by-hash index-catalog new-idx-addr cur-idx-addr))
            (do (log/debug "notify-index: same t behind commit, applying based on hash tie-breaker"
                           {:new-idx-addr new-idx-addr :cur-idx-addr cur-idx-addr})
                (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
                  (update-branch-with-index ledger branch updated-db false)))
            (do (log/debug "notify-index: same t behind commit, keeping current based on hash"
                           {:new-idx-addr new-idx-addr :cur-idx-addr cur-idx-addr})
                ::index-current))

          :else
          (do (log/debug "notify-index: ignoring older index"
                         {:new-idx-t new-idx-t :cur-idx-t cur-idx-t})
              ::index-current)))
      (do (log/debug "notify-index: applying index behind commit (no current index)"
                     {:new-idx-t new-idx-t :cur-t cur-t})
          (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
            (update-branch-with-index ledger branch updated-db false))))))

(defn- handle-index-matches-commit
  "Handles case where new index t equals current commit t.
  Compares against current index (if exists) to decide whether to apply."
  [ledger branch db index-catalog new-root new-idx-t new-idx-addr cur-idx-addr]
  (go-try
    (if cur-idx-addr
      (let [cur-root  (<? (index-storage/read-db-root index-catalog cur-idx-addr))
            cur-idx-t (:t cur-root)]
        (cond
          (flake/t-after? new-idx-t cur-idx-t)
          (do (log/debug "notify-index: applying newer index at commit t"
                         {:new-idx-t new-idx-t :cur-idx-t cur-idx-t})
              (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
                (update-branch-with-index ledger branch updated-db false)))

          (= new-idx-t cur-idx-t)
          (if (<? (compare-index-by-hash index-catalog new-idx-addr cur-idx-addr))
            (do (log/debug "notify-index: same t at commit, applying based on hash tie-breaker"
                           {:new-idx-addr new-idx-addr :cur-idx-addr cur-idx-addr})
                (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
                  (update-branch-with-index ledger branch updated-db false)))
            (do (log/debug "notify-index: same t at commit, keeping current based on hash"
                           {:new-idx-addr new-idx-addr :cur-idx-addr cur-idx-addr})
                ::index-current))

          :else
          (do (log/warn "notify-index: current index newer than commit?"
                        {:new-idx-t new-idx-t :cur-idx-t cur-idx-t})
              ::index-current)))
      (do (log/debug "notify-index: applying first index for this commit")
          (let [updated-db (<? (apply-index-to-db db index-catalog new-root new-idx-addr))]
            (update-branch-with-index ledger branch updated-db false))))))

(defn notify-index
  "Applies an index-only update when the provided index root matches the current commit t.

  Returns one of:
  - ::index-updated     when index was applied and address changed
  - ::index-current     when index address is same or older
  - ::stale             when index root.t is ahead of current commit t"
  [ledger {:keys [index-address branch]}]
  (go-try
    (let [branch       (or branch (:branch @(:state ledger)))
          db           (current-db ledger branch)
          {:keys [index-catalog]} ledger
          cur-t        (:t db)
          cur-idx-addr (get-in db [:commit :index :address])]
      (log/debug "notify-index called" {:alias (:alias ledger)
                                        :branch branch
                                        :cur-t cur-t
                                        :cur-idx-addr cur-idx-addr
                                        :new-idx-addr index-address})

      (if (= index-address cur-idx-addr)
        (do (log/debug "notify-index: same address, skipping")
            ::index-current)

        (let [new-root  (<? (index-storage/read-db-root index-catalog index-address))
              new-idx-t (:t new-root)]
          (log/debug "notify-index loaded" {:new-idx-t new-idx-t :cur-t cur-t})

          (cond
            (flake/t-after? new-idx-t cur-t)
            (do (log/debug "notify-index: index ahead of commit; marking stale"
                           {:new-idx-t new-idx-t :cur-t cur-t})
                ::stale)

            (= new-idx-t cur-t)
            (<? (handle-index-matches-commit ledger branch db index-catalog
                                             new-root new-idx-t index-address cur-idx-addr))

            (flake/t-before? new-idx-t cur-t)
            (<? (handle-index-behind-commit ledger branch db index-catalog
                                            new-root new-idx-t index-address cur-t cur-idx-addr))

            :else
            (do (log/error "notify-index: unexpected state" {:new-idx-t new-idx-t :cur-t cur-t})
                ::index-current)))))))

(defrecord Ledger [id address alias did state cache commit-catalog
                   index-catalog reasoner primary-publisher secondary-publishers indexing-opts])

(defn initial-state
  [branches current-branch]
  {:branches branches
   :branch   current-branch
   :graphs   {}})

(defn instantiate
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [combined-alias ledger-address commit-catalog index-catalog primary-publisher secondary-publishers
   indexing-opts did latest-commit]
  (let [alias*     (util.ledger/ensure-ledger-branch combined-alias)
        branch     (util.ledger/ledger-branch alias*)
        publishers (cons primary-publisher secondary-publishers)
        branches   {branch (branch/state-map alias* branch commit-catalog index-catalog
                                             publishers latest-commit indexing-opts)}]
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                alias*  ;; Full alias including branch
                  :address              ledger-address
                  :commit-catalog       commit-catalog
                  :index-catalog        index-catalog
                  :primary-publisher    primary-publisher
                  :secondary-publishers secondary-publishers
                  :cache                (atom {})
                  :reasoner             #{}
                  :indexing-opts        indexing-opts})))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [{:keys [alias primary-address publish-addresses commit-catalog index-catalog
           primary-publisher secondary-publishers]}
   {:keys [did indexing] :as _opts}]
  (go-try
    (let [init-time      (util/current-time-iso)
          genesis-commit (<? (commit-storage/write-genesis-commit
                              commit-catalog alias publish-addresses init-time))
          commit-address (util/get-first-value genesis-commit const/iri-address)
          _              (when primary-publisher
                           (<? (nameservice/publish-commit primary-publisher alias commit-address 0)))]
      (instantiate alias primary-address commit-catalog index-catalog
                   primary-publisher secondary-publishers indexing did genesis-commit))))

(defn trigger-index!
  "Manually triggers indexing for a ledger on the specified branch.
   Uses the current db for that branch. Returns a channel that will receive
   the result when indexing completes.

   Options:
   - branch: Branch name (defaults to main branch if not specified)"
  ([ledger]
   (trigger-index! ledger nil))
  ([ledger branch]
   (let [branch-meta (get-branch-meta ledger branch)]
     (branch/trigger-index! branch-meta))))

(defn- update-branch-after-reindex!
  "Swap in a reindexed db, but only if the branch didn't advance."
  [branch-meta expected-t expected-commit-id expected-commit-address reindexed-db]
  (swap! (:state branch-meta)
         (fn [{:keys [current-db] :as current-state}]
           (let [cur-t         (:t current-db)
                 cur-commit-id (get-in current-db [:commit :id])
                 cur-commit-address (get-in current-db [:commit :address])
                 id-match?     (if (some? expected-commit-id)
                                 (= cur-commit-id expected-commit-id)
                                 true)
                 address-match? (if (some? expected-commit-address)
                                  (= cur-commit-address expected-commit-address)
                                  true)]
             (when-not (and (= cur-t expected-t) id-match? address-match?)
               (throw (ex-info "Ledger advanced during reindex; refusing to overwrite newer state."
                               {:status 409
                                :error :db/reindex-conflict
                                :expected {:t expected-t :commit-id expected-commit-id :commit-address expected-commit-address}
                                :current  {:t cur-t :commit-id cur-commit-id :commit-address cur-commit-address}})))
             (assoc current-state
                    :commit     (:commit reindexed-db)
                    :current-db reindexed-db)))))

(defn- validate-reindex-from-t!
  [from-t current-t]
  (when (< from-t 1)
    (throw (ex-info "from-t must be >= 1 (t=0 is genesis commit with no data)"
                    {:status 400 :error :db/invalid-reindex-options :from-t from-t})))
  (when (> from-t current-t)
    (throw (ex-info (str "from-t cannot exceed current t value of " current-t)
                    {:status 400 :error :db/invalid-reindex-options
                     :from-t from-t :current-t current-t})))
  (when (= current-t 0)
    (throw (ex-info "Cannot reindex ledger with only genesis commit (t=0). No data to reindex."
                    {:status 400 :error :db/invalid-reindex-options :current-t current-t}))))

(defn- effective-reindex-batch-bytes
  [batch-bytes indexing-opts threshold-map]
  (or batch-bytes
      (:reindex-max-bytes indexing-opts)
      (:reindex-max-bytes threshold-map)
      10000000)) ;; 10MB default

(defn- threshold-map
  "Returns a map containing reindex thresholds for the current ledger state.

  Does not require realizing an AsyncDB. If an index root exists, reads the root-map
  from storage and applies `flake-db/add-reindex-thresholds` (which prefers
  indexing-opts but falls back to root config/defaults). If no index exists yet,
  uses defaults via `flake-db/add-reindex-thresholds` against a genesis root-map."
  [ledger-alias commit-map index-catalog indexing-opts]
  (go-try
    (let [root-map (if-let [index-address (get-in commit-map [:index :address])]
                     (<? (index-storage/read-db-root index-catalog index-address))
                     (flake-db/genesis-root-map ledger-alias))]
      (flake-db/add-reindex-thresholds root-map indexing-opts))))

(defn- maybe-clean-garbage-before-reindex!
  [index-catalog commit-map max-old-indexes]
  (go-try
    (when-let [index-address (get-in commit-map [:index :address])]
      (when (nat-int? max-old-indexes)
        (log/info "Running garbage collection before reindex"
                  {:alias (get commit-map :alias)
                   :index-address index-address
                   :max-old-indexes max-old-indexes})
        (async/<! (garbage/clean-garbage* index-catalog index-address max-old-indexes))))))

(defn- publish-reindexed-index!
  [primary-publisher ledger-alias reindexed-db]
  (go-try
    (when-let [index-address (get-in reindexed-db [:commit :index :address])]
      (let [index-t (get-in reindexed-db [:commit :index :data :t])]
        (log/info "Publishing reindexed index"
                  {:alias ledger-alias
                   :index-address index-address
                   :index-t index-t})
        (when primary-publisher
          (<? (nameservice/publish-index primary-publisher ledger-alias index-address index-t)))))))

(defn reindex!
  "Rebuilds the index from commit history (offline), regenerating stats.

  Options:
    :from-t        - Start t (default 1; t=0 is genesis)
    :batch-bytes   - Novelty threshold per batch
    :index-files-ch - Optional channel for index file notifications
    :branch        - Branch to reindex (default current branch)

  Note: new transactions should be blocked during reindex."
  ([ledger]
   (reindex! ledger {}))
  ([{:keys [commit-catalog index-catalog indexing-opts primary-publisher] :as ledger}
    {:keys [from-t batch-bytes index-files-ch branch] :or {from-t 1}}]
   (go-try
     ;; Validate branch early - current-db throws if branch is invalid.
     ;; IMPORTANT: Do not realize AsyncDB; rely only on shared keys / storage reads.
     (let [branch-name  (or branch (:branch @(:state ledger)))
           db           (current-db ledger branch-name)
           current-t    (:t db)
           ledger-alias (:alias db)
           commit-map   (:commit db)]

       (validate-reindex-from-t! from-t current-t)

       (let [thresholds      (<? (threshold-map ledger-alias commit-map index-catalog indexing-opts))
             max-old-indexes (:max-old-indexes thresholds)
             branch-meta     (get-branch-meta ledger branch-name)
             expected-commit-id      (:id commit-map)
             expected-commit-address (:address commit-map)
             commit-jsonld    (branch/commit-map->commit-jsonld commit-map)
             batch-bytes*     (effective-reindex-batch-bytes batch-bytes indexing-opts thresholds)
             error-ch         (async/chan 1)
             genesis-db       (flake-db/genesis-db ledger-alias commit-catalog index-catalog indexing-opts)
             commits-ch       (->> (commit-storage/trace-commits commit-catalog commit-jsonld from-t error-ch)
                                   (flake-db/with-commit-data commit-catalog error-ch))
             result-ch        (flake-db/reindex-from-commits genesis-db commits-ch batch-bytes* index-files-ch)]

         (<? (maybe-clean-garbage-before-reindex! index-catalog commit-map max-old-indexes))

         (log/info "Starting reindex for ledger" ledger-alias
                   {:from-t from-t
                    :batch-bytes batch-bytes*
                    :current-t current-t
                    :branch branch-name})

         (async/alt!
           error-ch ([e]
                     (log/error e "Reindex failed for" ledger-alias)
                     (throw e))

           result-ch ([reindexed-db]
                      (if (util/exception? reindexed-db)
                        (throw reindexed-db)
                        (do
                          (when-not (= (:t reindexed-db) current-t)
                            (throw (ex-info "Reindexed db t doesn't match expected t"
                                            {:status 500 :error :db/reindex-mismatch
                                             :expected-t current-t :actual-t (:t reindexed-db)})))

                          (update-branch-after-reindex! branch-meta current-t expected-commit-id expected-commit-address reindexed-db)
                          (<? (publish-reindexed-index! primary-publisher ledger-alias reindexed-db))

                          (log/info "Reindex complete for" ledger-alias
                                    {:stats-properties (count (get-in reindexed-db [:stats :properties]))
                                     :stats-classes (count (get-in reindexed-db [:stats :classes]))})
                          reindexed-db)))))))))

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn parse-keypair
  [ledger {:keys [did private] :as opts}]
  (let [private* (or private
                     (:private did)
                     (-> ledger :did :private))
        did*     (or (some-> private* did/private->did)
                     did
                     (-> ledger :did :id))]
    (assoc opts :did did*, :private private*)))

(defn parse-data-helpers
  [{:keys [context] :as opts}]
  (let [ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context ctx-used-atom)]
    (assoc opts
           :commit-data-opts {:compact-fn    compact-fn
                              :compact       (fn [iri] (json-ld/compact iri compact-fn))
                              :id-key        (json-ld/compact const/iri-id compact-fn)
                              :type-key      (json-ld/compact const/iri-type compact-fn)
                              :ctx-used-atom ctx-used-atom})))

(defn parse-commit-opts
  [ledger opts]
  (-> opts
      (update :context parse-commit-context)
      (->> (parse-keypair ledger))
      parse-data-helpers))

(defn save-txn!
  ([{:keys [commit-catalog alias] :as _ledger} txn]
   (let [ledger-name (util.ledger/ledger-base-name alias)]
     (save-txn! commit-catalog ledger-name txn)))
  ([commit-catalog ledger-name txn]
   (let [path (str/join "/" [ledger-name "txn"])]
     (storage/content-write-json commit-catalog path txn))))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don't think the db should be handling any of it
(defn write-transaction!
  [ledger ledger-name staged]
  (go-try
    (let [{:keys [txn author annotation]} staged
          {:keys [commit-catalog]} ledger]
      (if txn
        (let [{txn-id :address} (<? (save-txn! commit-catalog ledger-name txn))]
          {:txn-id     txn-id
           :author     author
           :annotation annotation})
        staged))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn update-commit-id
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-hash]
  (let [commit-id (commit-data/hash->commit-id commit-hash)]
    [(assoc commit-map :id commit-id)
     (assoc commit-jsonld "id" commit-id)]))

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [commit-jsonld (commit-data/->json-ld commit)
          ;; For credential/generate, we need a DID map with public key
          did-map (when (and did private)
                    (if (map? did)
                      did
                      (did/private->did-map private)))
          signed-commit (if did-map
                          (<? (credential/generate commit-jsonld private did-map))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (-> [commit commit-jsonld]
              (update-commit-id (:hash commit-res))
              (update-commit-address (:address commit-res)))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger.
   Uses atomic publish-commit to update only commit fields, avoiding
   overwriting index data that may have been updated by a separate indexer."
  [{:keys [primary-publisher secondary-publishers] :as _ledger} commit-jsonld]
  (go-try
    (let [ledger-alias   (get commit-jsonld "alias")
          commit-address (get commit-jsonld "address")
          commit-t       (get-in commit-jsonld ["data" "t"])
          _              (log/debug "publish-commit using atomic update"
                                    {:alias ledger-alias :commit-t commit-t})
          result         (<? (nameservice/publish-commit primary-publisher
                                                         ledger-alias
                                                         commit-address
                                                         commit-t))]
      (when-let [secondaries (seq secondary-publishers)]
        (nameservice/publish-commit-to-all ledger-alias commit-address commit-t secondaries))
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (assoc :commit new-commit
               :staged nil
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes))))

(defn indexing-needed?
  [novelty-size min-size]
  (>= novelty-size min-size))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger db]
   (commit! ledger db {}))
  ([{ledger-alias :alias :as ledger}
    {:keys [branch t stats commit] :as staged-db}
    opts]
   (log/debug "commit!: write-transaction start" {:ledger ledger-alias})
   (go-try
     (let [{:keys [commit-catalog]} ledger
           ledger-name (util.ledger/ledger-base-name ledger-alias)

           {:keys [tag time message did private commit-data-opts index-files-ch]
            :or   {time (util/current-time-iso)}}
           (parse-commit-opts ledger opts)

           {:keys [db-jsonld staged-txn]}
           (commit-data/db->jsonld staged-db commit-data-opts)

           {:keys [txn-id author annotation]}
           (<? (write-transaction! ledger ledger-name staged-txn))

           _ (log/debug "commit!: write-jsonld(db) start" {:ledger ledger-alias})

           data-write-result (<? (commit-storage/write-jsonld commit-catalog ledger-name db-jsonld))

           _ (log/debug "commit!: write-jsonld(db) done" {:ledger ledger-alias :db-address (:address data-write-result)})
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file
           dbid              (commit-data/hash->db-id (:hash data-write-result))
           keypair           {:did did, :private private}

           new-commit (commit-data/new-db-commit-map {:old-commit commit
                                                      :issuer     did
                                                      :message    message
                                                      :tag        tag
                                                      :dbid       dbid
                                                      :t          t
                                                      :time       time
                                                      :db-address db-address
                                                      :author     author
                                                      :annotation annotation
                                                      :txn-id     txn-id
                                                      :flakes     (:flakes stats)
                                                      :size       (:size stats)})

           _ (log/debug "commit!: write-commit start" {:ledger ledger-alias})

           {:keys [commit-map commit-jsonld write-result]}
           (<? (write-commit commit-catalog ledger-name keypair new-commit))

           _ (log/debug "commit!: write-commit done" {:ledger ledger-alias :commit-address (:address write-result)})

           db  (formalize-commit staged-db commit-map)

           _ (log/debug "commit!: ledger/update-commit! start" {:ledger ledger-alias :t t})

           db* (update-commit! ledger branch db index-files-ch)]

       (log/debug "commit!: ledger/update-commit! done, publish-commit start" {:ledger ledger-alias :t t :at time})

       (<? (publish-commit ledger commit-jsonld))

       (log/debug "commit!: publish-commit done" {:ledger ledger-alias})

       (if (track/track-txn? opts)
         (let [index-t (commit-data/index-t commit-map)
               novelty-size (get-in db* [:novelty :size] 0)
               ;; Always read threshold from realized FlakeDB; db* may be AsyncDB
               reindex-min-bytes (or (:reindex-min-bytes db) 1000000)]
           (-> write-result
               (select-keys [:address :hash :size])
               (assoc :ledger-id ledger-alias
                      :t t
                      :db db*
                      :indexing-needed (indexing-needed? novelty-size reindex-min-bytes)
                      :index-t index-t
                      :indexing-enabled (indexing-enabled? ledger branch)
                      :novelty-size novelty-size)))
         db*)))))

(defn transact!
  [ledger parsed-txn]
  (go-try
    (let [{:keys [branch] :as parsed-opts,
           :or   {branch const/default-branch-name}}
          (:opts parsed-txn)

          db       (current-db ledger branch)
          staged   (<? (transact/stage-triples db parsed-txn))
          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          cmt-opts (dissoc parsed-opts :context :identity)]
      (if (track/track-txn? parsed-opts)
        (let [staged-db     (:db staged)
              commit-result (<? (commit! ledger staged-db cmt-opts))]
          (merge staged commit-result))
        (<? (commit! ledger staged cmt-opts))))))
