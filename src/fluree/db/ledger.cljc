(ns fluree.db.ledger
  (:require [fluree.db.branch :as branch]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util :as util :refer [get-first get-first-value]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn get-branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

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
   (let [branch-meta (get-branch-meta ledger branch-name)]
     (when-not branch-meta
       (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                            "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                       {:status 400, :error :db/invalid-branch})))
     (-> branch-meta
         (branch/update-commit! db index-files-ch)
         branch/current-db))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  ([ledger]
   (status ledger commit-data/default-branch))
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
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond
        (= commit-t (flake/next-t current-t))
        (let [updated-db (<? (flake.transact/-merge-commit db expanded-commit expanded-data))]
          (update-commit! ledger branch updated-db)
          ::updated)

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
  [alias ledger-address commit-catalog index-catalog primary-publisher secondary-publishers
   indexing-opts did latest-commit]
  (let [[_ branch] (util.ledger/ledger-parts alias)
        branch (or branch "main")
        publishers (cons primary-publisher secondary-publishers)
        branches {branch (branch/state-map alias branch commit-catalog index-catalog
                                           publishers latest-commit indexing-opts)}]
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                alias  ;; Full alias including branch
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
    (let [;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (util/current-time-iso)
          genesis-commit (<? (commit-storage/write-genesis-commit
                              commit-catalog alias publish-addresses init-time))
          ;; Publish genesis commit to nameservice - convert expanded to compact format first
          _              (when primary-publisher
                           (let [;; Convert expanded genesis commit to compact JSON-ld format
                                 commit-map (commit-data/json-ld->map genesis-commit nil)
                                 compact-commit (commit-data/->json-ld commit-map)]
                             (<? (nameservice/publish primary-publisher compact-commit))))]
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
