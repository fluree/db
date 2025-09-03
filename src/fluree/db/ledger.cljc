(ns fluree.db.ledger
  (:require [clojure.string :as str]
            [fluree.db.async-db :as async-db]
            [fluree.db.branch :as branch]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

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
  (let [expanded      (json-ld/expand ns-record)
        ;; The @id field must contain the full ledger:branch alias
        ledger-alias  (get-first-value expanded const/iri-id)
        branch-val    (get-first-value expanded const/iri-branch)
        ns-t          (get-first-value expanded const/iri-fluree-t)
        commit-node   (get-first expanded const/iri-commit)
        commit-addr   (some-> commit-node (get-first-value const/iri-id))
        index-node    (get-first expanded const/iri-index)
        index-addr    (some-> index-node (get-first-value const/iri-id))]
    ;; Validate required/malformed fields with clear errors/warnings
    (when (or (nil? ledger-alias) (not (string? ledger-alias)))
      (log/warn "notify: nameservice record missing or invalid @id (ledger alias)" {:ns-record ns-record})
      (throw (ex-info "Invalid nameservice record: missing @id (ledger alias)"
                      {:status 400 :error :db/invalid-ns-record})))
    (when-not (str/includes? ledger-alias ":")
      (log/warn "notify: nameservice @id must include branch (ledger:branch)" {:ledger-alias ledger-alias})
      (throw (ex-info (str "Invalid nameservice record: @id must include branch (expected 'ledger:branch'), got '" ledger-alias "'")
                      {:status 400 :error :db/invalid-ns-record :ledger-alias ledger-alias})))
    (when (nil? ns-t)
      (log/warn "notify: nameservice record missing f:t (commit t)" {:ledger-alias ledger-alias :ns-record ns-record})
      (throw (ex-info "Invalid nameservice record: missing f:t (commit t)"
                      {:status 400 :error :db/invalid-ns-record :ledger-alias ledger-alias})))
    ;; If f:commit is present but malformed (not a node or missing @id), throw
    (when (and commit-node (nil? commit-addr))
      (log/warn "notify: nameservice record f:commit present but missing @id" {:ledger-alias ledger-alias :commit commit-node})
      (throw (ex-info "Invalid nameservice record: f:commit must be an object with @id"
                      {:status 400 :error :db/invalid-ns-record :ledger-alias ledger-alias})))
    ;; If f:index is present but malformed, warn (not fatal)
    (when (and index-node (nil? index-addr))
      (log/warn "notify: nameservice record f:index present but missing @id" {:ledger-alias ledger-alias :index index-node}))
    {:ledger-alias   ledger-alias
     :branch         (or branch-val
                         (second (str/split ledger-alias #":" 2)))
     :ns-t           ns-t
     :commit-address commit-addr
     :index-address  index-addr}))

(defn idx-address->idx-id
  "Extracts the hash from a content-addressed index address and returns the index ID.
  Address format is like: 'fluree:file://ledger/index/root/abc123def.json'
  Returns: 'fluree:index:sha256:abc123def'"
  [index-address]
  (let [hash (-> index-address
                 (str/split #"/")
                 last
                 (str/replace #"\.json$" ""))]
    (str "fluree:index:sha256:" hash)))

(defn notify-index
  "Applies an index-only update when the provided index root matches the current commit t.

    Returns one of:
    - ::index-updated     when index was applied and address changed
    - ::index-current     when index address is same or older
    - ::stale             when index root.t is ahead of current commit t"
  [ledger {:keys [index-address branch]}]
  (go-try
    (let [branch     (or branch (:branch @(:state ledger)))
          db         (current-db ledger branch)
          {:keys [index-catalog]} ledger
          cur-t      (:t db)
          cur-idx    (get-in db [:commit :index :address])]
      (log/debug "notify-index called" {:alias (:alias ledger)
                                        :branch branch
                                        :cur-t cur-t
                                        :cur-idx cur-idx
                                        :new-index-address index-address})
        ;; Short-circuit if index address hasn't changed
      (if (= index-address cur-idx)
        (do (log/debug "notify-index: index address unchanged, skipping" {:address index-address})
            ::index-current)

          ;; Only load the index file if the address is different
        (let [root    (<? (index-storage/read-db-root index-catalog index-address))
              root-t  (:t root)]
          (log/debug "notify-index loaded root" {:root-t root-t :cur-t cur-t})
          (cond
            (flake/t-after? root-t cur-t)
            (do (log/debug "notify-index: root ahead of current commit; marking stale"
                           {:root-t root-t :cur-t cur-t})
                ::stale)

            (flake/t-before? root-t cur-t)
            (if (some? cur-idx)
              (do (log/debug "notify-index: root behind current commit; ignoring"
                             {:root-t root-t :cur-t cur-t})
                  ::index-current)
              (do (log/debug "notify-index: root behind current commit but no current index; applying"
                             {:root-t root-t :cur-t cur-t})
                  (let [data       (-> db :commit :data)
                        index-id   (idx-address->idx-id index-address)
                        index-map  (commit-data/new-index data index-id index-address
                                                          (select-keys root [:spot :post :opst :tspo]))
                        updated-db (<? (dbproto/-index-update db index-map))]
                    (log/debug "notify-index: applying new index (no current index)" {:index-id index-id
                                                                                      :address index-address})
                    ;; Index-only update: update branch state without enforcing next-commit?
                    (let [branch-meta (get-branch-meta ledger branch)]
                      (swap! (:state branch-meta) branch/update-index updated-db))
                    ::index-updated)))

            :else
            (let [data       (-> db :commit :data)
                  index-id   (idx-address->idx-id index-address)
                  index-map  (commit-data/new-index data index-id index-address
                                                    (select-keys root [:spot :post :opst :tspo]))
                  res        (dbproto/-index-update db index-map)
                  updated-db (if (async-db/db? res)
                               (do (<? (async-db/deref-async res)) res)
                               (<? res))]
              (log/debug "notify-index: applying new index" {:index-id index-id
                                                             :address index-address})
              ;; Index-only update: update branch state without enforcing next-commit?
              (let [branch-meta (get-branch-meta ledger branch)]
                (swap! (:state branch-meta) branch/update-index updated-db))
              ::index-updated)))))))

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
  (let [;; Parse ledger name and branch from combined alias
        [_ branch] (if (str/includes? combined-alias ":")
                     (str/split combined-alias #":" 2)
                     [combined-alias "main"])
        publishers (cons primary-publisher secondary-publishers)
        branches {branch (branch/state-map combined-alias branch commit-catalog index-catalog
                                           publishers latest-commit indexing-opts)}]
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                combined-alias  ;; Full alias including branch
                  :address              ledger-address
                  :commit-catalog       commit-catalog
                  :index-catalog        index-catalog
                  :primary-publisher    primary-publisher
                  :secondary-publishers secondary-publishers
                  :cache                (atom {})
                  :reasoner             #{}
                  :indexing-opts        indexing-opts})))

(defn normalize-alias
  "For a ledger alias, removes any preceding '/' or '#' if exists."
  [ledger-alias]
  (if (or (str/starts-with? ledger-alias "/")
          (str/starts-with? ledger-alias "#"))
    (subs ledger-alias 1)
    ledger-alias))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [{:keys [alias primary-address publish-addresses commit-catalog index-catalog
           primary-publisher secondary-publishers]}
   {:keys [did indexing] :as _opts}]
  (go-try
    (let [normalized-alias  (normalize-alias alias)
          ;; Add :main if no branch is specified
          ledger-alias   (if (str/includes? normalized-alias ":")
                           normalized-alias
                           (str normalized-alias ":main"))
          ;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (util/current-time-iso)
          genesis-commit (<? (commit-storage/write-genesis-commit
                              commit-catalog ledger-alias publish-addresses init-time))
          ;; Publish genesis commit to nameservice - convert expanded to compact format first
          _              (when primary-publisher
                           (let [;; Convert expanded genesis commit to compact JSON-ld format
                                 commit-map (commit-data/json-ld->map genesis-commit nil)
                                 compact-commit (commit-data/->json-ld commit-map)]
                             (<? (nameservice/publish primary-publisher compact-commit))))]
      (instantiate ledger-alias primary-address commit-catalog index-catalog
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
