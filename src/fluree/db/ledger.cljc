(ns fluree.db.ledger
  (:require [clojure.string :as str]
            [fluree.db.branch :as branch]
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

;; TODO - no time travel, only latest db on a branch thus far
(defn current-db
  "Returns the current database for this ledger.
  Since each ledger now represents a single branch, no branch parameter is needed."
  [ledger]
  (when-let [state (:state ledger)]
    (branch/current-db @state)))

(defn update-commit!
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  ([ledger db]
   (update-commit! ledger db nil))
  ([{:keys [state alias] :as _ledger} db index-files-ch]
   (log/debug "Attempting to update ledger:" alias "with new commit to t" (:t db))
   (when-not state
     (throw (ex-info "Unable to update commit - ledger has no state"
                     {:status 400, :error :db/invalid-ledger})))
   (-> @state
       (branch/update-commit! db index-files-ch)
       branch/current-db)))

(defn status
  "Returns current commit metadata for this ledger"
  [{:keys [address alias state]}]
  (when state
    (let [current-db  (branch/current-db @state)
          {:keys [commit stats t]} current-db
          {:keys [size flakes]} stats
          ;; Extract branch from alias
          branch (or (util.ledger/ledger-branch alias) "main")]
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
    (let [commit-t  (-> expanded-commit
                        (get-first const/iri-data)
                        (get-first-value const/iri-fluree-t))
          db        (current-db ledger)
          current-t (:t db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond
        (= commit-t (flake/next-t current-t))
        (let [updated-db (<? (flake.transact/-merge-commit db expanded-commit expanded-data))]
          (update-commit! ledger updated-db)
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

(defn instantiate
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [combined-alias ledger-address commit-catalog index-catalog primary-publisher secondary-publishers
   indexing-opts did latest-commit & [branch-metadata]]
  (let [;; Parse ledger name and branch from combined alias
        branch (or (util.ledger/ledger-branch combined-alias) "main")
        publishers (cons primary-publisher secondary-publishers)
        branch-state (branch/state-map combined-alias branch commit-catalog index-catalog
                                       publishers latest-commit indexing-opts)
        ;; Add branch metadata
        ;; When creating new ledger (no branch-metadata), it's always main branch
        ;; When loading existing ledger, branch-metadata comes from nameservice
        branch-state-with-meta (if branch-metadata
                                 ;; Loading existing ledger - use metadata from nameservice
                                 (merge branch-state branch-metadata)
                                 ;; Creating new ledger - always main branch
                                 (assoc branch-state
                                        :created-at (util/current-time-iso)
                                        :created-from nil ;; main branch has no parent
                                        :protected true))]  ;; main branch is protected
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom branch-state-with-meta)  ;; Just the branch state directly
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
  "Manually triggers indexing for this ledger's current database.
   Returns a channel that will receive the result when indexing completes."
  [ledger]
  (when-let [state (:state ledger)]
    (branch/trigger-index! @state)))

;; Branch operations are now handled at the connection/nameservice level
;; Each branch is a separate ledger object

(defn branch-info
  "Returns detailed information about this ledger's branch"
  [{:keys [primary-publisher alias state] :as _ledger}]
  (go-try
    ;; Extract branch from alias
    (let [current-branch (or (util.ledger/ledger-branch alias) "main")]
      ;; Get nameservice record for branch if available, otherwise get from state
      (if primary-publisher
        (let [ns-record (<? (nameservice/lookup primary-publisher alias))]
          {:name current-branch
           :head (get-in ns-record ["f:commit" "@id"])
           :t (get ns-record "f:t")
           :created-at (get ns-record "f:createdAt")
           :created-from (get ns-record "f:createdFrom")
           :protected (get ns-record "f:protected")
           :description (get ns-record "f:description")})
        ;; No publisher, return info from in-memory state
        (when state
          (let [branch-meta @state
                current-db (branch/current-db branch-meta)]
            {:name current-branch
             :head (get-in current-db [:commit :address])
             :t (:t current-db)
             :created-at (:created-at branch-meta)
             :created-from (:created-from branch-meta)
             :protected (:protected branch-meta)
             :description (:description branch-meta)}))))))