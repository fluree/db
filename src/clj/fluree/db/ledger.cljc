(ns fluree.db.ledger
  (:require [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.constants :as const]
            [fluree.db.commit.storage :as commit-storage]
            [clojure.string :as str]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]))

#?(:clj (set! *warn-on-reflection* true))

(defn get-branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

;; TODO - no time travel, only latest db on a branch thus far
(defn current-db
  ([ledger]
   (current-db ledger nil))
  ([ledger branch]
   (let [branch-meta (get-branch-meta ledger branch)]
     ;; if branch is nil, will return default
     (when-not branch-meta
       (throw (ex-info (str "Invalid branch: " branch ".")
                       {:status 400 :error :db/invalid-branch})))
     (branch/current-db branch-meta))))

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
                       {:status 400 :error :db/invalid-branch})))
     (-> branch-meta
         (branch/update-commit! db index-files-ch)
         branch/current-db))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  ([ledger]
   (status ledger "main"))
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

(defn close-ledger
  "Shuts down ledger and resources."
  [{:keys [cache state] :as _ledger}]
  (reset! state {:closed? true})
  (reset! cache {}))

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [ledger expanded-commit]
  (go-try
    (let [[commit-jsonld _proof] (commit-storage/verify-commit expanded-commit)

          branch     (-> expanded-commit
                         (get-first-value const/iri-branch)
                         keyword)
          commit-t   (-> expanded-commit
                         (get-first const/iri-data)
                         (get-first-value const/iri-fluree-t))
          current-db (current-db ledger branch)
          current-t  (:t current-db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond

        (= commit-t (flake/next-t current-t))
        (let [db-address     (-> commit-jsonld
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-address))
              commit-storage (-> ledger :conn :store)
              db-data-jsonld (<? (commit-storage/read-commit-jsonld commit-storage db-address))
              updated-db     (<? (transact/-merge-commit current-db commit-jsonld db-data-jsonld))]
          (update-commit! ledger branch updated-db))

        ;; missing some updates, dump in-memory ledger forcing a reload
        (flake/t-after? commit-t (flake/next-t current-t))
        (do
          (log/debug "Received commit update that is more than 1 ahead of current ledger state. "
                     "Will dump in-memory ledger and force a reload: " (:alias ledger))
          (close-ledger ledger)
          false)

        (= commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however we already have this commit so not applying: " current-t)
          false)

        (flake/t-before? commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however, latest t is more current: " current-t)
          false)))))

(defrecord Ledger [conn id address alias did state cache commit-storage
                   index-storage reasoner])

(defn initial-state
  [branches current-branch]
  {:closed?  false
   :branches branches
   :branch   current-branch
   :graphs   {}})

(defn instantiate
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [conn ledger-alias ledger-address branch commit-catalog index-catalog publishers
   indexing-opts did latest-commit]
  (let [branches {branch (branch/state-map ledger-alias branch commit-catalog index-catalog
                                           publishers latest-commit indexing-opts)}]
    (map->Ledger {:conn                 conn
                  :id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                ledger-alias
                  :address              ledger-address
                  :commit-catalog       commit-catalog
                  :index-catalog        index-catalog
                  :cache                (atom {})
                  :reasoner             #{}})))

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
  [{:keys [conn alias primary-address publish-addresses commit-catalog index-catalog
           publishers]}
   {:keys [did branch indexing] :as opts}]
  (go-try
    (let [ledger-alias*  (normalize-alias alias)
          ;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (or (:fluree.db.json-ld.migrate.sid/time opts)
                             (util/current-time-iso))
          genesis-commit (<? (commit-storage/write-genesis-commit
                               commit-catalog alias branch publish-addresses init-time))]
      (instantiate conn ledger-alias* primary-address branch commit-catalog index-catalog
                   publishers indexing did genesis-commit))))
