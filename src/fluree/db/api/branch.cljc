(ns fluree.db.api.branch
  "Internal branch operations for Fluree DB.
  This namespace contains the implementation logic for branch management."
  (:require [fluree.db.connection :as connection]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn create-branch!
  "Creates a new branch from an existing branch.
  
  Parameters:
    conn - Connection object
    new-branch-spec - Full branch spec (e.g., 'ledger:new-branch')
    from-branch-spec - Source branch spec (e.g., 'ledger:old-branch')
    from-commit - (optional) Specific commit ID to branch from, defaults to latest
    
  Returns the new branch metadata."
  [conn new-branch-spec from-branch-spec from-commit]
  (go-try
    (let [[ledger-id new-branch] (util.ledger/ledger-parts new-branch-spec)
          [from-ledger-id from-branch] (util.ledger/ledger-parts from-branch-spec)]

      (when (not= ledger-id from-ledger-id)
        (throw (ex-info "Cannot create branch across different ledgers"
                        {:status 400 :error :db/invalid-branch-operation})))

      ;; Load source ledger to get its current commit
      (let [source-ledger (<? (connection/load-ledger conn from-branch-spec))
            source-db (ledger/current-db source-ledger)
            source-commit (or from-commit (get-in source-db [:commit :id]))

            ;; Create branch metadata
            created-at (util/current-time-iso)
            branch-metadata {:created-at created-at
                             :created-from {"f:branch" from-branch
                                            "f:commit" {"@id" source-commit}}}

            ;; Copy cuckoo filter from source branch to new branch (if storage supports it)
            index-catalog (:index-catalog source-db)
            _ (when (and index-catalog (:storage index-catalog))
                ;; Read the source branch's filter and copy it if it exists
                (when-let [source-filter (<? (cuckoo/read-filter index-catalog ledger-id from-branch))]
                  (<? (cuckoo/write-filter index-catalog ledger-id new-branch
                                           (:t source-db) source-filter))))

            ;; Prepare commit for new branch
            source-commit-map (:commit source-db)
            compact-commit (-> source-commit-map
                               commit-data/->json-ld
                               (assoc "alias" new-branch-spec
                                      "branch" new-branch
                                      "branchMetadata" branch-metadata))

            ;; Publish to nameservice
            primary-publisher (:primary-publisher conn)
            _ (when primary-publisher
                (<? (nameservice/publish primary-publisher compact-commit)))]

        {:name new-branch
         :created-at created-at
         :created-from {:branch from-branch :commit source-commit}
         :head source-commit}))))

(defn list-branches
  "Lists all available branches for a ledger.
  
  Parameters:
    conn - Connection object
    ledger-alias - Ledger alias string (without branch)
    
  Returns a vector of branch names."
  [conn ledger-alias]
  (go-try
    (log/info "list-branches for ledger:" ledger-alias)
    (if-some [primary-publisher (:primary-publisher conn)]
      ;; Get all nameservice records and filter for this ledger's branches
      (let [_ (log/info "Getting all nameservice records...")
            records (<? (nameservice/all-records primary-publisher))
            _ (log/info "Got" (count records) "nameservice records")
            ;; Filter for this ledger's branches
            branches (distinct
                      (for [record records
                            :let [;; The ledger field is an object with @id in the nameservice records
                                  ledger-obj (get record "f:ledger")
                                  ledger-name (if (map? ledger-obj)
                                                (get ledger-obj "@id")
                                                ledger-obj)
                                  branch-name (get record "f:branch")]
                            :when (and (= ledger-name ledger-alias) branch-name)]
                        branch-name))]
        (log/info "Found branches:" branches "for ledger:" ledger-alias)
        (vec branches))
      ;; No nameservice available
      (throw (ex-info "No nameservice available for querying branches"
                      {:status 400 :error :db/no-nameservice})))))

(defn branch-info
  "Returns detailed information about a specific branch.
  
  Parameters:
    conn - Connection object
    branch-spec - Full branch spec (e.g., \"ledger:branch\")
    
  Returns branch metadata including creation info, head commit, etc."
  [conn branch-spec]
  (go-try
    ;; Load the ledger for this branch (which already knows its branch)
    (let [branch-ledger (<? (connection/load-ledger conn branch-spec))]
      ;; Get the branch info directly from the loaded ledger
      (<? (ledger/branch-info branch-ledger)))))

(defn delete-branch!
  "Deletes a branch.
  
  Parameters:
    conn - Connection object
    branch-spec - Full branch spec to delete (e.g., \"ledger:branch\")
    
  Cannot delete the default branch or protected branches.
  Returns when deletion is complete."
  [conn branch-spec]
  (go-try
    (let [[_ledger-id branch] (util.ledger/ledger-parts branch-spec)
          ;; First check if it's the main branch (handle nil as main too)
          _ (when (or (= branch "main") (nil? branch))
              (throw (ex-info "Cannot delete the main branch"
                              {:status 400 :error :db/cannot-delete-main-branch})))
          ;; Load the branch to check if it exists and is protected
          ledger (<? (connection/load-ledger conn branch-spec))
          ;; Get branch info to check protection status
          branch-info (<? (ledger/branch-info ledger))
          _ (when (:protected branch-info)
              (throw (ex-info (str "Cannot delete protected branch: " branch)
                              {:status 400 :error :db/cannot-delete-protected-branch})))
          ;; Now delete the branch from nameservice
          primary-publisher (:primary-publisher conn)
          ;; Also delete the cuckoo filter for this branch
          index-catalog (:index-catalog ledger)
          [ledger-id branch-name] (util.ledger/ledger-parts branch-spec)]
      (if primary-publisher
        (do
          (<? (nameservice/retract primary-publisher branch-spec))
          ;; Remove from connection cache and subscriptions
          (ns-subscribe/release-ledger conn branch-spec)
          ;; Delete the cuckoo filter file for this branch
          (when (and index-catalog (:storage index-catalog))
            (<? (cuckoo/delete-filter index-catalog ledger-id branch-name))))
        (throw (ex-info "No nameservice available for branch deletion"
                        {:status 400 :error :db/no-nameservice})))
      {:deleted branch-spec})))

(defn rename-branch!
  "Renames a branch.
  
  Parameters:
    conn - Connection object
    old-branch-spec - Current branch spec (e.g., \"ledger:old-branch\")
    new-branch-spec - New branch spec (e.g., \"ledger:new-branch\")
    
  Returns when rename is complete."
  [conn old-branch-spec new-branch-spec]
  (go-try
    (let [[old-ledger-id old-branch] (util.ledger/ledger-parts old-branch-spec)
          [new-ledger-id new-branch] (util.ledger/ledger-parts new-branch-spec)]

      (when (not= old-ledger-id new-ledger-id)
        (throw (ex-info "Cannot rename branch across different ledgers"
                        {:status 400 :error :db/invalid-branch-operation})))

      (when (or (= old-branch "main") (nil? old-branch))
        (throw (ex-info "Cannot rename the main branch"
                        {:status 400 :error :db/cannot-rename-main-branch})))

      ;; Load the branch to get its current state
      (let [ledger (<? (connection/load-ledger conn old-branch-spec))
            branch-info (<? (ledger/branch-info ledger))
            _ (when (:protected branch-info)
                (throw (ex-info (str "Cannot rename protected branch: " old-branch)
                                {:status 400 :error :db/cannot-rename-protected-branch})))

            ;; Get current commit
            source-db (ledger/current-db ledger)
            source-commit-map (:commit source-db)

            ;; Prepare updated commit with new branch name
            updated-commit (-> source-commit-map
                               commit-data/->json-ld
                               (assoc "alias" new-branch-spec
                                      "branch" new-branch
                                    ;; Preserve branch metadata
                                      "branchMetadata" (select-keys branch-info
                                                                    [:created-at :created-from
                                                                     :protected :description])))

            ;; Publish new branch and retract old
            primary-publisher (:primary-publisher conn)]

        (if primary-publisher
          (do
            ;; Create new branch record
            (<? (nameservice/publish primary-publisher updated-commit))
            ;; Delete old branch record
            (<? (nameservice/retract primary-publisher old-branch-spec))
            {:renamed-from old-branch-spec
             :renamed-to new-branch-spec})
          (throw (ex-info "No nameservice available for branch renaming"
                          {:status 400 :error :db/no-nameservice})))))))