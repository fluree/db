(ns fluree.db.api.branch
  "Internal branch operations for Fluree DB.
  This namespace contains the implementation logic for branch management."
  (:require [fluree.db.connection :as connection]
            [fluree.db.constants :as const]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.branch :as util.branch]
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
            source-commit-id (or from-commit (get-in source-db [:commit :id]))

            ;; Create branch metadata
            metadata {:created-at (util/current-time-iso)
                      :source-branch from-branch
                      :source-commit source-commit-id}

            ;; Copy cuckoo filter from source branch to new branch (if storage supports it)
            index-catalog (:index-catalog source-db)
            _ (when (and index-catalog (:storage index-catalog))
                ;; Read the source branch's filter and copy it if it exists
                (when-let [source-filter (<? (cuckoo/read-filter index-catalog ledger-id from-branch))]
                  (<? (cuckoo/write-filter index-catalog ledger-id new-branch
                                           (:t source-db) source-filter))))

            ;; Prepare commit for new branch with flat metadata fields
            source-commit-map (:commit source-db)
            compact-commit (-> source-commit-map
                               commit-data/->json-ld
                               (assoc "alias" new-branch-spec
                                      "branch" new-branch)
                               (util.branch/augment-commit-with-metadata metadata))

            primary-publisher (:primary-publisher conn)
            secondary-publishers (:secondary-publishers conn)
            _ (log/debug "create-branch! publishing commit for" new-branch-spec
                         "with primary-publisher?" (boolean primary-publisher))
            _ (when primary-publisher
                (log/debug "Publishing commit with alias:" (get compact-commit "alias")
                           "address:" (get compact-commit "address")
                           "t:" (get-in compact-commit ["data" "t"]))
                (<? (nameservice/publish primary-publisher compact-commit))
                (log/debug "Published commit for" new-branch-spec)
                ;; Also publish to secondary publishers asynchronously
                (nameservice/publish-to-all compact-commit secondary-publishers))]

        (util.branch/branch-creation-response new-branch metadata source-commit-id)))))

(defn- same-ledger?
  "Check if a nameservice record belongs to a specific ledger.
  The ledger field in nameservice records can be either a string or an object with @id."
  [ledger-alias record]
  (let [ledger-obj (get record "f:ledger")
        ledger-name (if (map? ledger-obj)
                      (get ledger-obj "@id")
                      ledger-obj)]
    (= ledger-name ledger-alias)))

(defn- main-branch?
  "Check if a branch name represents the main/default branch.
   Returns true for 'main' or nil (which defaults to main)."
  [branch-name]
  (or (= branch-name const/default-branch-name)
      (nil? branch-name)))

(defn list-branches
  "Lists all available branches for a ledger.
  
  Parameters:
    conn - Connection object
    ledger-alias - Ledger alias string (without branch)
    
  Returns a vector of branch names."
  [conn ledger-alias]
  (go-try
    (log/debug "list-branches for ledger:" ledger-alias)
    (if-some [primary-publisher (:primary-publisher conn)]
      (let [records (<? (nameservice/all-records primary-publisher))
            ;; Extract branches for the specified ledger
            branches (->> records
                          (filter (partial same-ledger? ledger-alias))
                          (mapv #(get % "f:branch")))]
        (log/debug "Found branches:" branches "for ledger:" ledger-alias)
        branches)
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
    (let [branch-ledger (<? (connection/load-ledger conn branch-spec))]
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
    (let [branch-spec* (util.ledger/ensure-ledger-branch branch-spec)
          [ledger-id branch] (util.ledger/ledger-parts branch-spec*)
          _ (when (main-branch? branch)
              (throw (ex-info "Cannot delete the main branch. Use the drop API to remove the entire ledger."
                              {:status 400 :error :db/cannot-delete-main-branch})))
          ledger (<? (connection/load-ledger conn branch-spec*))
          branch-info (<? (ledger/branch-info ledger))
          ;; Also get the index catalog for cuckoo filter deletion
          index-catalog (:index-catalog ledger)]
      (when (:protected branch-info)
        (throw (ex-info (str "Cannot delete protected branch: " branch)
                        {:status 400 :error :db/cannot-delete-protected-branch})))
      (if-let [primary-publisher (:primary-publisher conn)]
        (do
          (<? (nameservice/retract primary-publisher branch-spec*))
          (ns-subscribe/release-ledger conn branch-spec*)
          ;; Delete the cuckoo filter file for this branch
          (when (and index-catalog (:storage index-catalog))
            (<? (cuckoo/delete-filter index-catalog ledger-id branch))))
        (throw (ex-info "No nameservice available for branch deletion"
                        {:status 400 :error :db/no-nameservice})))
      {:deleted branch-spec*})))

(defn rename-branch!
  "Renames a branch.
  
  Parameters:
    conn - Connection object
    old-branch-spec - Current branch spec (e.g., \"ledger:old-branch\")
    new-branch-spec - New branch spec (e.g., \"ledger:new-branch\")
    
  Returns when rename is complete."
  [conn old-branch-spec new-branch-spec]
  (go-try
    (let [old-branch-spec* (util.ledger/ensure-ledger-branch old-branch-spec)
          new-branch-spec* (util.ledger/ensure-ledger-branch new-branch-spec)
          [old-ledger-id old-branch] (util.ledger/ledger-parts old-branch-spec*)
          [new-ledger-id new-branch] (util.ledger/ledger-parts new-branch-spec*)]

      (when (not= old-ledger-id new-ledger-id)
        (throw (ex-info "Cannot rename branch across different ledgers"
                        {:status 400 :error :db/invalid-branch-operation})))

      (when (main-branch? old-branch)
        (throw (ex-info "Cannot rename the main branch"
                        {:status 400 :error :db/cannot-rename-main-branch})))

      ;; Load the branch to get its current state
      (let [ledger (<? (connection/load-ledger conn old-branch-spec*))
            branch-info (<? (ledger/branch-info ledger))
            _ (when (:protected branch-info)
                (throw (ex-info (str "Cannot rename protected branch: " old-branch)
                                {:status 400 :error :db/cannot-rename-protected-branch})))

            source-db (ledger/current-db ledger)
            source-commit-map (:commit source-db)

            updated-commit (-> source-commit-map
                               commit-data/->json-ld
                               (assoc "alias" new-branch-spec*
                                      "branch" new-branch)
                               (util.branch/augment-commit-with-metadata branch-info))]

        (if-let [primary-publisher (:primary-publisher conn)]
          (do
            (<? (nameservice/publish primary-publisher updated-commit)) ;; Create new branch record 
            (<? (nameservice/retract primary-publisher old-branch-spec*)) ;; Delete old branch record
            {:renamed-from old-branch-spec*
             :renamed-to new-branch-spec*})
          (throw (ex-info "No nameservice available for branch renaming"
                          {:status 400 :error :db/no-nameservice})))))))