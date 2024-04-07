(ns fluree.db.json-ld.branch
  (:require [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log :include-macros true])

  (:refer-clojure :exclude [name]))

#?(:clj (set! *warn-on-reflection* true))

;; branch operations on json-ld ledger

(defn current-branch
  "Returns current branch name."
  [ledger]
  (-> ledger
      :state
      deref
      :branch))

(defn branch-meta
  "Returns branch map data for current branch, or specified branch"
  ([ledger]
   (branch-meta ledger (current-branch ledger)))
  ([ledger branch]
   (-> ledger
       :state
       deref
       :branches
       (get branch))))

;; TODO - if you branch from an uncommitted branch, and then commit, commit the current-branch too
(defn new-branch-map
  "Returns a new branch name for specified branch name off of
  supplied current-branch."
  [current-branch-map alias branch ns-addresses]
  (let [{:keys [t latest-db]
         :or   {t 0}} current-branch-map]
    {:name      branch
     :t         t
     :commit    (commit-data/blank-commit alias branch ns-addresses)
     :latest-db latest-db
     :from      (-> current-branch-map
                    (select-keys [:name :t :commit]))}))

(defn updated-index?
  [current-commit new-commit]
  (flake/t-before? (commit-data/index-t current-commit)
                   (commit-data/index-t new-commit)))

(defn update-db
  "Updates the latest staged db and returns new branch data."
  [{:keys [t] :as branch-data} {db-t :t, :as db}]
  (if (or (= (flake/next-t t) db-t)
          (= t db-t)
          (zero? t)) ;; when loading a ledger from disk, 't' will be zero but ledger will be >= 1
    (-> branch-data
        (assoc :t db-t
               :latest-db db))
    (throw (ex-info (str "Unable to create new DB version on ledger, latest 't' value is: "
                         t " however new db t value is: " db-t ".")
                    {:status 500 :error :db/invalid-time}))))

(defn update-commit
  "There are 3 t values, the db's t, the 'commit' attached to the db's t, and
  then the ledger's latest commit t (in branch-data). The db 't' and db commit 't'
  should be the same at this point (just after committing the db). The ledger's latest
  't' should be the same (if just updating an index) or after the db's 't' value."
  [branch-data db]
  (let [{db-commit :commit, db-t :t} db
        {branch-commit :commit} branch-data
        ledger-t       (commit-data/t branch-commit)
        commit-t       (commit-data/t db-commit)
        _              (when-not (= db-t commit-t)
                         (throw (ex-info (str "Unexpected Error. Db's t value and commit's t value are not the same: "
                                              db-t " and " commit-t " respectively.")
                                         {:status 500 :error :db/invalid-db})))
        index-updated? (updated-index? branch-commit db-commit)
        db*            (if index-updated?
                         (assoc db :commit (commit-data/use-latest-index db-commit branch-commit))
                         db)]
    (when-not (or (nil? ledger-t)
                  (and index-updated?
                       (not (flake/t-after? commit-t ledger-t))) ; index update may come after multiple commits
                  (= commit-t (inc ledger-t)))
      (throw (ex-info (str "Commit failed, latest committed db is " ledger-t
                           " and you are trying to commit at db at t value of: "
                           commit-t ". These should be one apart. Likely db was "
                           "updated by another user or process.")
                      {:status 400 :error :db/invalid-commit})))
    (-> branch-data
        (update-db db*)
        (assoc :commit (:commit db*)))))

(defn latest-db
  "Returns latest db from branch data"
  [branch-data]
  (:latest-db branch-data))
