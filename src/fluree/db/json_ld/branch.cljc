(ns fluree.db.json-ld.branch
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

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
  ([ledger] (branch-meta ledger (current-branch ledger)))
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
  [current-branch branch-name]
  (let [{:keys [t commit idx dbs]
         :or   {commit 0, dbs (list)}} current-branch
        ;; is current branch uncommitted? If so, when committing new branch we must commit current-branch too
        uncommitted? (and t (> t commit))]
    {:name      branch-name
     :t         t
     :commit    commit
     :idx       idx
     :latest-db nil
     :from      (-> current-branch
                    (select-keys [:name :t])
                    (assoc :uncommitted? uncommitted?))}))

(defn update-db
  "Updates the latest staged db and returns new branch data."
  [{:keys [t] :as branch-data} db]
  (let [{db-t :t} db]
    (when (and t (not= db-t (dec t)))
      (throw (ex-info (str "Unable to create new DB version on ledger, latest 't' value is: "
                           t " however new db t value is: " db-t ".")
                      {:status 500 :error :db/invalid-time})))
    (-> branch-data
        (assoc :t db-t
               :latest-db db))))

(defn latest-db
  "Returns latest db from branch data"
  [branch-data]
  (:latest-db branch-data))

(defn latest-commit
  "Returns latest commit info from branch-data"
  [branch-data]
  (:commit branch-data))


;; TODO
(defn branch
  "Creates, or changes, a ledger's branch"
  [ledger branch]
  (let [{:keys [state]} ledger
        {:keys [branches branch]} @state
        [branch-t [branch-current branch-commit]] branch
        branch*     (util/str->keyword branch)
        new?        (contains? branches branch*)
        is-current? (= branch)]

    )

  )