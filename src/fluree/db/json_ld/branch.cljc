(ns fluree.db.json-ld.branch
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log])
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
         :or   {commit 0, t 0, dbs (list)}} current-branch
        ;; is current branch uncommitted? If so, when committing new branch we must commit current-branch too
        uncommitted? (and t (> t commit))]
    {:name        branch-name
     :t           t
     :commit      commit                                    ;;  't' value of latest commit
     :commit-meta nil                                       ;; commit metadata used by ledger method (e.g. ipfs) to store relevant metadata specific to the method
     :idx         idx
     :latest-db   nil                                       ;; latest staged db (if different from commit-db)
     :commit-db   nil                                       ;; latest committed db
     :from        (-> current-branch
                      (select-keys [:name :t])
                      (assoc :uncommitted? uncommitted?))}))

(defn update-db
  "Updates the latest staged db and returns new branch data."
  [{:keys [t] :as branch-data} db]
  (let [{db-t :t} db]
    (let [next-t (dec t)]
      (if (or (= next-t db-t)
              (= t db-t)
              (zero? t))                                    ;; if zero, means we are placing in new db - likely loaded from disk
        (-> branch-data
            (assoc :t db-t
                   :latest-db db))
        (throw (ex-info (str "Unable to create new DB version on ledger, latest 't' value is: "
                             t " however new db t value is: " db-t ".")
                        {:status 500 :error :db/invalid-time}))))))

(defn update-commit
  [branch-data {:keys [commit] :as db} force?]
  (let [{db-t :t} db
        next-t? (= db-t (dec (:commit branch-data)))
        {:keys [meta]} commit]
    (when-not (or next-t?
                  (zero? db-t))                             ;; zero db-t is bootstrapping, which we allow bootstrap tx at zero
      (throw (ex-info (str "Commit failed, latest committed db is " (:commit branch-data)
                           " and you are trying to commit at db at t value of: "
                           db-t ". These should be one apart. Likely db was "
                           "updated by another user or process.")
                      {:status 400 :error :db/invalid-commit})))
    (-> branch-data
        (update-db db)
        (assoc :commit db-t)
        (assoc :commit-meta meta)
        (assoc :commit-db db))))

(defn latest-db
  "Returns latest db from branch data"
  [branch-data]
  (:latest-db branch-data))

(defn latest-commit-db
  "Returns latest committed db"
  [branch-data]
  (:commit-db branch-data))

(defn latest-commit
  "Returns latest commit info from branch-data"
  [branch-data]
  (:commit branch-data))

(defn name
  "Returns branch name from branch metadata"
  [branch-data]
  (:name branch-data))


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