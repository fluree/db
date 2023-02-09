(ns fluree.db.json-ld.branch
  (:require [fluree.db.util.core :as util]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as dbproto]
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
  [current-branch-map alias branch]
  (let [{:keys [t commit]
         :or   {t 0}} current-branch-map
        ;; is current branch uncommitted? If so, when committing new branch we must commit current-branch too
        uncommitted? (and commit (> t (-> commit :db :t)))]
    {:name      branch
     :t         t
     :commit    (commit-data/blank-commit {:alias  alias
                                           :branch (util/keyword->str branch)})
     ;:index     {:id               nil                      ;; unique id (hash of root) of index
     ;            :address          nil                      ;; address to get to index 'root'
     ;            :db               {}                       ;; db commit-map object of indexed db
     ;            :update-commit-fn nil                      ;; function to call when index is updated to create a new commit with updated index address
     ;            :spot             nil                      ;; top level branch of each index , eliminates file lookup of index root
     ;            :psot             nil
     ;            :post             nil
     ;            :opst             nil
     ;            :tspo             nil}
     :latest-db nil                                         ;; latest staged db (if different from commit-db)
     :commit-db nil                                         ;; latest committed db
     :from      (-> current-branch-map
                    (select-keys [:name :t])
                    (assoc :uncommitted? uncommitted?))}))

(defn update-db
  "Updates the latest staged db and returns new branch data."
  [{:keys [t index] :as branch-data} {db-t :t, :as db}]
  (if (or (= (dec t) db-t)
          (= t db-t)
          (zero? t))                                        ;; when loading a ledger from disk, 't' will be zero but ledger will be >= 1
    (let [db* (dbproto/-index-update db index)]
      (-> branch-data
          (assoc :t db-t
                 :latest-db db*)))
    (throw (ex-info (str "Unable to create new DB version on ledger, latest 't' value is: "
                         t " however new db t value is: " db-t ".")
                    {:status 500 :error :db/invalid-time}))))

(defn update-commit-with-index
  "If an update-commit-fn exists in state, calls it."
  [index new-index]
  (when-let [commit-fn (:update-commit-fn index)]
    (if (fn? commit-fn)
      (commit-fn new-index)
      (log/warn "update-commit-fn in ledger's state index was not a function: " index))))

(defn update-commit
  "There are 3 t values, the db's t, the 'commit' attached to the db's t, and
  then the ledger's latest commit t (in branch-data). The db 't' and db commit 't'
  should be the same at this point (just after committing the db). The ledger's latest
  't' should be the same (if just updating an index) or after the db's 't' value."
  [branch-data db force?]
  (let [{db-commit :commit, db-t :t} db
        {branch-commit :commit} branch-data
        ledger-t       (commit-data/t branch-commit)
        commit-t       (commit-data/t db-commit)
        _              (when-not (= (- db-t) commit-t)
                         (throw (ex-info (str "Unexpected Error. Db's t value and commit's t value are not the same: "
                                              (- db-t) " and " commit-t " respectively.")
                                         {:status 500 :error :db/invalid-db})))
        updated-index? (not= (commit-data/index-t branch-commit)
                             (commit-data/index-t db-commit))
        db*            (if updated-index?
                         (assoc db :commit (commit-data/use-latest-index db-commit branch-commit))
                         db)]
    (when-not (or (nil? ledger-t)
                  (and updated-index? (>= ledger-t commit-t)) ;; index update may come after multiple commits
                  (= commit-t (inc ledger-t)))
      (throw (ex-info (str "Commit failed, latest committed db is " ledger-t
                           " and you are trying to commit at db at t value of: "
                           commit-t ". These should be one apart. Likely db was "
                           "updated by another user or process.")
                      {:status 400 :error :db/invalid-commit})))
    (-> branch-data
        (update-db db*)
        (assoc :commit (:commit db*)
               :commit-db db*))))

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

(defn latest-commit-t
  "Returns the latest commit 't' value from branch-data, or 0 (zero) if no commit yet."
  [branch-data]
  (or (commit-data/t (latest-commit branch-data))
      0))

;; TODO
#_(defn branch
    "Creates, or changes, a ledger's branch"
    [ledger branch]
    (let [{:keys [state]} ledger
          {:keys [branches branch]} @state
          [branch-t [branch-current branch-commit]] branch
          branch*     (util/str->keyword branch)
          new?        (contains? branches branch*)
          is-current? (= branch)]))
