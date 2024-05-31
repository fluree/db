(ns fluree.db.json-ld.branch
  (:require [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.indexer :as indexer]
            [fluree.json-ld :as json-ld]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.database.async :as async-db]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.core.async :as async :refer [<! go-loop]]
            [fluree.db.index :as index]))

#?(:clj (set! *warn-on-reflection* true))

(defn same-commit?
  [current-commit indexed-commit]
  (let [current-t (commit-data/t current-commit)
        indexed-t (commit-data/t indexed-commit)]
    (and (= current-t indexed-t)
         (= (:id current-t)
            (:id indexed-t)))))

(defn older-commit?
  [current-commit indexed-commit]
  (let [current-t (commit-data/t current-commit)
        indexed-t (commit-data/t indexed-commit)]
    (> current-t indexed-t)))

(defn newer-index?
  [commit-x commit-y]
  (let [x-index-t (commit-data/index-t commit-x)
        y-index-t (commit-data/index-t commit-y)]
    (and (some? x-index-t)
         (or (nil? y-index-t)
             (> x-index-t y-index-t)))))

(defn commit-map->commit-jsonld
  [commit-map]
  (-> commit-map commit-data/->json-ld json-ld/expand))

(defn load-db
  [conn alias branch commit]
  (let [commit-jsonld (commit-map->commit-jsonld commit)]
    (async-db/load conn alias branch commit-jsonld)))

(defn update-index
  [{current-commit :commit, :as current-state}
   {:keys [conn alias branch], indexed-commit :commit, :as indexed-db}]
  (if (same-commit? current-commit indexed-commit)
    (if (newer-index? indexed-commit current-commit)
      (assoc current-state
             :commit     indexed-commit
             :current-db indexed-db)
      current-state)
    (if (older-commit? current-commit indexed-commit)
      (if (newer-index? indexed-commit current-commit)
        (let [latest-index  (:index indexed-commit)
              latest-commit (assoc current-commit :index latest-index)
              latest-db     (load-db conn alias branch latest-commit)]
          (assoc current-state
                 :commit     latest-commit
                 :current-db latest-db))
        current-state)
      (do (log/warn "Rejecting index update for future commit at transaction:"
                    (commit-data/t indexed-commit)
                    "because it is after the current transaction value:"
                    (commit-data/t current-commit))
          current-state))))

(defn use-latest-index
  [{db-commit :commit, :as db} conn alias branch idx-commit]
  (if (and idx-commit
           (newer-index? idx-commit db-commit))
    (let [latest-index  (:index idx-commit)
          latest-commit (assoc db-commit :index latest-index)]
      (load-db conn alias branch latest-commit))
    db))

(defn index-queue
  [conn alias branch branch-state]
  (let [buf   (async/sliding-buffer 1)
        queue (async/chan buf)]
    (go-loop [last-index-commit nil]
      (when-let [{:keys [db index-files-ch]} (<! queue)]
        (let [db* (use-latest-index db conn alias branch last-index-commit)]
          (if-let [indexed-db (try* (<? (indexer/index db* index-files-ch))
                                    (catch* e
                                      (log/error e "Error updating index")))]
            (do (swap! branch-state update-index indexed-db)
                (recur (:commit indexed-db)))
            (recur last-index-commit)))))
    queue))

(defn enqueue-index!
  [idx-q db index-files-ch]
  (async/put! idx-q {:db db, :index-files-ch index-files-ch}))

(defn state-map
  "Returns a branch map for specified branch name at supplied commit"
  [conn ledger-alias branch-name commit-jsonld]
  (let [initial-db (async-db/load conn ledger-alias branch-name commit-jsonld)
        commit-map (commit-data/jsonld->clj commit-jsonld)
        state      (atom {:commit     commit-map
                          :current-db initial-db})
        idx-q      (index-queue conn ledger-alias branch-name state)]
    {:name        branch-name
     :conn        conn
     :alias       ledger-alias
     :state       state
     :index-queue idx-q}))

(defn next-commit?
  [current-commit new-commit]
  (let [current-t (commit-data/t current-commit)
        new-t     (commit-data/t new-commit)]
    (and (or (nil? current-t)
             (= new-t (inc current-t)))
         (= (-> new-commit :previous :id)
            (:id current-commit)))))

(defn update-commit
  [{current-commit :commit, :as current-state}
   {:keys [conn alias branch], new-commit :commit, :as new-db}]
  (if (next-commit? current-commit new-commit)
    (if (newer-index? current-commit new-commit)
      (let [latest-index  (:index current-commit)
            latest-commit (assoc new-commit :index latest-index)
            latest-db     (load-db conn alias branch latest-commit)]
        (assoc current-state
               :commit     latest-commit
               :current-db latest-db))
      (assoc current-state
             :commit     new-commit
             :current-db new-db))
    (do
      (log/warn "Commit update failure.\n  Current commit:" current-commit
                "\n  New commit:" new-commit)
      (throw (ex-info (str "Commit failed, latest committed db is "
                           (commit-data/t current-commit)
                           " and you are trying to commit at db at t value of: "
                           (commit-data/t new-commit)
                           ". These should be one apart. Likely db was "
                           "updated by another user or process.")
                      {:status 400 :error :db/invalid-commit})))))

(defn update-commit!
  "There are 3 t values, the db's t, the 'commit' attached to the db's t, and
  then the ledger's latest commit t (in branch-data). The db 't' and db commit 't'
  should be the same at this point (just after committing the db). The ledger's latest
  't' should be the same (if just updating an index) or after the db's 't' value."
  [{:keys [state index-queue] :as branch-map} new-db index-files-ch]
  (let [updated-db (-> state
                       (swap! update-commit new-db)
                       :current-db)]
    (enqueue-index! index-queue updated-db index-files-ch)
    branch-map))

(defn current-db
  "Returns current db from branch data"
  [{:keys [state] :as _branch-map}]
  (:current-db @state))

(defn current-commit
  [{:keys [state] :as _branch-map}]
  (:commit @state))
