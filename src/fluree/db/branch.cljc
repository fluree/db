(ns fluree.db.branch
  (:require [clojure.core.async :as async :refer [go <! go-loop]]
            [fluree.db.async-db :as async-db]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn same-t?
  [current-commit indexed-commit]
  (let [current-t (commit-data/t current-commit)
        indexed-t (commit-data/t indexed-commit)]
    (= current-t indexed-t)))

(defn newer-commit?
  [current-commit indexed-commit]
  (let [current-t (commit-data/t current-commit)
        indexed-t (commit-data/t indexed-commit)]
    (> current-t indexed-t)))

(defn same-index?
  [commit-x commit-y]
  (let [x-index-t (commit-data/index-t commit-x)
        y-index-t (commit-data/index-t commit-y)]
    (= x-index-t y-index-t)))

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
  [alias branch commit-catalog index-catalog commit-map]
  (let [commit-jsonld (commit-map->commit-jsonld commit-map)]
    (async-db/load alias branch commit-catalog index-catalog
                   commit-jsonld commit-map nil)))

(defn update-index-async
  "Returns an updated async-db with the index changes.

  Because we are updating the index in an atom, we want to
  return immediately - and for a large amount of novelty,
  updating the db to reflect the latest index can take some time
  which would lead to atom contention."
  [{:keys [alias commit branch t] :as current-db} index-map]
  (if (async-db/db? current-db)
    (dbproto/-index-update current-db index-map)
    (let [updated-commit (assoc commit :index index-map)
          updated-db     (async-db/->async-db alias branch updated-commit t)]
      (go ;; update index in the background, return updated db immediately
        (->> (dbproto/-index-update current-db index-map)
             (async-db/deliver! updated-db)))
      updated-db)))

(defn update-index
  [{current-commit :commit, current-db :current-db, :as current-state}
   {indexed-commit :commit, :as indexed-db}]
  (if (same-t? current-commit indexed-commit)
    (if (newer-index? indexed-commit current-commit)
      (assoc current-state
             :commit     indexed-commit
             :current-db indexed-db)
      current-state)
    (if (newer-commit? current-commit indexed-commit)
      (if (newer-index? indexed-commit current-commit)
        (let [latest-db (update-index-async current-db (:index indexed-commit))]
          (assoc current-state
                 :commit     (:commit latest-db)
                 :current-db latest-db))
        current-state)
      (do (log/warn "Rejecting index update for future commit at transaction:"
                    (commit-data/t indexed-commit)
                    "because it is after the current transaction value:"
                    (commit-data/t current-commit))
          current-state))))

(defn reload-with-index
  [{:keys [commit-catalog index-catalog commit] :as _db} alias branch index]
  (let [indexed-commit (assoc commit :index index)]
    (load-db alias branch commit-catalog index-catalog indexed-commit)))

(defn use-latest-db
  "Returns the most recent db from branch-state if it matches
  the target 't' and index-t values of the next index job from
  index-queue.

  Most of the time the current state already has the prepared db,
  in the occasion there is a difference then we must build the target db."
  [{:keys [commit] :as _db-to-index} latest-idx-commit branch-state]
  (let [{latest-commit :commit, :as latest-db} (:current-db @branch-state)]
    (when (and (same-t? commit latest-commit)
               (same-index? latest-idx-commit latest-commit))
      latest-db)))

(defn use-latest-index
  [{db-commit :commit, :as db} idx-commit alias branch branch-state]
  (if (newer-index? idx-commit db-commit)
    (let [updated-db (or (use-latest-db db idx-commit branch-state)
                         (try* (dbproto/-index-update db (:index idx-commit))
                               (catch* e (log/error e "Exception updating db with new index, attempting full reload. Exception:" (ex-message e))
                                       (reload-with-index db alias branch (:index idx-commit)))))]
      updated-db)
    db))

(defn index-queue
  [alias branch publishers branch-state]
  (let [buf   (async/sliding-buffer 1)
        queue (async/chan buf)]
    (go-loop [last-index-commit nil]
      (when-let [{:keys [db index-files-ch]} (<! queue)]
        (let [db* (use-latest-index db last-index-commit alias branch branch-state)]
          (if-let [indexed-db (try* (<? (indexer/index db* index-files-ch)) ;; indexer/index always returns a FlakeDB (never AsyncDB)
                                    (catch* e
                                      (log/error e "Error updating index")))]
            (let [[{prev-commit :commit} {indexed-commit :commit}]
                  (swap-vals! branch-state update-index indexed-db)]
              (when (not= prev-commit indexed-commit)
                (let [commit-jsonld (commit-data/->json-ld indexed-commit)]
                  (nameservice/publish-to-all commit-jsonld publishers)))
              (recur indexed-commit))
            (recur last-index-commit)))))
    queue))

(defn enqueue-index!
  [idx-q db index-files-ch]
  (async/put! idx-q {:db db, :index-files-ch index-files-ch}))

(defn state-map
  "Returns a branch map for specified branch name at supplied commit"
  ([ledger-alias branch-name commit-catalog index-catalog publishers commit-jsonld]
   (state-map ledger-alias branch-name commit-catalog index-catalog publishers commit-jsonld nil))
  ([ledger-alias branch-name commit-catalog index-catalog publishers commit-jsonld indexing-opts]
   (let [commit-map (commit-data/jsonld->clj commit-jsonld)
         initial-db (async-db/load ledger-alias branch-name commit-catalog index-catalog
                                   commit-jsonld commit-map indexing-opts)
         state      (atom {:commit     commit-map
                           :current-db initial-db})
         idx-q      (index-queue ledger-alias branch-name publishers state)]
     {:name        branch-name
      :alias       ledger-alias
      :state       state
      :index-queue idx-q})))

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
   {new-commit :commit, :as new-db}]
  (if (next-commit? current-commit new-commit)
    (if (newer-index? current-commit new-commit)
      (let [latest-db (update-index-async new-db (:index current-commit))]
        (assoc current-state
               :commit     (:commit latest-db)
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
                       (swap! update-commit (policy/root-db new-db))
                       :current-db)]
    (enqueue-index! index-queue updated-db index-files-ch)
    branch-map))

(defn current-db
  "Returns current db from branch data"
  [{:keys [state] :as _branch-map}]
  (:current-db @state))
