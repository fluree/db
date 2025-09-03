(ns fluree.db.branch
  (:require [clojure.core.async :as async :refer [go <! go-loop]]
            [fluree.db.async-db :as async-db]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
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
  [combined-alias commit-catalog index-catalog commit-map]
  (let [commit-jsonld (commit-map->commit-jsonld commit-map)]
    (async-db/load combined-alias commit-catalog index-catalog
                   commit-jsonld commit-map nil)))

(defn update-index-async
  "Returns an updated async-db with the index changes.

  Because we are updating the index in an atom, we want to
  return immediately - and for a large amount of novelty,
  updating the db to reflect the latest index can take some time
  which would lead to atom contention."
  [{:keys [alias commit t] :as current-db} index-map]
  (if (async-db/db? current-db)
    (dbproto/-index-update current-db index-map)
    (let [updated-commit (assoc commit :index index-map)
          updated-db     (async-db/->async-db alias updated-commit t)]
      (go ;; update index in the background, return updated db immediately
        (let [db* (<? (dbproto/-index-update current-db index-map))]
          (async-db/deliver! updated-db db*)))
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
  [{:keys [commit-catalog index-catalog commit alias] :as _db} index]
  (let [indexed-commit (assoc commit :index index)]
    (load-db alias commit-catalog index-catalog indexed-commit)))

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
  [{db-commit :commit, :as db} idx-commit branch-state]
  (if (newer-index? idx-commit db-commit)
    (let [updated-db (or (use-latest-db db idx-commit branch-state)
                         (try* (<? (dbproto/-index-update db (:index idx-commit)))
                               (catch* e (log/error e "Exception updating db with new index, attempting full reload. Exception:" (ex-message e))
                                       (reload-with-index db (:index idx-commit)))))]
      updated-db)
    db))

(defn index-queue
  [publishers branch-state]
  (let [buf   (async/sliding-buffer 1)
        queue (async/chan buf)]
    (go-loop [last-index-commit nil]
      (when-let [{:keys [db index-files-ch complete-ch]} (<! queue)]
        (let [db* (use-latest-index db last-index-commit branch-state)
              result (try*
                       (let [indexed-db (<? (indexer/index db* index-files-ch)) ;; indexer/index always returns a FlakeDB (never AsyncDB)
                             [{prev-commit :commit} {indexed-commit :commit}]
                             (swap-vals! branch-state update-index indexed-db)]
                         (when (not= prev-commit indexed-commit)
                           (let [commit-jsonld (commit-data/->json-ld indexed-commit)]
                             (nameservice/publish-to-all commit-jsonld publishers)))
                         {:status :success, :db indexed-db, :commit indexed-commit})
                       (catch* e
                         (log/error e "Error updating index")
                         {:status :error
                          :error  (ex-info "Indexing failed"
                                           {:alias (:alias db*)
                                            :t     (:t db*)}
                                           e)}))]
          (when complete-ch
            (async/put! complete-ch result))
          (if (= :success (:status result))
            (recur (:commit result))
            (recur last-index-commit)))))
    queue))

(defn enqueue-index!
  ([idx-q db index-files-ch]
   (enqueue-index! idx-q db index-files-ch nil))
  ([idx-q db index-files-ch complete-ch]
   (async/put! idx-q {:db db, :index-files-ch index-files-ch, :complete-ch complete-ch})))

(defn state-map
  "Returns a branch map for specified branch name at supplied commit"
  ([combined-alias branch-name commit-catalog index-catalog publishers commit-jsonld]
   (state-map combined-alias branch-name commit-catalog index-catalog publishers commit-jsonld nil))
  ([combined-alias branch-name commit-catalog index-catalog publishers commit-jsonld indexing-opts]
   (let [commit-map (commit-data/jsonld->clj commit-jsonld)
         initial-db (async-db/load combined-alias commit-catalog index-catalog
                                   commit-jsonld commit-map indexing-opts)
         state      (atom {:commit     commit-map
                           :current-db initial-db})
         idx-q      (index-queue publishers state)]
     {:name          branch-name
      :alias         combined-alias
      :state         state
      :index-queue   idx-q
      :indexing-opts indexing-opts})))

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
      (let [current-t   (commit-data/t current-commit)
            new-t       (commit-data/t new-commit)
            t-ok?       (or (nil? current-t)
                            (= new-t (inc current-t)))
            current-id  (:id current-commit)
            new-prev-id (-> new-commit :previous :id)
            prev-ok?    (= new-prev-id current-id)]
        (log/warn "Commit update failure.\n  Current commit:" current-commit
                  "\n  New commit:" new-commit)
        (log/warn "Commit update failure detail"
                  {:current-t current-t
                   :new-t new-t
                   :t-ok? t-ok?
                   :current-id current-id
                   :new-prev-id new-prev-id
                   :prev-ok? prev-ok?}))
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
  [{:keys [state index-queue indexing-opts] :as branch-map} new-db index-files-ch]
  (let [updated-db (-> state
                       (swap! update-commit (policy/root-db new-db))
                       :current-db)]
    (when-not (:indexing-disabled indexing-opts)
      (enqueue-index! index-queue updated-db index-files-ch))
    branch-map))

(defn current-db
  "Returns current db from branch data"
  [{:keys [state] :as _branch-map}]
  (:current-db @state))

(defn trigger-index!
  "Manually triggers indexing for this branch's current db.
   Returns immediately with complete-ch that will receive result when indexing completes.
   The complete-ch parameter is optional - if not provided, a new channel is created."
  ([branch-map]
   (trigger-index! branch-map nil nil))
  ([branch-map index-files-ch]
   (trigger-index! branch-map index-files-ch nil))
  ([{:keys [index-queue] :as branch-map} index-files-ch complete-ch]
   (let [complete-ch (or complete-ch (async/chan 1))
         current-db (current-db branch-map)]
     (enqueue-index! index-queue current-db index-files-ch complete-ch)
     complete-ch)))
