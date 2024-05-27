(ns fluree.db.json-ld.branch
  (:require [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.indexer :as indexer]
            [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.database.async :as async-db]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.core.async :as async :refer [<! go-loop]]))

#?(:clj (set! *warn-on-reflection* true))

(defn update-index
  [current-state indexed-db]
  )

(defn index-queue
  [branch-state]
  (let [buf   (async/sliding-buffer 1)
        queue (async/chan buf)]
    (go-loop []
      (when-let [{:keys [db index-files-ch]} (<! queue)]
        (try*
          (when-let [new-db (<? (indexer/collect db index-files-ch))]
            (swap! branch-state update-index new-db))
          (catch* e
                  (log/error e "Error updating index"))
          (finally
            (async/close! index-files-ch)))
        (recur)))
    queue))

(defn state-map
  "Returns a branch map for specified branch name at supplied commit"
  [conn ledger-alias branch-name commit-jsonld]
  (let [initial-db (async-db/load conn ledger-alias branch-name commit-jsonld)
        commit-map (commit-data/jsonld->clj commit-jsonld)
        state      (atom {:commit commit-map
                          :current-db initial-db})
        idx-q      (index-queue state)]
    {:name       branch-name
     :conn       conn
     :alias      ledger-alias
     :state      state
     :indexer    idx-q}))

(defn skipped-t?
  [new-t current-t]
  (and (not (or (nil? current-t)
                (zero? current-t))) ; when loading a ledger from disk, 't' will
                                    ; be zero but ledger 't' will be >= 1
       (flake/t-after? new-t (flake/next-t current-t))))

(defn updated-index?
  [current-commit new-commit]
  (flake/t-before? (commit-data/index-t current-commit)
                   (commit-data/index-t new-commit)))

(defn use-latest
  [new-db current-db]
  (let [new-t     (:t new-db)
        current-t (:t current-db)]
    (if (skipped-t? new-t current-t)
      (throw (ex-info (str "Unable to create new DB version on ledger. "
                           "current 't' value is: " current-t
                           " however new t value is: " new-t
                           ". Successive 't' values must be contiguous.")
                      {:status 500 :error :db/invalid-time}))
      (let [current-commit (:commit current-db)]
        (if (flake/t-before? new-t current-t)
          (let [outdated-commit (:commit new-db)
                latest-commit   (commit-data/use-latest-index current-commit outdated-commit)]
            (if (updated-index? current-commit latest-commit)
              (dbproto/-index-update current-db (:index latest-commit))
              current-db))
          (let [new-commit    (:commit new-db)
                latest-commit (commit-data/use-latest-index new-commit current-commit)]
            (if (updated-index? new-commit latest-commit)
              (dbproto/-index-update new-db (:index latest-commit))
              new-db)))))))

(defn current-db
  "Returns current db from branch data"
  [{:keys [state] :as _branch-map}]
  (:current-db @state))

(defn current-commit
  [{:keys [state] :as _branch-map}]
  (:commit @state))

(defn updatable-commit?
  [current-commit new-commit]
  (let [current-t (commit-data/t current-commit)
        new-t     (commit-data/t new-commit)]
    (or (nil? current-t)
        (and (updated-index? current-commit new-commit)
             (not (flake/t-after? new-t current-t))) ; index update may come after multiple commits
        (= new-t (inc current-t)))))

(defn update-commit!
  "There are 3 t values, the db's t, the 'commit' attached to the db's t, and
  then the ledger's latest commit t (in branch-data). The db 't' and db commit 't'
  should be the same at this point (just after committing the db). The ledger's latest
  't' should be the same (if just updating an index) or after the db's 't' value."
  [{:keys [state] :as branch-map} {new-commit :commit, db-t :t, :as db}]
  (swap! state
         (fn [{:keys [current-db] :as current-state}]
           (let [current-commit (:commit current-state)
                 current-t      (commit-data/t current-commit)
                 new-t          (commit-data/t new-commit)]
             (if (= db-t new-t)
               (if (updatable-commit? current-commit new-commit)
                 (let [{:keys [commit] :as current-db*} (use-latest db current-db)]
                   {:commit commit
                    :current-db current-db*})
                 (do
                   (log/warn "Commit update failure.\n  Current commit:" current-commit
                             "\n  New commit:" new-commit)
                   (throw (ex-info (str "Commit failed, latest committed db is " current-t
                                        " and you are trying to commit at db at t value of: "
                                        new-t ". These should be one apart. Likely db was "
                                        "updated by another user or process.")
                                   {:status 400 :error :db/invalid-commit}))))
               (throw (ex-info (str "Unexpected Error updating commit database. "
                                    "New database has an inconsistent t from its commit:"
                                    db-t " and " new-t " respectively.")
                               {:status 500 :error :db/invalid-db}))))))
  branch-map)
