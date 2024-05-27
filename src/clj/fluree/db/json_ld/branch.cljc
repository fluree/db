(ns fluree.db.json-ld.branch
  (:require [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.indexer :as indexer]
            [fluree.json-ld :as json-ld]
            [fluree.db.database.async :as async-db]
            [fluree.db.db.json-ld :as jld-db]
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
        (= new-t (inc current-t)))))

(defn newer-index?
  [current-commit new-commit]
  (let [current-index-t (commit-data/index-t current-commit)
        new-index-t (commit-data/index-t new-commit)]
    (and (some? current-index-t)
         (or (nil? new-index-t)
             (> current-index-t new-index-t)))))

(defn commit-map->commit-jsonld
  [commit-map]
  (-> commit-map commit-data/->json-ld json-ld/expand))

(defn update-commit!
  "There are 3 t values, the db's t, the 'commit' attached to the db's t, and
  then the ledger's latest commit t (in branch-data). The db 't' and db commit 't'
  should be the same at this point (just after committing the db). The ledger's latest
  't' should be the same (if just updating an index) or after the db's 't' value."
  [{:keys [conn alias state name] :as branch-map} {new-commit :commit, :as new-db}]
  (swap! state
         (fn [{current-commit :commit, :as _current-state}]
           (if (updatable-commit? current-commit new-commit)
             (if (newer-index? current-commit new-commit)
               (let [latest-index         (:index current-commit)
                     latest-commit        (assoc new-commit :index latest-index)
                     latest-commit-jsonld (commit-map->commit-jsonld new-commit)
                     latest-db            (async-db/load conn alias name latest-commit-jsonld)]
                 {:commit     latest-commit
                  :current-db latest-db})
               {:commit     new-commit
                :current-db new-db})
             (do
               (log/warn "Commit update failure.\n  Current commit:" current-commit
                         "\n  New commit:" new-commit)
               (throw (ex-info (str "Commit failed, latest committed db is "
                                    (commit-data/t current-commit)
                                    " and you are trying to commit at db at t value of: "
                                    (commit-data/t new-commit)
                                    ". These should be one apart. Likely db was "
                                    "updated by another user or process.")
                               {:status 400 :error :db/invalid-commit}))))))
  branch-map)
