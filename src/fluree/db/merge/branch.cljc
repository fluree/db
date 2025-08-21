(ns fluree.db.merge.branch
  "Branch analysis and comparison functions for merge operations."
  (:require [clojure.core.async :as async]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.connection :as connection]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.ledger :as ledger]
            [fluree.db.merge.commit :as merge-commit]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn- same-commit?
  "Check if two databases are at the same commit."
  [source-db target-db]
  (= (get-in source-db [:commit :id])
     (get-in target-db [:commit :id])))

(defn- branch-origin
  "Get the commit ID a branch was created from."
  [branch-info]
  (or (get-in branch-info [:created-from "f:commit" "@id"])      ; nameservice expanded
      (get-in branch-info [:created-from :commit])                ; internal map
      (get-in branch-info ["f:createdFrom" "f:commit" "@id"])     ; raw nameservice
      (get-in branch-info ["created-from" "f:commit" "@id"])))

(defn- branch-created-from?
  "Check if source branch was created from target commit."
  [source-branch-info target-commit-id]
  (= (branch-origin source-branch-info) target-commit-id))

(defn- branches-share-origin?
  "Check if two branches were created from the same commit."
  [source-branch-info target-branch-info]
  (when-let [source-origin (branch-origin source-branch-info)]
    (= source-origin (branch-origin target-branch-info))))

(defn- get-commit-chain
  "Returns vector of commit maps from genesis to head for a db."
  [conn db]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          latest-expanded (<? (merge-commit/expand-latest-commit conn db))
          error-ch (async/chan)
          tuples (commit-storage/trace-commits commit-catalog latest-expanded 0 error-ch)]
      (loop [acc []]
        (if-let [[commit-expanded _] (<? tuples)]
          (recur (conj acc (commit-data/json-ld->map commit-expanded nil)))
          acc)))))

(defn find-lca
  "Finds the last common ancestor commit between two branches.
  Returns commit id string."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [source-commit-id (get-in source-db [:commit :id])
          target-commit-id (get-in target-db [:commit :id])]
      (cond
        (same-commit? source-db target-db) source-commit-id
        (branch-created-from? source-branch-info target-commit-id) target-commit-id
        (branch-created-from? target-branch-info source-commit-id) source-commit-id
        (branches-share-origin? source-branch-info target-branch-info) (branch-origin source-branch-info)
        :else
        (let [source-chain (<? (get-commit-chain conn source-db))
              target-chain (<? (get-commit-chain conn target-db))
              source-id-set (into #{} (keep :id) source-chain)
              ;; Try id-based match first
              lca-id (some (fn [commit]
                             (let [cid (:id commit)]
                               (when (and cid (contains? source-id-set cid)) cid)))
                           (reverse target-chain))
              ;; Fallback: t-based match (find matching :data :t) and return that commit's id
              lca-by-t (when (nil? lca-id)
                         (let [source-t-set (into #{} (map #(get-in % [:data :t])) source-chain)]
                           (some (fn [commit]
                                   (let [ct (get-in commit [:data :t])]
                                     (when (and ct (contains? source-t-set ct))
                                       (:id commit))))
                                 (reverse target-chain))))]
          (or lca-id lca-by-t))))))

(defn can-fast-forward?
  "Checks if merge from source to target is a fast-forward merge.
  A fast-forward is possible when target branch's HEAD is an ancestor of source branch's HEAD."
  [conn source-db target-db source-branch-info target-branch-info]
  (go-try
    (let [target-head (get-in target-db [:commit :id])
          common-ancestor (<? (find-lca conn source-db target-db
                                        source-branch-info
                                        target-branch-info))]
      (log/debug "Fast-forward check:"
                 "target-head:" target-head
                 "common:" common-ancestor
                 "is-ff?" (= target-head common-ancestor))
      (= target-head common-ancestor))))

(defn load-branches
  "Load source and target branches with their metadata."
  [conn from-spec to-spec]
  (go-try
    (let [source-ledger (<? (connection/load-ledger conn from-spec))
          target-ledger (<? (connection/load-ledger conn to-spec))]
      {:source-ledger source-ledger
       :target-ledger target-ledger
       :source-db (ledger/current-db source-ledger)
       :target-db (ledger/current-db target-ledger)
       :source-branch-info (<? (ledger/branch-info source-ledger))
       :target-branch-info (<? (ledger/branch-info target-ledger))})))

(defn validate-same-ledger!
  "Validates that source and target are on the same ledger."
  [from to]
  (let [from-parts (re-find #"^([^:]+):(.+)$" from)
        to-parts (re-find #"^([^:]+):(.+)$" to)]
    (when-not (and from-parts to-parts)
      (throw (ex-info "Invalid branch specification. Expected format: ledger:branch"
                      {:status 400 :error :db/invalid-branch-spec
                       :from from :to to})))
    (when-not (= (nth from-parts 1) (nth to-parts 1))
      (throw (ex-info "Source and target must be on the same ledger"
                      {:status 400 :error :db/different-ledgers
                       :from from :to to})))))