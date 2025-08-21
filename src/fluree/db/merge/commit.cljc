(ns fluree.db.merge.commit
  "Functions related to commit operations and analysis for merge operations."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.async-db :as async-db]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

(defn ensure-sync-db
  "Ensures we have a synchronous database, dereferencing async if needed."
  [db]
  (go-try
    (if (async-db/db? db)
      (<? (async-db/deref-async db))
      db)))

(defn expand-latest-commit
  [conn db]
  (go-try
    (let [commit-catalog (:commit-catalog conn)
          commit-map (:commit db)
          latest-address (:address commit-map)]
      (if (and latest-address (string? latest-address) (not (str/blank? latest-address)))
        (first (<? (commit-storage/read-verified-commit commit-catalog latest-address)))
        ;; Fallback: expand from in-memory commit map
        (let [compact (commit-data/->json-ld commit-map)
              commit-id (commit-data/commit-json->commit-id compact)
              compact* (assoc compact "id" commit-id)]
          (json-ld/expand compact*))))))

(defn extract-commits-since
  "Extracts commits after LCA. Returns vector of commit maps
  in chronological order (oldest first)."
  [conn source-db lca-commit-id]
  (go-try
    ;; If LCA is the current commit, there are no commits since then
    (if (= lca-commit-id (get-in source-db [:commit :id]))
      []
      (let [commit-catalog (:commit-catalog conn)
            latest-expanded (<? (expand-latest-commit conn source-db))
            error-ch (async/chan)
            ;; include genesis (t=0) so LCA at genesis can be located
            tuples (commit-storage/trace-commits commit-catalog latest-expanded 0 error-ch)
            traced (loop [acc []]
                     (if-let [[exp _] (<? tuples)]
                       (recur (conj acc (commit-data/json-ld->map exp nil)))
                       acc))
            vtr traced
            head-id (get-in source-db [:commit :id])
            normalize-id (fn [cid]
                           (when cid
                             (let [s (str cid)]
                               (if (str/ends-with? s ".json")
                                 (subs s 0 (- (count s) 5))
                                 s))))]
        (log/debug "extract-commits-since-storage: traced-count=" (count vtr)
                   "head-id=" head-id "lca-id=" lca-commit-id)
        (if (seq vtr)
          (let [ids (mapv :id vtr)
                ids-norm (mapv normalize-id ids)
                lca-norm (normalize-id lca-commit-id)
                idx (when lca-norm
                      (let [found-idx (first (keep-indexed
                                              (fn [i id] (when (= id lca-norm) i))
                                              ids-norm))]
                        (or found-idx -1)))
                idx* (if (or (nil? idx) (= -1 idx))
                       ;; Don't try SHA lookup if we can't find the commit
                       -1
                       idx)
                after-lca (cond
                            (nil? idx*) vtr
                            (= -1 idx*) vtr
                            :else (subvec vtr (inc idx*)))]
            (log/debug "extract-commits-since-storage: after-lca-count=" (count after-lca)
                       "after-lca-ids=" (mapv :id after-lca))
            (vec after-lca))
          (let [walked (loop [acc [] cur (:commit source-db)]
                         (if (and cur (not= (:id cur) lca-commit-id))
                           (recur (conj acc cur) (:previous cur))
                           acc))
                walked* (vec (reverse walked))]
            (log/debug "extract-commits-since-storage: in-memory-walk-count=" (count walked*)
                       "walk-ids=" (mapv :id walked*))
            walked*))))))

(defn collect-commit-namespaces
  "Collects all unique namespace IRIs from a sequence of commits."
  [conn commits]
  (go-try
    (loop [namespaces #{}
           remaining commits]
      (if-let [commit (first remaining)]
        (let [commit-catalog (:commit-catalog conn)
              data-address (get-in commit [:data :address])
              data-jsonld (when data-address
                            (<? (commit-storage/read-data-jsonld commit-catalog data-address)))
              commit-nses-raw (get data-jsonld const/iri-namespaces)
              commit-nses (when (seq commit-nses-raw)
                            (mapv :value commit-nses-raw))]
          (recur (into namespaces commit-nses) (rest remaining)))
        namespaces))))

(defn read-commit-data
  "Reads the actual data from a commit.
  Returns map with :asserted and :retracted flakes."
  [conn commit db-context]
  (go-try
    (when-let [data-address (get-in commit [:data :address])]
      (let [commit-catalog (:commit-catalog conn)
            data-jsonld (<? (commit-storage/read-data-jsonld commit-catalog data-address))
            assert-data (get data-jsonld const/iri-assert)
            retract-data (get data-jsonld const/iri-retract)
            t (get-in commit [:data :t])
            asserted-flakes (when assert-data
                              (flake-db/create-flakes true db-context t assert-data))
            retracted-flakes (when retract-data
                               (flake-db/create-flakes false db-context t retract-data))]
        (log/debug "read-commit-data: t=" t
                   "assert-count=" (count assert-data)
                   "retract-count=" (count retract-data))
        {:asserted asserted-flakes
         :retracted retracted-flakes
         :all (concat (or asserted-flakes [])
                      (or retracted-flakes []))}))))

