(ns fluree.db.virtual-graph.drop
  "Handles deletion of virtual graphs and their artifacts."
  (:require [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn drop-artifacts
  "Deletes all storage artifacts for a virtual graph"
  [{:keys [index-catalog] :as _conn} vg-name]
  (go-try
    (let [vg-path (str "virtual-graphs/" vg-name "/")]
      (log/debug "Dropping VG artifacts for" vg-name "at path" vg-path)
      (if (satisfies? storage/RecursiveListableStore index-catalog)
        (do
          (let [vg-files (<? (storage/list-paths-recursive index-catalog vg-path))]
            (log/debug "Found" (count vg-files) "VG files to delete")
            (doseq [file-path vg-files]
              (log/debug "Deleting VG file:" file-path)
              (<? (storage/delete index-catalog file-path)))
            ;; Try to delete the directory itself (may not work on all storage types)
            (try*
              (<? (storage/delete index-catalog vg-path))
              (catch* e
                nil)))
          :vg-artifacts-dropped)
        (do
          (log/warn "Storage backend does not support listing files, cannot clean up VG artifacts")
          :vg-artifacts-not-dropped)))))

(defn drop-virtual-graph
  "Drops a virtual graph and all its associated data.
   VG names follow the same convention as ledgers - normalized with branch."
  [conn vg-name]
  (go-try
    (let [{:keys [primary-publisher]} conn
          ;; Normalize name to include branch (e.g., "my-vg" -> "my-vg:main")
          normalized-name (util.ledger/ensure-ledger-branch vg-name)]
      (log/info "Dropping virtual graph:" normalized-name)

      ;; 1. Remove from nameservice (which also unregisters dependencies)
      (<? (nameservice/retract primary-publisher normalized-name))

      ;; 2. Delete all index files for this VG
      (<? (drop-artifacts conn normalized-name))

      ;; 3. No cache to clear since VGs aren't cached in connection

      (log/info "Dropped virtual graph:" normalized-name)
      :dropped)))