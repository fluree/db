(ns fluree.db.virtual-graph.drop
  "Handles deletion of virtual graphs and their artifacts."
  (:require [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(defn drop-artifacts
  "Deletes all storage artifacts for a virtual graph"
  [{:keys [index-catalog] :as _conn} vg-name]
  (go-try
    (let [storage (:storage index-catalog)
          vg-path (str "virtual-graphs/" vg-name "/")]
      (log/debug "Dropping VG artifacts for" vg-name "at path" vg-path)
      ;; List all files under the VG directory
      (if (satisfies? storage/ListableStore storage)
        (let [vg-files (<? (storage/list-paths storage vg-path))]
          (log/debug "Found" (count vg-files) "VG files to delete")
          ;; Delete each file
          (doseq [file-path vg-files]
            (log/debug "Deleting VG file:" file-path)
            (<? (storage/delete storage file-path)))
          ;; Try to delete the directory itself (may not work on all storage types)
          (try*
            (<? (storage/delete storage vg-path))
            (catch* e
              ;; Directory deletion might not be supported, that's OK
              nil)))
        (log/warn "Storage backend does not support listing files, cannot clean up VG artifacts"))
      :vg-artifacts-dropped)))

(defn drop-virtual-graph
  "Drops a virtual graph and all its associated data"
  [conn vg-name]
  (go-try
    (let [{:keys [primary-publisher]} conn]
      (log/info "Dropping virtual graph:" vg-name)

      ;; 1. Remove from nameservice (which also unregisters dependencies)
      (<? (nameservice/retract primary-publisher vg-name))

      ;; 2. Delete all index files for this VG
      (<? (drop-artifacts conn vg-name))

      ;; 3. No cache to clear since VGs aren't cached in connection

      (log/info "Dropped virtual graph:" vg-name)
      :dropped)))