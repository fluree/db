(ns fluree.db.migrations.nameservice-v1
  "Nameservice Migration to ns@v1 Format
  
  This migration handles the transition from the legacy nameservice format to the new
  ns@v1 directory structure with minimal storage records.
  
  WHAT IT DOES:
  - Detects old nameservice files stored at root level (e.g., ledger-name.json)
  - Extracts essential metadata from full commit JSON-LD records
  - Creates new minimal records with only: commit address, t value, index address
  - Stores them in the new ns@v1/ directory with branch-aware naming (ledger-name@branch.json)
  - Cleans up old files after successful migration
  
  WHEN IT RUNS:
  - Automatically at file storage initialization if old format is detected
  - Only runs once when ledger directories exist but no ns@v1/ directory is present
  
  SAFETY:
  - Read-only detection phase before any modifications
  - Sequential processing with proper error handling
  - Old files only deleted after successful migration
  - Logging for monitoring and debugging"
  (:require #?(:clj [clojure.java.io :as io])
            [clojure.string :as str]
            [fluree.db.nameservice.storage :as ns-storage]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn needs-migration?
  "Check if nameservice migration is needed by detecting old format files without ns@v1 directory"
  [file-store]
  (go-try
    (let [root-path   (:root file-store)
          root-files  (<? (fs/list-files root-path))
          has-ledgers? (some #(and (not= % "ns@v1")
                                   (not (str/includes? % ".")))
                             root-files)
          has-ns-v1?   (some #(= % "ns@v1") root-files)]
      (and has-ledgers? (not has-ns-v1?)))))

(defn find-json-files-recursively
  "Find all JSON files in directory tree, excluding certain paths"
  [root-path]
  #?(:clj
     (let [root-file (io/file root-path)]
       (->> (file-seq root-file)
            (filter #(.isFile ^java.io.File %))
            (map #(.getPath ^java.io.File %))
            (filter #(and (str/ends-with? % ".json")
                          (not (str/includes? % "/ns@v1/"))
                          (not (str/includes? % "/commit/"))
                          (not (str/includes? % "/index/"))
                          (not (str/includes? % "/txn/"))))))
     :cljs
     (throw (ex-info "Migration not supported in ClojureScript" {:path root-path}))))

(defn find-old-nameservice-files
  "Find old nameservice files in ledger directories - handles both root level and nested paths"
  [file-store]
  (go-try
    (let [root-path   (:root file-store)
          ;; Find all .json files recursively, excluding ns@v1 and commit directories
          all-json-files (find-json-files-recursively root-path)
          ;; Filter to only nameservice files (those that match ledger naming pattern)
          ;; clj-kondo false positive - ledger-path is used in the clj branch  
          ns-files #_{:clj-kondo/ignore [:unused-binding]}
          (filter (fn [path]
                    (let [relative-path (str/replace path (str root-path "/") "")
                                   ;; Remove .json extension
                          ledger-path (str/replace relative-path #"\.json$" "")]
                               ;; Check if corresponding ledger directory exists
                      #?(:clj (.exists (io/file (str root-path "/" ledger-path)))
                         :cljs false)))
                  all-json-files)]
      (mapv (fn [full-path]
              (let [relative-path (str/replace full-path (str root-path "/") "")
                    ledger-alias (str/replace relative-path #"\.json$" "")]
                {:ledger-alias ledger-alias
                 :file-path relative-path
                 :full-path full-path}))
            ns-files))))

(defn extract-commit-metadata
  "Extract relevant metadata from old nameservice commit for migration"
  [old-commit-data]
  (let [;; Old format has nested structure: branches[0].commit contains the actual commit data
        branches       (get old-commit-data "branches")
        first-branch   (first branches)
        commit-data    (get first-branch "commit")
        ;; Extract values from the nested commit structure
        ledger-alias   (or (get old-commit-data "ledgerAlias")
                           (get commit-data "alias"))
        branch         (or (get commit-data "branch") "main")
        commit-address (get commit-data "address")
        t-value        (get-in commit-data ["data" "t"])
        ;; Extract index address if present in old format
        index-address  (get-in commit-data ["index" "address"])]
    {:ledger-alias ledger-alias
     :branch branch
     :commit-address commit-address
     :t-value t-value
     :index-address index-address}))

(defn migrate-nameservice-file
  "Migrate a single old nameservice file to new format"
  [file-store {:keys [ledger-alias file-path full-path]}]
  (go-try
    (log/info "Migrating nameservice file for ledger:" ledger-alias)
    (when-let [old-data-str (<? (fs/read-file full-path))]
      (let [old-data (json/parse old-data-str false)
            {:keys [branch commit-address t-value index-address]}
            (extract-commit-metadata old-data)

            ;; Create new minimal record using existing function
            new-record (ns-storage/ns-record ledger-alias branch commit-address t-value index-address)
            record-bytes (json/stringify-UTF8 new-record)
            new-filename (ns-storage/local-filename ledger-alias branch)]

        ;; Write to new location using storage interface
        (<? (storage/write-bytes file-store new-filename record-bytes))

        ;; Clean up old file using filesystem
        (<? (fs/delete-file full-path))

        (log/info "Successfully migrated nameservice for" ledger-alias "to" new-filename)
        {:migrated true :ledger ledger-alias :old-path file-path :new-path new-filename}))))

(defn migrate-all-nameservice-files
  "Migrate all old nameservice files to new ns@v1 format"
  [file-store]
  (go-try
    (log/info "Starting nameservice migration to ns@v1 format")
    (let [old-files (<? (find-old-nameservice-files file-store))]
      (if (seq old-files)
        (do
          (log/info "Found" (count old-files) "nameservice files to migrate:"
                    (mapv :ledger-alias old-files))
          ;; Process files sequentially with proper async handling
          (loop [files old-files
                 results []]
            (if-let [file-info (first files)]
              (let [result (<? (migrate-nameservice-file file-store file-info))]
                (recur (rest files) (conj results result)))
              (do
                (log/info "Nameservice migration completed. Migrated" (count results) "files")
                results))))
        (do
          (log/info "No nameservice files found to migrate")
          [])))))

(defn run-migration-if-needed
  "Check if migration is needed and run it if so"
  [file-store]
  (go-try
    (when (<? (needs-migration? file-store))
      (log/info "Old nameservice format detected, starting migration...")
      (<? (migrate-all-nameservice-files file-store)))))