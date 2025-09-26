(ns fluree.db.migrations.nameservice
  "Nameservice Migration to ns@v2 Format

  This migration handles the transition from the legacy nameservice formats (pre-ns@v1 and
  ns@v1) to the new ns@v2 directory structure with minimal storage records.

  WHAT IT DOES:
  - Detects old nameservice files stored at root level (e.g., ledger-name.json)
  - Extracts essential metadata from full commit JSON-LD records
  - Creates new minimal records with only: commit address, t value, index address
  - Stores them in the new ns@v2/ directory with branch-aware naming (ledger-name/branch.json)
  - Cleans up old files after successful migration
  - Additionally: migrates prior ns@v1 flat files (ledger@branch.json) and nested files
    (ledger/branch.json) to ns@v2 nested layout

  WHEN IT RUNS:
  - Automatically at file storage initialization (legacy and flat->nested checks are independent)

  SAFETY:
  - Read-only detection phase before any modifications
  - Sequential processing with proper error handling
  - Old files only deleted after successful migration
  - Logging for monitoring and debugging"
  (:require #?(:clj [clojure.java.io :as io])
            #?@(:cljs [[fluree.db.platform :as platform]
                       ["fs" :as node-fs]
                       ["path" :as node-path]])
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.nameservice.storage :as ns-storage]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn needs-migration?
  "Check if legacy nameservice migration is needed by detecting old format files when
  neither ns@v1 nor ns@v2 directory exists. Avoids work when either ns dir is already present."
  [file-store]
  (go-try
    (let [root-path   (:root file-store)
          root-files  (<? (fs/list-files root-path))
          has-ledgers? (some #(and (not (#{"ns@v1" "ns@v2"} %))
                                   (not (str/includes? % ".")))
                             root-files)
          has-ns-v1?   (some #(= % "ns@v1") root-files)
          has-ns-v2?   (some #(= % "ns@v2") root-files)]
      (and has-ledgers? (not has-ns-v1?) (not has-ns-v2?)))))

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
     (if platform/BROWSER
       (throw (ex-info "Migration not supported in browser" {:path root-path}))
       ;; Node.js implementation
       (let [find-files (fn find-files [dir acc]
                          (let [entries (node-fs/readdirSync dir #js {:withFileTypes true})]
                            (reduce (fn [acc entry]
                                      (let [entry-name (.-name entry)
                                            full-path (node-path/join dir entry-name)]
                                        (cond
                                          (.isDirectory entry)
                                          (if (not (contains? #{"ns@v1" "commit" "index" "txn"} entry-name))
                                            (find-files full-path acc)
                                            acc)

                                          (and (.isFile entry)
                                               (str/ends-with? entry-name ".json"))
                                          (conj acc full-path)

                                          :else acc)))
                                    acc
                                    (js->clj entries))))]
         (find-files root-path [])))))

(defn find-old-nameservice-files
  "Find old nameservice files in ledger directories - handles both root level and nested paths"
  [file-store]
  (go-try
    (let [root-path   (:root file-store)
          ;; Find all .json files recursively, excluding ns@v1 and commit directories
          all-json-files (find-json-files-recursively root-path)
          ;; Filter to only nameservice files (those that match ledger naming pattern)
          ;; clj-kondo false positive - ledger-path is used in the clj branch
          ns-files
          (filter (fn [path]
                    (let [relative-path (str/replace path (str root-path "/") "")
                          ;; Remove .json extension
                          ledger-path (str/replace relative-path #"\.json$" "")
                          ledger-full-path (str root-path "/" ledger-path)]
                      ;; Check if corresponding ledger directory exists
                      #?(:clj (.exists (io/file ledger-full-path))
                         :cljs (if platform/BROWSER
                                 false
                                 (node-fs/existsSync ledger-full-path)))))
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
        branch         (or (get commit-data "branch") const/default-branch-name)
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
  "Migrate a single pre-ns@v1 nameservice file directly to current nested layout (ns@v2)."
  [file-store {:keys [ledger-alias file-path full-path]}]
  (go-try
    (log/info "Migrating nameservice file for ledger:" ledger-alias)
    (when-let [old-data-str (<? (fs/read-file full-path))]
      (let [old-data (json/parse old-data-str false)
            {:keys [branch commit-address t-value index-address]}
            (extract-commit-metadata old-data)

            ;; Create new minimal record using existing function
            full-alias (str ledger-alias ":" branch)
            new-record (ns-storage/new-ns-record full-alias commit-address t-value
                                                 index-address nil)
            record-bytes (json/stringify-UTF8 new-record)
            ;; Write to current ns version nested path (ns@v2/...)
            [ledger-name branch-name] (util.ledger/ledger-parts full-alias)
            new-filename (str const/ns-version "/" ledger-name "/"
                              (or branch-name const/default-branch-name) ".json")]

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
    (log/info "Starting legacy nameservice migration to" const/ns-version "format")
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
                (log/info "Legacy nameservice migration completed. Migrated" (count results) "files")
                results))))
        (do
          (log/info "No nameservice files found to migrate")
          [])))))

;; --- Flat ns@v1 (ledger@branch.json) -> Nested ns@v1 (ledger/branch.json) migration ---

(def flat-filename-regex
  ;; Explicitly target ns@v1 flat files regardless of current const/ns-version
  (re-pattern "^ns@v1/([^/]+)@([^/]+)\\.json$"))

(defn find-flat-nameservice-files
  "Find ns@v1 files using the legacy flat naming: ns@v1/<ledger>@<branch>.json"
  [file-store]
  (go-try
    (if (satisfies? storage/RecursiveListableStore file-store)
      (let [paths (<? (storage/list-paths-recursive file-store "ns@v1"))
            matches (keep (fn [p]
                            (when-let [[_ ledger branch] (re-matches flat-filename-regex p)]
                              (let [alias        (str ledger ":" branch)
                                    ;; Force new nested path under ns@v2
                                    new-filename (str const/ns-version "/" ledger "/" branch ".json")]
                                {:old-path p
                                 :ledger   ledger
                                 :branch   branch
                                 :alias    alias
                                 :new-path new-filename})))
                          paths)]
        (vec matches))
      [])))

(defn migrate-flat-file
  "Migrate one flat ns@v1 nameservice file to nested layout. Idempotent if new file exists."
  [file-store {:keys [old-path new-path alias]}]
  (go-try
    (log/info "Migrating flat ns record for" alias ":" old-path "->" new-path)
    ;; If new path already exists, just delete the old file and return
    (if (<? (storage/read-bytes file-store new-path))
      (do
        (log/info "New path already exists for" alias ", removing old file only")
        (let [addr (-> file-store storage/location (storage/build-address old-path))]
          (<? (storage/delete file-store addr)))
        {:migrated false :skipped true :alias alias :old-path old-path :new-path new-path})
      (let [content (<? (storage/read-bytes file-store old-path))
            bytes*  (cond
                      (nil? content) nil
                      (string? content) (bytes/string->UTF8 content)
                      :else content)]
        (when-not bytes*
          (throw (ex-info (str "Unable to read old nameservice file: " old-path)
                          {:status 500 :error :db/migration})))
        (<? (storage/write-bytes file-store new-path bytes*))
        (let [addr (-> file-store storage/location (storage/build-address old-path))]
          (<? (storage/delete file-store addr)))
        (log/info "Migrated flat ns record for" alias)
        {:migrated true :alias alias :old-path old-path :new-path new-path}))))

(defn migrate-flat-nameservice-files
  "Migrate all ns@v1 flat files to nested layout."
  [file-store]
  (go-try
    (let [flat-files (<? (find-flat-nameservice-files file-store))]
      (if (seq flat-files)
        (do
          (log/info "Found" (count flat-files) "flat ns@v1 files to migrate")
          (loop [files flat-files
                 results []]
            (if-let [info (first files)]
              (let [res (<? (migrate-flat-file file-store info))]
                (recur (rest files) (conj results res)))
              (do
                (log/info "Flat ns@v1 migration completed. Migrated" (count (filter :migrated results))
                          "files, skipped" (count (filter :skipped results)) "already updated files")
                results))))
        []))))

;; Comprehensive v1 -> v2 migration (handles both flat and nested v1 layouts)

(def v1-flat-regex (re-pattern "^ns@v1/(.+)@([^/]+)\\.json$"))
(def v1-nested-one-regex (re-pattern "^ns@v1/([^/]+)/([^/]+)\\.json$"))
(def v1-nested-two-regex (re-pattern "^ns@v1/([^/]+)/([^/]+)/([^/]+)\\.json$"))

(defn find-v1-nameservice-files
  "Find all ns@v1 nameservice files (flat and nested) and compute target ns@v2 paths.
  Scopes discovery to ns@v1 only via recursive listing and avoids scanning root."
  [file-store]
  (go-try
    (if (satisfies? storage/RecursiveListableStore file-store)
      (let [paths (<? (storage/list-paths-recursive file-store "ns@v1"))]
        (->> paths
             (keep (fn [p]
                     (cond
                       (re-matches v1-flat-regex p)
                       (let [[_ ledger-path branch] (re-matches v1-flat-regex p)]
                         {:old-path p
                          :alias    (str ledger-path ":" branch)
                          :new-path (str const/ns-version "/" ledger-path "/" branch ".json")})

                       (re-matches v1-nested-two-regex p)
                       (let [[_ seg1 seg2 branch] (re-matches v1-nested-two-regex p)
                             ledger-path (str seg1 "/" seg2)]
                         {:old-path p
                          :alias    (str ledger-path ":" branch)
                          :new-path (str const/ns-version "/" ledger-path "/" branch ".json")})

                       (re-matches v1-nested-one-regex p)
                       (let [[_ ledger branch] (re-matches v1-nested-one-regex p)]
                         {:old-path p
                          :alias    (str ledger ":" branch)
                          :new-path (str const/ns-version "/" ledger "/" branch ".json")})

                       :else nil)))
             vec))
      [])))

(defn migrate-v1-file
  "Migrate one ns@v1 nameservice file to ns@v2 nested layout. Idempotent if new file exists."
  [file-store {:keys [old-path new-path alias]}]
  (go-try
    (log/info "Migrating ns@v1 record for" alias ":" old-path "->" new-path)
    (if (<? (storage/read-bytes file-store new-path))
      (do
        (log/info "New path already exists for" alias ", removing old file only")
        (let [addr (-> file-store storage/location (storage/build-address old-path))]
          (<? (storage/delete file-store addr)))
        {:migrated false :skipped true :alias alias :old-path old-path :new-path new-path})
      (let [content (<? (storage/read-bytes file-store old-path))
            bytes*  (cond
                      (nil? content) nil
                      (string? content) (bytes/string->UTF8 content)
                      :else content)]
        (when-not bytes*
          (throw (ex-info (str "Unable to read nameservice file: " old-path)
                          {:status 500 :error :db/migration})))
        (<? (storage/write-bytes file-store new-path bytes*))
        (let [addr (-> file-store storage/location (storage/build-address old-path))]
          (<? (storage/delete file-store addr)))
        (log/info "Migrated ns@v1 record for" alias)
        {:migrated true :alias alias :old-path old-path :new-path new-path}))))

(defn migrate-v1-to-v2
  "Migrate all ns@v1 files (flat and nested) to ns@v2 nested layout."
  [file-store]
  (go-try
    (let [v1-files (<? (find-v1-nameservice-files file-store))]
      (if (seq v1-files)
        (do
          (log/info "Found" (count v1-files) "ns@v1 files to migrate to" const/ns-version)
          (loop [files v1-files
                 results []]
            (if-let [info (first files)]
              (let [res (<? (migrate-v1-file file-store info))]
                (recur (rest files) (conj results res)))
              (do
                (log/info "ns@v1 ->" const/ns-version "migration completed. Migrated"
                          (count (filter :migrated results))
                          "files, skipped" (count (filter :skipped results)) "already updated files")
                results))))
        []))))

(defn run-migration-if-needed
  "Run nameservice migrations as needed, minimizing expensive scans.
  - If ns@v2 exists: do nothing
  - Else if ns@v1 exists: migrate ns@v1 (flat or nested) -> ns@v2
  - Else: if legacy layout likely (has ledgers, no ns@v1/ns@v2), run legacy -> ns@v2"
  [file-store]
  (go-try
    (let [root-path (:root file-store)
          v2-path   (str (fs/local-path root-path) "/ns@v2")
          v1-path   (str (fs/local-path root-path) "/ns@v1")
          has-ns-v2? (<? (fs/exists? v2-path))
          has-ns-v1? (<? (fs/exists? v1-path))]
      (cond
        has-ns-v2?
        (do (log/info "Nameservice directory ns@v2 present. No migration needed.") nil)

        has-ns-v1?
        (do (log/info "ns@v1 detected without ns@v2. Migrating ns@v1 ->" const/ns-version "...")
            (<? (migrate-v1-to-v2 file-store)))

        :else
        (when (<? (needs-migration? file-store))
          (log/info "Legacy nameservice detected (no ns@v1/ns@v2). Migrating to" const/ns-version "...")
          (<? (migrate-all-nameservice-files file-store)))))))
