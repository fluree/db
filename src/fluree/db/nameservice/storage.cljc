(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  "Returns the local filename for a ledger's nameservice record.
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v1/ledger-name/branch.json'."
  [ledger-alias]
  (let [[ledger-name branch] (str/split ledger-alias #":" 2)]
    (str "ns@v1/" ledger-name "/" branch ".json")))

(defn ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch' with metadata support."
  ([ledger-alias commit-address t index-address]
   (ns-record ledger-alias commit-address t index-address nil))
  ([ledger-alias commit-address t index-address metadata]
   (let [[alias branch] (str/split ledger-alias #":" 2)]
     (cond-> {"@context"     {"f" iri/f-ns}
              "@id"          ledger-alias  ;; Already includes :branch
              "@type"        ["f:Database" "f:PhysicalDatabase"]
              "f:ledger"     {"@id" alias}  ;; Just the ledger name without branch
              "f:branch"     branch
              "f:commit"     {"@id" commit-address}
              "f:t"          t
              "f:status"     "ready"}
       index-address (assoc "f:index" {"@id" index-address})
       (:created-at metadata) (assoc "f:createdAt" (:created-at metadata))
       (:created-from metadata) (assoc "f:createdFrom" (:created-from metadata))
       (:protected metadata) (assoc "f:protected" (:protected metadata))
       (:description metadata) (assoc "f:description" (:description metadata))))))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [this data]
    (go-try
      (let [;; Extract data from compact JSON-LD format (both genesis and regular commits now use this)
            ledger-alias   (get data "alias")  ;; Already includes :branch
            commit-address (get data "address")
            t-value        (get-in data ["data" "t"])
            index-address  (get-in data ["index" "address"])
            branch-metadata (get data "branchMetadata")

            ;; Check if this branch already exists
            existing-record (<? (nameservice/lookup this ledger-alias))
            existing-t      (when existing-record (get existing-record "f:t"))

            ;; Preserve existing branch metadata if not provided in this publish
            preserved-metadata (if (and existing-record (not branch-metadata))
                                 {:created-at (get existing-record "f:createdAt")
                                  :created-from (get existing-record "f:createdFrom")
                                  :protected (get existing-record "f:protected")
                                  :description (get existing-record "f:description")}
                                 branch-metadata)

            ;; Validation logic
            _ (when existing-record
                (cond
                  ;; Branch creation: if branchMetadata exists and t matches existing, this is invalid
                  (and branch-metadata (= t-value existing-t))
                  (throw (ex-info (str "Cannot create branch - it already exists with t=" existing-t)
                                  {:status 409 :error :db/branch-exists
                                   :alias ledger-alias :existing-t existing-t}))

                  ;; Normal commit: new t must be greater than or equal to existing t
                  ;; Allow same t for updates (e.g., index updates)
                  (< t-value existing-t)
                  (throw (ex-info (str "Cannot publish commit with t=" t-value
                                       " - current HEAD is at t=" existing-t)
                                  {:status 409 :error :db/invalid-commit-sequence
                                   :alias ledger-alias :new-t t-value :existing-t existing-t}))))

            ns-metadata    (ns-record ledger-alias commit-address t-value index-address preserved-metadata)
            record-bytes   (json/stringify-UTF8 ns-metadata)
            filename       (local-filename ledger-alias)]
        (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :filename filename})
        (let [res (<? (storage/write-bytes store filename record-bytes))]
          (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :filename filename})
          res))))

  (retract [_ ledger-alias]
    (let [filename (local-filename ledger-alias)
          address  (-> store
                       storage/location
                       (storage/build-address filename))]
      (storage/delete store address)))

  (publishing-address [_ ledger-alias]
    ;; Just return the alias - lookup will handle branch extraction via local-filename
    (go ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      ;; ledger-address is just the alias (potentially with :branch)
      (let [filename (local-filename ledger-address)]
        (log/debug "StorageNameService lookup:" {:ledger-address ledger-address
                                                 :filename filename})
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (json/parse record-bytes false)))))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (storage/get-local-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join "/"))))

  (all-records [_]
    (go-try
      ;; Read all JSON files from ns@v1 directory structure
      (let [base-dir "ns@v1/"
            all-files (<? (storage/list-paths-recursive store base-dir))]
        ;; Use pipeline for parallel reading and parsing
        (if (seq all-files)
          (let [input-ch (async/chan 100 (filter #(str/ends-with? % ".json")))
                _ (async/onto-chan! input-ch all-files)
                output-ch (async/chan 100)
                ;; Function to read and parse a single file
                read-and-parse (fn [file-path out-ch]
                                 (go
                                   (try
                                     (when-let [record-bytes (<? (storage/read-bytes store file-path))]
                                       (let [record (json/parse record-bytes false)]
                                         (async/>! out-ch record)))
                                     #?(:clj (catch Exception e
                                               (log/error e "Error reading/parsing file:" file-path))
                                        :cljs (catch :default e
                                                (log/error e "Error reading/parsing file:" file-path))))
                                   (async/close! out-ch)))
                ;; Set up pipeline with parallelism
                _ (async/pipeline-async 10 ; parallelism level
                                        output-ch
                                        read-and-parse
                                        input-ch)]
            ;; Collect all results into a vector
            (<? (async/into [] output-ch)))
          ;; No files found
          [])))))

(defn published-ledger?
  "Checks if a ledger is published in this nameservice"
  [ns ledger-alias]
  (go-try
    (boolean (<? (nameservice/lookup ns ledger-alias)))))

(defn create-nameservice
  [store]
  (->StorageNameService store))

(defn start
  "Start a storage nameservice"
  [store]
  (create-nameservice store))