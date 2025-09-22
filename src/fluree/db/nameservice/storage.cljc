(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.branch :as util.branch]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn local-path
  "Returns the local path for a ledger's nameservice record.
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v2/ledger-name/branch.json'."
  [ledger-alias]
  (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (str const/ns-version "/" ledger-name "/" branch ".json")))

(defn ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch' with metadata support."
  ([ledger-alias commit-address t index-address]
   (ns-record ledger-alias commit-address t index-address nil))
  ([ledger-alias commit-address t index-address metadata]
   (let [[alias branch] (util.ledger/ledger-parts ledger-alias)
         branch (or branch const/default-branch-name)
         base-record {"@context"     {"f" iri/f-ns}
                      "@id"          ledger-alias  ;; Already includes :branch
                      "@type"        ["f:Database" "f:PhysicalDatabase"]
                      "f:ledger"     {"@id" alias}  ;; Just the ledger name without branch
                      "f:branch"     branch
                      "f:commit"     {"@id" commit-address}
                      "f:t"          t
                      "f:status"     "ready"}]
     (cond-> (merge base-record (util.branch/metadata->flat-fields metadata))
       index-address (assoc "f:index" {"@id" index-address})))))

(defn- validate-publish
  "Validates whether a publish operation is allowed based on existing records.
  Throws an exception if the operation is invalid."
  [ledger-alias t-value existing-record is-branch-creation]
  (when existing-record
    (let [existing-t (get existing-record "f:t")]
      (cond
        ;; Branch creation: if this is a new branch creation and t matches existing, this is invalid
        (and is-branch-creation (= t-value existing-t))
        (throw (ex-info (str "Cannot create branch - it already exists with t=" existing-t)
                        {:status 409 :error :db/branch-exists
                         :alias ledger-alias :existing-t existing-t}))

        ;; Normal commit: new t must be greater than or equal to existing t
        ;; Allow same t for updates (e.g., index updates)
        (< t-value existing-t)
        (throw (ex-info (str "Cannot publish commit with t=" t-value
                             " - current HEAD is at t=" existing-t)
                        {:status 409 :error :db/invalid-commit-sequence
                         :alias ledger-alias :new-t t-value :existing-t existing-t}))))))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [this data]
    (go-try
      (let [ledger-alias   (get data "alias")  ;; Already includes :branch
            commit-address (get data "address")
            t-value        (get-in data ["data" "t"])
            index-address  (get-in data ["index" "address"])
            ;; Extract metadata from incoming data and existing record
            new-metadata (util.branch/extract-branch-metadata data)
            existing-record (<? (nameservice/lookup this ledger-alias))
            existing-metadata (when existing-record (util.branch/extract-branch-metadata existing-record))

            ;; Merge metadata, preserving existing values when not provided
            metadata (merge existing-metadata new-metadata)

            ;; Check if this is a branch creation (has source metadata)
            is-branch-creation (and (:source-branch new-metadata) (:source-commit new-metadata))
            _ (log/debug "StorageNameService/publish called with alias:" ledger-alias
                         "commit-address:" commit-address "t:" t-value
                         "is-branch-creation?" is-branch-creation)

            ns-metadata    (ns-record ledger-alias commit-address t-value index-address metadata)
            record-bytes   (json/stringify-UTF8 ns-metadata)
            path           (local-path ledger-alias)]

        (validate-publish ledger-alias t-value existing-record is-branch-creation)

        (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :path path})
        (let [res (<? (storage/write-bytes store path record-bytes))]
          (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :path path})
          res))))

  (retract [_ ledger-alias]
    (let [path (local-path ledger-alias)
          address  (-> store
                       storage/location
                       (storage/build-address path))]
      (storage/delete store address)))

  (publishing-address [_ ledger-alias]
    ;; Just return the alias - lookup will handle branch extraction via local-path
    (go ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      ;; ledger-address is just the alias (potentially with :branch)
      (let [path (local-path ledger-address)]
        (log/info "StorageNameService lookup:" {:ledger-address ledger-address
                                                :path           path})
        (when-let [record-bytes (<? (storage/read-bytes store path))]
          (log/info "StorageNameService lookup found record for" ledger-address)
          (json/parse record-bytes false)))))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (storage/get-local-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join "/"))))

  (all-records [_]
    (go-try
      ;; Use recursive listing to support ledger names with '/' characters
      (if (satisfies? storage/RecursiveListableStore store)
        (if-let [list-paths-result (storage/list-paths-recursive store const/ns-version)]
          (loop [remaining-paths (<? list-paths-result)
                 records         []]
            (if-let [path (first remaining-paths)]
              (let [file-content (<? (storage/read-bytes store path))]
                (if file-content
                  (let [content-str (if (string? file-content)
                                      file-content
                                      #?(:clj (let [^bytes bytes-content file-content]
                                                (String. bytes-content "UTF-8"))
                                         :cljs (js/String.fromCharCode.apply nil file-content)))
                        record      (json/parse content-str false)]
                    (recur (rest remaining-paths) (conj records record)))
                  (recur (rest remaining-paths) records)))
              records))
          [])
        ;; Fallback for stores that don't support ListableStore
        (do
          (log/warn "Storage backend does not support RecursiveListableStore protocol")
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