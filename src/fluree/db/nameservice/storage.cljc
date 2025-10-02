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

(defn new-ns-record
  "Generates nameservice metadata map for JSON storage with branch metadata support.
   Expects ledger-alias to be in format 'ledger:branch'."
  [ledger-alias commit-address t index-address metadata]
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
      index-address (assoc "f:index" {"@id" index-address}))))

(defn get-t
  [ns-record]
  (get ns-record "f:t" 0))

(defn update-commit-address
  "Updates commit address if new t is greater than existing t.
   For branch creation, also validates that branch doesn't already exist."
  [ns-record ledger-alias commit-address commit-t is-branch-creation]
  (if (and commit-address commit-t)
    (let [prev-t (get-t ns-record)]
      (cond
        ;; Branch creation: if this is a new branch creation and t matches existing, this is invalid
        (and is-branch-creation (= commit-t prev-t))
        (throw (ex-info (str "Cannot create branch - it already exists with t=" prev-t)
                        {:status 409 :error :db/branch-exists
                         :alias ledger-alias :existing-t prev-t}))

        ;; Normal commit: new t must be greater than existing t
        (< commit-t prev-t)
        (throw (ex-info (str "Cannot publish commit with t=" commit-t
                             " - current HEAD is at t=" prev-t)
                        {:status 409 :error :db/invalid-commit-sequence
                         :alias ledger-alias :new-t commit-t :existing-t prev-t}))

        ;; Allow same t for updates (e.g., index updates)
        (>= commit-t prev-t)
        (assoc ns-record
               "f:t" commit-t
               "f:commit" {"@id" commit-address})))
    ns-record))

(defn update-index-address
  [ns-record index-address]
  (if index-address
    (assoc ns-record "f:index" {"@id" index-address})
    ns-record))

(defn update-metadata
  "Merges branch metadata, preserving existing values when not provided."
  [ns-record new-metadata]
  (let [existing-metadata (util.branch/extract-branch-metadata ns-record)
        merged-metadata (merge existing-metadata new-metadata)]
    (merge ns-record (util.branch/metadata->flat-fields merged-metadata))))

(defn update-ns-record
  "Atomically updates nameservice record with validation."
  [ns-record ledger-alias commit-address commit-t index-address metadata is-branch-creation]
  (if (some? ns-record)
    (-> ns-record
        (update-commit-address ledger-alias commit-address commit-t is-branch-creation)
        (update-index-address index-address)
        (update-metadata metadata))
    ;; New record
    (new-ns-record ledger-alias commit-address commit-t index-address metadata)))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [ledger-alias   (get data "alias")  ;; Already includes :branch
          path           (local-path ledger-alias)]
      (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :path path})
      (let [commit-address (get data "address")
            commit-t       (get-in data ["data" "t"])
            index-address  (get-in data ["index" "address"])
            ;; Extract metadata from incoming data
            new-metadata   (util.branch/extract-branch-metadata data)
            ;; Check if this is a branch creation (has source metadata)
            is-branch-creation (and (:source-branch new-metadata) (:source-commit new-metadata))
            _ (log/debug "StorageNameService/publish" {:alias ledger-alias
                                                       :commit-address commit-address
                                                       :t commit-t
                                                       :is-branch-creation? is-branch-creation})
            record-updater (fn [ns-record]
                             (update-ns-record ns-record ledger-alias commit-address commit-t
                                               index-address new-metadata is-branch-creation))
            res            (storage/swap-json store path record-updater)]
        (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :path path})
        res)))

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
