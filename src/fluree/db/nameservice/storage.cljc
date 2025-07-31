(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  ([ledger-alias]
   (local-filename ledger-alias "main"))
  ([ledger-alias branch]
   (str "ns@v1/" ledger-alias "@" (or branch "main") ".json")))

(defn publishing-address*
  [store ledger-alias]
  (-> store
      storage/location
      (storage/build-address ledger-alias)))

(defn ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format"
  [ledger-alias branch commit-address t index-address]
  (let [branch (or branch "main")]
    (cond-> {"@context"     {"f" iri/f-ns}
             "@id"          (str ledger-alias "@" branch)
             "@type"        ["f:Database" "f:PhysicalDatabase"]
             "f:ledger"     {"@id" ledger-alias}
             "f:branch"     branch
             "f:commit"     {"@id" commit-address}
             "f:t"          t
             "f:status"     "ready"}
      index-address (assoc "f:index" {"@id" index-address}))))

(defrecord StorageNameService [store vg-state]
  nameservice/Publisher
  (publish [_ data]
    (if (= (get data "type") "virtual-graph")
      ;; Handle virtual graph records (already JSON-LD bytes + filename provided)
      (let [record-bytes (get data "bytes")
            filename     (get data "filename")]
        (log/debug "nameservice.storage/publish virtual-graph" {:filename filename})
        (storage/write-bytes store filename record-bytes))
      ;; Handle regular commit records
      (let [ledger-alias   (get data "alias")
            branch         (or (get data "branch")
                               (when (and (string? ledger-alias)
                                          (str/includes? ledger-alias "@"))
                                 (subs ledger-alias (inc (str/last-index-of ledger-alias "@"))))
                               "main")
            commit-address (get data "address")
            t-value        (get-in data ["data" "t"])
            index-address  (get-in data ["index" "address"])
            ns-metadata    (ns-record ledger-alias branch commit-address t-value index-address)
            record-bytes   (json/stringify-UTF8 ns-metadata)
            filename       (local-filename ledger-alias branch)]
        (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :branch branch :filename filename})
        (let [res (storage/write-bytes store filename record-bytes)]
          (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :branch branch :filename filename})
          res))))

(defn get-commit
  "Returns the minimal nameservice record."
  ([record]
   (get-commit record nil))
  ([record _branch]
   ;; Always return the record itself for new format
   record))

;; Virtual Graph Dependency Tracking Functions

(defn is-virtual-graph-record?
  "Checks if a nameservice record is a virtual graph"
  [record]
  (some #{"f:VirtualGraphDatabase"} (get record "@type" [])))

(defn extract-vg-dependencies
  "Extracts ledger dependencies from a VG record"
  [vg-record]
  (mapv #(get % "@id") (get vg-record "f:dependencies" [])))

(defn check-vg-dependencies
  "Returns set of VG names that depend on the ledger, or empty set if none"
  [publisher ledger-alias]
  (get-in @(:vg-state publisher) [:dependencies ledger-alias] #{}))

(defn register-dependencies
  [publisher json-ld]
  (let [vg-name (get json-ld "f:name")
        dependencies (extract-vg-dependencies json-ld)]
    (log/debug "Registering VG dependencies for" vg-name ":" dependencies)
    (swap! (:vg-state publisher)
           (fn [state]
             (reduce (fn [s dep-ledger]
                       (update-in s [:dependencies dep-ledger]
                                  (fnil conj #{}) vg-name))
                     state
                     dependencies)))))

(defn initialize-vg-dependencies
  "Scans all virtual graph records at startup to build dependency map"
  [publisher]
  (go-try
    (let [all-records (<? (nameservice/all-records publisher))
          vg-records (filter is-virtual-graph-record? all-records)]

      (log/debug "Initializing VG dependencies from" (count vg-records) "virtual graph records")

      (doseq [vg-record vg-records]
        (<? (register-dependencies publisher vg-record)))

      (log/debug "VG dependency initialization complete. Dependencies:"
                 (:dependencies @(:vg-state publisher))))))

(defn unregister-vg-dependencies
  "Remove dependencies for a deleted virtual graph."
  [publisher vg-name]
  (log/debug "Unregistering VG dependencies for" vg-name)
  (swap! (:vg-state publisher)
         update :dependencies
         (fn [deps]
           (reduce-kv (fn [m ledger vgs]
                        (let [updated-vgs (disj vgs vg-name)]
                          (if (empty? updated-vgs)
                            (dissoc m ledger)
                            (assoc m ledger updated-vgs))))
                    deps  ;; Start with existing deps, not empty map!
                    deps))))

(defrecord StorageNameService [store vg-state]
  nameservice/Publisher
  (publish [this record]
    (go-try
      (let [filename (if-let [vg-name (:vg-name record)]
                       (local-filename vg-name)
                       (local-filename (str (get record "alias") "@" (get record "branch"))))
            json-ld (record->json-ld record)
            result (->> json-ld
                        json/stringify-UTF8
                        (storage/write-bytes store filename)
                        <?)]
        (log/debug "Nameservice published:" filename)
        (when (is-virtual-graph-record? json-ld)
          ;; If this is a virtual graph, register dependencies
          (register-dependencies this json-ld))
        result)))

  (retract [this target]
    (go-try
      (let [;; Check if target is a ledger (contains @) or VG (no @)
            ledger? (str/includes? target "@")
            address (-> store
                        storage/location
                        (storage/build-address (local-filename target)))]

        ;; If this is a VG, unregister dependencies first
        (when-not ledger?
          (unregister-vg-dependencies this target))

        (<? (storage/delete store address)))))

  (publishing-address [_ ledger-alias]
    (go (publishing-address* store ledger-alias)))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias branch]} (nameservice/resolve-address (storage/location store) ledger-address nil)
            filename                (local-filename (str alias "@" branch))]
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
      ;; Use the ListableStore protocol to list all nameservice files
      (if (satisfies? storage/ListableStore store)
        (loop [remaining-paths (<? (storage/list-paths store "ns@v1"))
               records []]
          (if-let [path (first remaining-paths)]
            (if-let [file-content (<? (storage/read-bytes store path))]
              (let [content-str (if (string? file-content)
                                  file-content
                                  (bytes/UTF8->string file-content))
                    record (json/parse content-str false)]
                (recur (rest remaining-paths) (conj records record)))
              (recur (rest remaining-paths) records))
            records))
        ;; Fallback for stores that don't support ListableStore
        (do
          (log/debug "Storage backend does not support ListableStore protocol")
          [])))))

(defn start
  [store]
  (let [publisher (->StorageNameService store (atom {}))]
    ;; Initialize VG dependencies from existing records asynchronously (fire and forget)
    (go-try
      (<? (initialize-vg-dependencies publisher)))
    publisher))
