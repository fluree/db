(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn local-filename
  "Returns the local filename for a ledger's nameservice record.
   Can handle both resource names (for VG) and ledger aliases.
   For ledgers, expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v2/ledger-name/branch.json' for ledgers
   or 'ns@v2/resource-name.json' for virtual graphs."
  [resource-name]
  (if (str/includes? resource-name ":")
    (let [[ledger-name branch] (util.ledger/ledger-parts resource-name)
          branch (or branch const/default-branch-name)]
      (str const/ns-version "/" ledger-name "/" branch ".json"))
    (str const/ns-version "/" resource-name ".json")))

(defn new-ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch'."
  [ledger-alias commit-address t index-address index-t]
  (let [[alias branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (cond-> {"@context"     {"f" iri/f-ns}
             "@id"          ledger-alias  ;; Already includes :branch
             "@type"        ["f:Database" "f:PhysicalDatabase"]
             "f:ledger"     {"@id" alias}  ;; Just the ledger name without branch
             "f:branch"     branch
             "f:commit"     {"@id" commit-address}
             "f:t"          t
             "f:status"     "ready"}
      index-address (assoc "f:index" {"@id" index-address
                                      "f:t" index-t}))))

(defn get-t
  [ns-record]
  (get ns-record "f:t" 0))

(defn get-index-t
  [ns-record]
  (get-in ns-record ["f:index" "f:t"] 0))

(defn update-commit-address
  [ns-record commit-address commit-t]
  (if (and commit-address commit-t)
    (let [prev-t (get-t ns-record)]
      (if (< prev-t commit-t)
        (assoc ns-record
               "f:t" commit-t
               "f:commit" {"@id" commit-address})
        ns-record))
    ns-record))

(defn update-index-address
  [ns-record index-address index-t]
  (if index-address
    (let [prev-t (get-index-t ns-record)]
      (if (or (nil? index-t) (< prev-t index-t))
        (let [index-record (cond-> {"@id" index-address}
                             index-t (assoc "f:t" index-t))]
          (assoc ns-record "f:index" index-record))
        ns-record))
    ns-record))

(defn update-ns-record
  [ns-record ledger-alias commit-address commit-t index-address index-t]
  (if (some? ns-record)
    (-> ns-record
        (update-commit-address commit-address commit-t)
        (update-index-address index-address index-t))
    (new-ns-record ledger-alias commit-address commit-t
                   index-address index-t)))

;; Convert internal record map to JSON-LD for nameservice storage
(defmulti record->json-ld
  "Converts a nameservice record to JSON-LD format"
  (fn [record]
    (cond
      (contains? record :vg-name) :virtual-graph
      (= (get record "type") "virtual-graph") :virtual-graph
      :else :ledger)))

(defmethod record->json-ld :ledger
  [record]
  (let [{:strs [alias address]
         {:strs [t]} "data"
         {index-address "address"
          {:strs [index-t]} "data"} "index"} record]
    (new-ns-record alias address t index-address index-t)))

(defmethod record->json-ld :virtual-graph
  [{:keys [vg-name vg-type status dependencies config] :as _record}]
  {"@context" {"f" iri/f-ns
               "fidx" "https://ns.flur.ee/index#"}
   "@id" vg-name
   "@type" (cond-> ["f:VirtualGraphDatabase"]
             vg-type (conj vg-type))
   "f:name" vg-name
   "f:status" (or status "ready")
   "f:dependencies" (mapv (fn [dep] {"@id" dep}) dependencies)
   "fidx:config" {"@type" "@json"
                  "@value" config}})

;; NOTE: Primary StorageNameService defined later; this earlier definition was removed to avoid duplication

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
      (if-let [vg-name (:vg-name record)]
        ;; Virtual Graph: use write-bytes (non-atomic, but VGs aren't concurrently updated)
        (let [filename (local-filename vg-name)
              json-ld (record->json-ld record)
              result (->> json-ld
                          json/stringify-UTF8
                          (storage/write-bytes store filename)
                          <?)]
          (log/debug "Nameservice published VG:" filename)
          (register-dependencies this json-ld)
          result)
        ;; Ledger: use swap-json for atomic updates
        (let [ledger-alias (get record "alias")  ;; Already includes :branch
              filename     (local-filename ledger-alias)
              commit-address (get record "address")
              commit-t       (get-in record ["data" "t"])
              index-address  (get-in record ["index" "address"])
              index-t        (get-in record ["index" "data" "t"])
              record-updater (fn [ns-record]
                               (update-ns-record ns-record ledger-alias commit-address commit-t
                                                 index-address index-t))
              res            (storage/swap-json store filename record-updater)]
          (log/debug "Nameservice published ledger:" {:ledger ledger-alias :filename filename})
          res))))

  (retract [this target]
    (go-try
      (let [;; Check if target is a ledger (contains :) or VG (no :)
            ledger? (str/includes? target ":")
            address (-> store
                        storage/location
                        (storage/build-address (local-filename target)))]

        ;; If this is a VG, unregister dependencies first
        (when-not ledger?
          (unregister-vg-dependencies this target))

        (<? (storage/delete store address)))))

  (publishing-address [_ ledger-alias]
    ;; Just return the alias - lookup will handle branch extraction via local-filename
    (go ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      ;; ledger-address is just the alias (potentially with :branch)
      (let [filename (local-filename ledger-address)]
        (log/debug "StorageNameService lookup:" {:ledger-address ledger-address
                                                 :filename       filename})
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
                                      (bytes/UTF8->string file-content))
                        record (json/parse content-str false)]
                    (recur (rest remaining-paths) (conj records record)))
                  (recur (rest remaining-paths) records)))
              records))
          [])
        ;; Fallback for stores that don't support RecursiveListableStore
        (do
          (log/debug "Storage backend does not support RecursiveListableStore protocol")
          [])))))

(defn start
  [store]
  (let [publisher (->StorageNameService store (atom {}))]
    ;; Initialize VG dependencies from existing records asynchronously (fire and forget)
    (go-try
      (<? (initialize-vg-dependencies publisher)))
    publisher))
