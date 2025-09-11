(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn local-filename
  "Returns the local filename for a nameservice record.
   For ledgers: expects ledger-alias to be in format 'ledger:branch'.
   For virtual graphs: uses just the VG name.
   Returns path like 'ns@v2/ledger-name/branch.json' or 'ns@v2/vg-name.json'."
  ([resource-name]
   (if (str/includes? resource-name ":")
     ;; It's a ledger with branch notation
     (let [[ledger-name branch] (util.ledger/ledger-parts resource-name)
           branch (or branch "main")]
       (str const/ns-version "/" ledger-name "/" branch ".json"))
     ;; It's a virtual graph or other resource
     (str const/ns-version "/" resource-name ".json"))))

(defn publishing-address*
  [store ledger-alias]
  (-> store
      storage/location
      (storage/build-address ledger-alias)))

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
  (let [ledger-alias (get record "alias")
        [alias branch] (if ledger-alias
                         (util.ledger/ledger-parts ledger-alias)
                         [(get record "alias") (get record "branch")])
        branch (or branch "main")
        address (get record "address")
        t (get-in record ["data" "t"])
        index-address (get-in record ["index" "address"])]
    (cond-> {"@context"     {"f" iri/f-ns}
             "@id"          (str alias ":" branch)  ;; Use : delimiter
             "@type"        ["f:Database" "f:PhysicalDatabase"]
             "f:ledger"     {"@id" alias}
             "f:branch"     branch
             "f:commit"     {"@id" address}
             "f:t"          t
             "f:status"     "ready"}
      index-address (assoc "f:index" {"@id" index-address}))))

(defmethod record->json-ld :virtual-graph
  [{:keys [vg-name vg-type status dependencies config engine]}]
  (let [base-record {"@context" {"f" iri/f-ns
                                 "fidx" "https://ns.flur.ee/index#"}
                     "@id" vg-name
                     "@type" (cond-> ["f:VirtualGraphDatabase"]
                               vg-type (conj vg-type))
                     "f:name" vg-name
                     "f:status" (or status "ready")
                     "f:dependencies" (when (and (not= engine :r2rml)
                                                 (seq dependencies))
                                        (mapv (fn [dep] (if (string? dep) {"@id" dep} dep)) dependencies))}
        ;; Back-compat: always include opaque config blob
        with-config (assoc base-record "fidx:config" {"@type" "@json"
                                                      "@value" config})]
    (case engine
      ;; New R2RML-style schema using f:* keys
      (:r2rml "r2rml")
      (let [{:keys [mapping mappingInline baseIRI rdb]} config
            rdb* (select-keys rdb [:jdbcUrl :driver :user :password :options])
            record (cond-> (assoc with-config "f:engine" "r2rml")
                     mapping       (assoc "f:mapping" {"@id" mapping})
                     mappingInline (assoc "f:mappingInline" mappingInline)
                     baseIRI       (assoc "f:baseIRI" baseIRI)
                     (seq rdb*)    (assoc "f:rdb" rdb*))]
        record)

      ;; default (BM25 and others) keep prior structure
      with-config)))

(defn ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch'."
  [ledger-alias commit-address t index-address]
  (record->json-ld {"alias"   ledger-alias
                    "address" commit-address
                    "data"    {"t" t}
                    "index"   {"address" index-address}}))

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
  (publish [this data]
    (go-try
      (let [;; Check if this is a virtual graph or ledger
            is-vg? (contains? data :vg-name)
            filename (if is-vg?
                       (local-filename (:vg-name data))
                       (local-filename (get data "alias")))
            json-ld (if is-vg?
                      (record->json-ld data)
                      ;; For ledgers, use the existing ns-record function
                      (let [ledger-alias   (get data "alias")
                            commit-address (get data "address")
                            t-value        (get-in data ["data" "t"])
                            index-address  (get-in data ["index" "address"])]
                        (ns-record ledger-alias commit-address t-value index-address)))
            record-bytes (json/stringify-UTF8 json-ld)]
        (log/debug "NS publish starting"
                   {:alias (or (:vg-name data) (get data "alias"))
                    :file filename})
        (let [res (<? (storage/write-bytes store filename record-bytes))]
          (log/debug "NS publish completed" {:alias (or (:vg-name data) (get data "alias")) :file filename})
          (when (is-virtual-graph-record? json-ld)
            ;; If this is a virtual graph, register dependencies
            (register-dependencies this json-ld))
          res))))

  (retract [this ledger-alias]
    (go-try
      (let [;; Check if target is a virtual graph (no :) or ledger (contains :)
            is-vg? (not (str/includes? ledger-alias ":"))
            filename (local-filename ledger-alias)
            address  (-> store
                         storage/location
                         (storage/build-address filename))]
        
        ;; If this is a VG, unregister dependencies first
        (when is-vg?
          (unregister-vg-dependencies this ledger-alias))
        
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
      ;; Use recursive listing to support ledger names with '/' characters
      (if (satisfies? storage/RecursiveListableStore store)
        (if-let [list-paths-result (storage/list-paths-recursive store const/ns-version)]
          (loop [remaining-paths (<? list-paths-result)
                 records []]
            (if-let [path (first remaining-paths)]
              (let [file-content (<? (storage/read-bytes store path))]
                (if file-content
                  (let [content-str (if (string? file-content)
                                      file-content
                                      #?(:clj  (if (bytes? file-content)
                                                 (String. ^bytes file-content "UTF-8")
                                                 (String. file-content "UTF-8"))
                                         :cljs (js/String.fromCharCode.apply nil file-content)))
                        record (json/parse content-str false)]
                    (recur (rest remaining-paths) (conj records record)))
                  (recur (rest remaining-paths) records)))
              records))
          [])
        ;; Fallback for stores that don't support ListableStore
        (do
          (log/warn "Storage backend does not support RecursiveListableStore protocol")
          [])))))

(defn start
  [store]
  (let [publisher (->StorageNameService store (atom {}))]
    ;; Initialize VG dependencies from existing records asynchronously (fire and forget)
    (go-try
      (<? (initialize-vg-dependencies publisher)))
    publisher))