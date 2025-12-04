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
  "Returns the local filename for a ledger's nameservice record.
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v2/ledger-name/branch.json'."
  [ledger-alias]
  (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (str const/ns-version "/" ledger-name "/" branch ".json")))

(defn index-filename
  "Returns the filename for a ledger's separate index record.
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v2/ledger-name/branch.index.json'.
   This separate file allows indexers to update index info without
   contending with transactors updating commit info."
  [ledger-alias]
  (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (str const/ns-version "/" ledger-name "/" branch ".index.json")))

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

(defn new-index-record
  "Generates a minimal index-only record for the separate index file.
   This file is used by indexers to update index info independently."
  [ledger-alias index-address index-t]
  {"@context" {"f" iri/f-ns}
   "@id"      ledger-alias
   "f:index"  {"@id" index-address
               "f:t"  index-t}})

(defn update-index-record
  "Updates an index-only record with new index info.
   Only updates if new index-t is greater than existing."
  [index-record ledger-alias index-address index-t]
  (if (some? index-record)
    (let [prev-t (get-in index-record ["f:index" "f:t"] 0)]
      (if (< prev-t index-t)
        (assoc index-record "f:index" {"@id" index-address "f:t" index-t})
        index-record))
    (new-index-record ledger-alias index-address index-t)))

(defn merge-index-into-record
  "Merges index info from the separate index file into a main NS record.
   Index file takes precedence if it has a higher index-t."
  [main-record index-record]
  (if index-record
    (let [main-index-t  (get-in main-record ["f:index" "f:t"] 0)
          file-index-t  (get-in index-record ["f:index" "f:t"] 0)]
      (if (> file-index-t main-index-t)
        (assoc main-record "f:index" (get index-record "f:index"))
        main-record))
    main-record))

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

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish-commit [_ ledger-alias commit-address commit-t]
    (let [filename (local-filename ledger-alias)]
      (log/debug "nameservice.storage/publish-commit start" {:ledger ledger-alias :filename filename})
      (storage/swap-json store filename
                         (fn [ns-record]
                           (if (some? ns-record)
                             (update-commit-address ns-record commit-address commit-t)
                             (new-ns-record ledger-alias commit-address commit-t nil nil))))))

  (publish-index [_ ledger-alias index-address index-t]
    (let [filename (index-filename ledger-alias)]
      (log/debug "nameservice.storage/publish-index start" {:ledger ledger-alias :filename filename})
      (storage/swap-json store filename
                         (fn [index-record]
                           (update-index-record index-record ledger-alias index-address index-t)))))

  (retract [_ ledger-alias]
    (go-try
      (let [main-filename  (local-filename ledger-alias)
            index-filename* (index-filename ledger-alias)
            main-address   (-> store storage/location (storage/build-address main-filename))
            index-address  (-> store storage/location (storage/build-address index-filename*))]
        ;; Delete both files - index file may not exist, which is fine
        (<? (storage/delete store main-address))
        (<? (storage/delete store index-address)))))

  (publishing-address [_ ledger-alias]
    ;; Just return the alias - lookup will handle branch extraction via local-filename
    (go ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      ;; ledger-address is just the alias (potentially with :branch)
      ;; Read both main file and index file in parallel, then merge
      (let [main-filename  (local-filename ledger-address)
            index-filename* (index-filename ledger-address)
            _              (log/debug "StorageNameService lookup:" {:ledger-address ledger-address
                                                                    :main-filename  main-filename
                                                                    :index-filename index-filename*})
            ;; Start both reads in parallel
            main-ch        (storage/read-bytes store main-filename)
            index-ch       (storage/read-bytes store index-filename*)
            ;; Await both results
            main-bytes     (<? main-ch)
            index-bytes    (<? index-ch)]
        (when main-bytes
          (let [main-record  (json/parse main-bytes false)
                index-record (when index-bytes (json/parse index-bytes false))]
            ;; Merge index file data into main record (index file takes precedence if newer)
            (merge-index-into-record main-record index-record))))))

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
          (let [all-paths (<? list-paths-result)
                ;; Filter out .index.json files - they're supplementary and will be merged
                main-paths (filterv #(not (str/ends-with? % ".index.json")) all-paths)]
            (loop [remaining-paths main-paths
                   records         []]
              (if-let [path (first remaining-paths)]
                (if-some [file-content (<? (storage/read-bytes store path))]
                  (let [content-str  (if (string? file-content)
                                       file-content
                                       #?(:clj (let [^bytes bytes-content file-content]
                                                 (String. bytes-content "UTF-8"))
                                          :cljs (js/String.fromCharCode.apply nil file-content)))
                        main-record  (json/parse content-str false)
                          ;; Try to read corresponding index file (branch.json -> branch.index.json)
                        index-path   (str/replace path #"\.json$" ".index.json")
                        index-record (when-let [idx-content (<? (storage/read-bytes store index-path))]
                                       (let [idx-str (if (string? idx-content)
                                                       idx-content
                                                       #?(:clj (let [^bytes bytes-content idx-content]
                                                                 (String. bytes-content "UTF-8"))
                                                          :cljs (js/String.fromCharCode.apply nil idx-content)))]
                                         (json/parse idx-str false)))
                          ;; Merge index data into main record
                        merged       (merge-index-into-record main-record index-record)]
                    (recur (rest remaining-paths) (conj records merged)))
                  (recur (rest remaining-paths) records))
                records)))
          [])
        ;; Fallback for stores that don't support ListableStore
        (do
          (log/warn "Storage backend does not support RecursiveListableStore protocol")
          [])))))

(defn start
  [store]
  (->StorageNameService store))
