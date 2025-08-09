(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  "Returns the local filename for a ledger's nameservice record.
   If ledger-alias contains @branch, extracts it. Otherwise uses provided branch or 'main'."
  ([ledger-alias]
   (if (str/includes? ledger-alias "@")
     (let [[alias branch] (str/split ledger-alias #"@" 2)]
       (local-filename alias branch))
     (local-filename ledger-alias "main")))
  ([ledger-alias branch]
   (str "ns@v1/" ledger-alias "@" (or branch "main") ".json")))

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

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [;; Extract data from compact JSON-LD format (both genesis and regular commits now use this)
          combined-alias (get data "alias")
          ;; Parse branch from combined alias if present
          [ledger-alias branch] (if (str/includes? combined-alias "@")
                                  (str/split combined-alias #"@" 2)
                                  [combined-alias "main"])
          commit-address (get data "address")
          t-value        (get-in data ["data" "t"])
          index-address  (get-in data ["index" "address"])
          ns-metadata    (ns-record ledger-alias branch commit-address t-value index-address)
          record-bytes   (json/stringify-UTF8 ns-metadata)
          filename       (local-filename ledger-alias branch)]
      (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :branch branch :filename filename})
      (let [res (storage/write-bytes store filename record-bytes)]
        (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :branch branch :filename filename})
        res)))

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
      ;; ledger-address is just the alias (potentially with @branch)
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
        (if-let [list-paths-result (storage/list-paths-recursive store "ns@v1")]
          (loop [remaining-paths (<? list-paths-result)
                 records []]
            (if-let [path (first remaining-paths)]
              (let [file-content (<? (storage/read-bytes store path))]
                (if file-content
                  (let [content-str (if (string? file-content)
                                      file-content
                                      #?(:clj (let [^bytes bytes-content file-content]
                                                (String. bytes-content "UTF-8"))
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
  (->StorageNameService store))
