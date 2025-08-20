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
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v1/ledger-name/branch.json'."
  [ledger-alias]
  (let [[ledger-name branch] (str/split ledger-alias #":" 2)]
    (str "ns@v1/" ledger-name "/" branch ".json")))

(defn ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch'."
  [ledger-alias commit-address t index-address]
  (let [[alias branch] (str/split ledger-alias #":" 2)]
    (cond-> {"@context"     {"f" iri/f-ns}
             "@id"          ledger-alias  ;; Already includes :branch
             "@type"        ["f:Database" "f:PhysicalDatabase"]
             "f:ledger"     {"@id" alias}  ;; Just the ledger name without branch
             "f:branch"     branch
             "f:commit"     {"@id" commit-address}
             "f:t"          t
             "f:status"     "ready"}
      index-address (assoc "f:index" {"@id" index-address}))))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [;; Extract data from compact JSON-LD format (both genesis and regular commits now use this)
          ledger-alias   (get data "alias")  ;; Already includes @branch
          commit-address (get data "address")
          t-value        (get-in data ["data" "t"])
          index-address  (get-in data ["index" "address"])
          ns-metadata    (ns-record ledger-alias commit-address t-value index-address)
          record-bytes   (json/stringify-UTF8 ns-metadata)
          filename       (local-filename ledger-alias)]
      (storage/write-bytes store filename record-bytes)))

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
