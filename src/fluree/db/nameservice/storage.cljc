(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]))

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

(defn get-commit
  "Returns the minimal nameservice record."
  ([record]
   (get-commit record nil))
  ([record _branch]
   ;; Always return the record itself for new format
   record))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [;; Extract data from compact JSON-LD format (both genesis and regular commits now use this)
          ledger-alias   (get data "alias")
          branch         (or (get data "branch") "main")
          commit-address (get data "address")
          t-value        (get-in data ["data" "t"])
          index-address  (get-in data ["index" "address"])
          ns-metadata    (ns-record ledger-alias branch commit-address t-value index-address)
          record-bytes   (json/stringify-UTF8 ns-metadata)
          filename       (local-filename ledger-alias branch)]
      (storage/write-bytes store filename record-bytes)))

  (retract [_ ledger-alias]
    (let [filename (local-filename ledger-alias)
          address  (-> store
                       storage/location
                       (storage/build-address filename))]
      (storage/delete store address)))

  (publishing-address [_ ledger-alias]
    (go (publishing-address* store ledger-alias)))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias branch]} (nameservice/resolve-address (storage/location store) ledger-address nil)
            branch                  (or branch "main")
            filename                (local-filename alias branch)]
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (let [record (json/parse record-bytes false)]
            ;; Use get-commit to handle both new and legacy formats
            (get-commit record branch))))))

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
        (if-let [list-paths-result (storage/list-paths store "ns@v1")]
          (loop [remaining-paths (<? list-paths-result)
                 records []]
            (if-let [path (first remaining-paths)]
              (let [file-content (<? (storage/read-bytes store path))]
                (if file-content
                  (let [content-str (if (string? file-content)
                                      file-content
                                      #?(:clj (String. ^"[B" file-content "UTF-8")
                                         :cljs (js/String.fromCharCode.apply nil file-content)))
                        record (json/parse content-str false)]
                    (recur (rest remaining-paths) (conj records record)))
                  (recur (rest remaining-paths) records)))
              records))
          [])
        ;; Fallback for stores that don't support ListableStore
        (do
          (println "Storage backend does not support ListableStore protocol")
          [])))))

(defn start
  [store]
  (->StorageNameService store))
