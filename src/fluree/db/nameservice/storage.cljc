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

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [ledger-alias (get data "alias")]
      (if (not ledger-alias)
        (do (log/warn "nameservice.storage/publish missing alias in commit data; skipping" {:data-keys (keys data)})
            (go nil))
        (let [filename     (local-filename ledger-alias)
              _            (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :filename filename})
              commit-address (get data "address")
              commit-t       (get-in data ["data" "t"])
              index-address  (get-in data ["index" "address"])
              index-t        (get-in data ["index" "data" "t"])
              record-updater (fn [ns-record]
                               (update-ns-record ns-record ledger-alias commit-address commit-t index-address index-t))
              res            (storage/swap-json store filename record-updater)]
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

(defn start
  [store]
  (->StorageNameService store))
