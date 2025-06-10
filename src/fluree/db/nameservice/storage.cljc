(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :refer [go]]
            [clojure.string :as str]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defn local-filename
  [ledger-alias]
  (str ledger-alias ".json"))

(defn publishing-address*
  [store ledger-alias]
  (-> store
      storage/location
      (storage/build-address ledger-alias)))

(defn ns-record
  "Generates nameservice metadata map for JSON storage. For now, since we only
  have a single branch possible, always sets default-branch. Eventually will
  need to merge changes from different branches into existing metadata map"
  [ns-address {address "address", alias "alias", branch "branch", :as commit-jsonld}]
  (let [branch-iri (str ns-address "(" branch ")")]
    {"@context"      "https://ns.flur.ee/ledger/v1"
     "@id"           ns-address
     "defaultBranch" branch-iri
     "ledgerAlias"   alias
     "branches"      [{"@id"     branch-iri
                       "address" address
                       "commit"  commit-jsonld}]}))

(defn get-commit
  ([record]
   (get-commit record nil))
  ([record branch]
   (log/info "Fetching commit from record:" record)
   (let [branch-iri (if branch
                      (str (get record "@id") "(" branch ")")
                      (get record "defaultBranch"))]
     (some #(when (= (get % "@id") branch-iri)
              (get % "commit"))
           (get record "branches")))))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ commit-jsonld]
    (let [ledger-alias (get commit-jsonld "alias")
          ns-address   (publishing-address* store ledger-alias)
          record       (ns-record ns-address commit-jsonld)
          record-bytes (json/stringify-UTF8 record)
          filename     (local-filename ledger-alias)]
      (storage/write-bytes store filename record-bytes)))

  (retract [_ ledger-alias]
    (storage/delete store (publishing-address* store (local-filename ledger-alias))))

  (publishing-address [_ ledger-alias]
    (go (publishing-address* store ledger-alias)))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      (let [{:keys [alias _branch]} (nameservice/resolve-address (storage/location store) ledger-address nil)
            filename                (local-filename alias)]
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (let [record (json/parse record-bytes false)]
            (get-commit record))))))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (storage/get-local-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/")))))

(defn start
  [store]
  (->StorageNameService store))
