(ns fluree.db.virtual-graph
  (:refer-clojure :exclude [sync])
  (:require [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]))

(defn parse-vg-type-string
  "Parses a namespaced type string like 'fidx:bm25' to extract the type 'bm25'.
  Returns nil if type-str is nil."
  [type-str]
  (when type-str
    (-> type-str (str/split #":") second str/lower-case)))

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result"))

(defprotocol SyncableVirtualGraph
  (sync [this as-of] [this as-of opts]
    "Waits for the virtual graph to complete any pending indexing operations.
     Returns a promise-chan that resolves when the VG is fully synced.
     
     Parameters:
       as-of - Transaction 't' value to sync to, or nil to sync to latest known
     
     Options:
       :timeout - Maximum time to wait in milliseconds (default 10000)"))

(defn vg-type-name
  [vg]
  (let [vg-type (:type vg)]
    (cond
      ;; Handle SID type (old format)
      (and (vector? vg-type) (iri/sid? (first vg-type)))
      (-> vg-type first iri/get-name str/lower-case)

      ;; Handle string array type from nameservice
      (and (vector? vg-type) (string? (first vg-type)))
      (let [type-str (->> vg-type
                          (filter #(str/starts-with? % "fidx:"))
                          first)]
        (parse-vg-type-string type-str))

      ;; Handle single string type
      (string? vg-type)
      (if (str/includes? vg-type ":")
        (parse-vg-type-string vg-type)
        (str/lower-case vg-type))

      :else
      (throw (ex-info "Unknown VG type format" {:type vg-type})))))

(defn vg-type-kw
  [vg]
  (keyword (vg-type-name vg)))

(defmulti write-vg
  (fn [_index-catalog vg]
    (vg-type-kw vg)))

(defn vg-storage-path
  "Returns the storage path for a virtual graph that is independent of any ledger.
  Path structure: virtual-graphs/{vg-name}/{type}/"
  [vg-type vg-name]
  (str/join "/" ["virtual-graphs" vg-name (name vg-type)]))

(defmulti read-vg
  (fn [_index-catalog storage-meta]
    (-> storage-meta :type keyword)))
