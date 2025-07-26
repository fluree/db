(ns fluree.db.virtual-graph
  (:require [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]))

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result"))

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
        (when type-str
          (-> type-str (str/split #":") second str/lower-case)))

      ;; Handle single string type
      (string? vg-type)
      (if (str/includes? vg-type ":")
        (-> vg-type (str/split #":") second str/lower-case)
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

(defn trim-alias-ref
  "Virtual graph aliases are prefixed by `##` to indicate they are relative virtual graphs.

  When writing a virtual graph to storage, etc. we need to remove the `##` prefix to get the actual alias."
  [vg-alias]
  (if (str/starts-with? vg-alias "##")
    (subs vg-alias 2)
    vg-alias))
