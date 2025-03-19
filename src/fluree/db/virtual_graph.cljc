(ns fluree.db.virtual-graph
  (:require [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]))

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result"))

(defn vg-type-name
  [vg]
  (-> vg :type first iri/get-name str/lower-case))

(defn vg-type-kw
  [vg]
  (keyword (vg-type-name vg)))

(defmulti write-vg
          (fn [_index-catalog vg]
            (vg-type-kw vg)))

(defn storage-path
  [vg-type-kw db-alias vg-alias]
  (str/join "/" [db-alias (name vg-type-kw) vg-alias]))

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
