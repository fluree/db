(ns fluree.db.virtual-graph
  (:require [clojure.string :as str]
            [fluree.db.json-ld.iri :as iri]))

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result"))

(defn vg-type-name
  [vg-type]
  (-> vg-type iri/get-name str/lower-case))

(defmulti write-vg
  (fn [_index-catalog vg]
    (-> vg :type first vg-type-name keyword)))

(defn storage-path
  [vg-type db-alias vg-alias]
  (let [vg-path (vg-type-name vg-type)]
    (str/join "/" [db-alias vg-path vg-alias])))

(defmulti read-vg
  (fn [_index-catalog vg-address]
    (-> vg-address (str/split #"/") second keyword)))
