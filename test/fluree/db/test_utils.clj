(ns fluree.db.test-utils
  (:require [clojure.test :refer :all]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log])
  (:import (java.io PushbackReader)))

(defn load-edn-resource
  [resource-path]
  (with-open [r (-> resource-path io/resource io/reader PushbackReader.)]
    (edn/read r)))

(def default-did (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [context did]
     :or   {context (load-edn-resource "default_context.edn")
            did     default-did}}]
   @(fluree/connect-memory {:defaults {:context context
                                       :did     did}})))

(defn load-movies
  [conn]
  (let [ledger    @(fluree/create conn "test/movies")
        movies    (load-edn-resource "movies.edn")
        stage1-db @(fluree/stage ledger movies)
        commit1   @(fluree/commit! stage1-db {:message "First commit!"
                                              :push?   true})]
    ledger))
