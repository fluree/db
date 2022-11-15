(ns fluree.db.test-utils
  (:require [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.resource #?(:clj :refer :cljs :refer-macros) [inline-edn-resource]]))

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [context did]
     :or   {context (inline-edn-resource "default-context.edn")
            did     (-> "default-dev-private-key.edn"
                        inline-edn-resource
                        did/private->did-map)}}]
   @(fluree/connect-memory {:defaults {:context context
                                       :did     did}})))

(defn load-movies
  [conn]
  (let [ledger    @(fluree/create conn "test/movies")
        movies    (inline-edn-resource "movies.edn")
        stage1-db @(fluree/stage ledger movies)
        commit1   @(fluree/commit! stage1-db {:message "First commit!"
                                              :push?   true})]
    ledger))

(defn load-people
  [conn]
  (let [ledger @(fluree/create conn "test/people")
        people (inline-edn-resource "people.edn")
        staged @(fluree/stage ledger people)
        commit @(fluree/commit! staged {:message "Adding people", :push? true})]
    ledger))
