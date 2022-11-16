(ns fluree.db.test-utils
  (:require [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]
            #?(:clj [fluree.resource :refer [inline-edn-resource]]))
  #?(:cljs (:require-macros [fluree.resource :refer [inline-edn-resource]])))

(def default-context
  (inline-edn-resource "default-context.edn"))

(def default-private-key
  (inline-edn-resource "default-dev-private-key.edn"))

(def movies
  (inline-edn-resource "movies.edn"))

(def people
  (inline-edn-resource "people.edn"))

(defn create-conn
  ([]
   (create-conn {}))
  ([{:keys [context did]
     :or   {context default-context
            did     (did/private->did-map default-private-key)}}]
   @(fluree/connect-memory {:defaults {:context context
                                       :did     did}})))

(defn load-movies
  [conn]
  (let [ledger @(fluree/create conn "test/movies")
        staged @(fluree/stage ledger movies)
        commit @(fluree/commit! staged {:message "First commit!", :push? true})]
    ledger))

(defn load-people
  [conn]
  (let [ledger @(fluree/create conn "test/people")
        staged @(fluree/stage ledger people)
        commit @(fluree/commit! staged {:message "Adding people", :push? true})]
    ledger))
