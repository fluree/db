(ns fluree.db.test-fixtures
  (:require [clojure.test :refer :all]
            [fluree.db.did :as did]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))


(def default-ctx {:id     "@id"
                  :type   "@type"
                  :schema "http://schema.org/"
                  :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                  :wiki   "https://www.wikidata.org/wiki/"
                  :skos   "http://www.w3.org/2008/05/skos#"
                  :f      "https://ns.flur.ee/ledger#"})

(def default-did (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c"))

(def memory-conn nil)
(def ledgers {})

(defn get-ledger
  "Retrieves a specific ledger for tests based on ledger keyword"
  [ledger]
  (get ledgers ledger))

(defn create-conn
  [opts]
  (let [conn @(fluree/connect-memory
                {:defaults {:context default-ctx
                            :did     default-did}})]
    (alter-var-root #'memory-conn (constantly conn))))


(defn load-movies
  [conn]
  (let [ledger    @(fluree/create conn "test/movies")
        stage1-db @(fluree/stage
                     ledger
                     {"@context"                  "https://schema.org",
                      "id"                        "https://www.wikidata.org/wiki/Q836821",
                      "type"                      ["Movie"],
                      "name"                      "The Hitchhiker's Guide to the Galaxy",
                      "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
                      "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                      "isBasedOn"                 {"id"     "https://www.wikidata.org/wiki/Q3107329",
                                                   "type"   "Book",
                                                   "name"   "The Hitchhiker's Guide to the Galaxy",
                                                   "isbn"   "0-330-25864-8",
                                                   "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                             "@type" "Person"
                                                             "name"  "Douglas Adams"}}})
        commit1   @(fluree/commit! stage1-db {:message "First commit!"
                                              :push?   true})]
    ledger))

(defn load-sample-ledgers
  []
  (let [conn   memory-conn
        movies (load-movies conn)]
    (alter-var-root #'ledgers (fn [m] (assoc m :test/movies movies)))))


(defn stop*
  []
  (alter-var-root #'memory-conn (constantly nil))
  (alter-var-root #'ledgers (constantly {})))


(defn test-system
  "This fixture is intended to be used like this:
  (use-fixture :once test-system)
  It starts up an in-memory (by default) connection for testing."
  ([tests] (test-system {} tests))
  ([opts tests]
   (try
     (create-conn opts)
     (load-sample-ledgers)
     (tests)
     (catch Throwable e
       (log/error e "Caught test exception")
       (throw e))
     (finally (stop*)))))