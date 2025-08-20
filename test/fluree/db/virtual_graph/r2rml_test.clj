(ns fluree.db.virtual-graph.r2rml-test
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<?]]))

(def h2-spec {:dbtype "h2:mem"
              :dbname "r2rml_test;DB_CLOSE_DELAY=-1"})

(def create-sql ["CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(100));"
                 "INSERT INTO people (id, name) VALUES (1, 'Alice');"
                 "INSERT INTO people (id, name) VALUES (2, 'Bob');"])

(def r2rml-ttl (str "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
                    "@prefix ex: <http://example.org/> .\n"
                    "@prefix schema: <http://schema.org/> .\n"
                    "ex:PeopleMap a rr:TriplesMap;\n"
                    "  rr:logicalTable [ rr:tableName \"PEOPLE\" ];\n"
                    "  rr:subjectMap [ rr:template \"http://example.org/person/{ID}\"; rr:termType rr:IRI ];\n"
                    "  rr:predicateObjectMap [ rr:predicate schema:name; rr:objectMap [ rr:column \"NAME\" ] ] .\n"))

(defn with-h2 [f]
  (jdbc/with-db-connection [conn h2-spec]
    (doseq [s create-sql]
      (jdbc/execute! conn [s]))
    (f)))

(use-fixtures :once with-h2)

(defn memory-conn []
  (let [memory-config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                   "@vocab" "https://ns.flur.ee/system#"}
                       "@id"      "memory"
                       "@graph"   [{"@id"   "memoryStorage"
                                    "@type" "Storage"}
                                   {"@id"              "connection"
                                    "@type"            "Connection"
                                    "parallelism"      4
                                    "cacheMaxMb"       1000
                                    "commitStorage"    {"@id" "memoryStorage"}
                                    "indexStorage"     {"@id" "memoryStorage"}
                                    "primaryPublisher" {"@type"   "Publisher"
                                                        "storage" {"@id" "memoryStorage"}}}]}
        system-map (system/initialize (config/parse memory-config))]
    system-map))

(defn publish-vg! [publisher vg-name]
  (let [tmp-file (java.io.File/createTempFile "r2rml" ".ttl")]
    (spit tmp-file r2rml-ttl)
    (async/<!! (nameservice/publish publisher {:vg-name vg-name
                                               :vg-type "fidx:R2RML"
                                               :engine  :r2rml
                                               :config  {:mapping (.getAbsolutePath tmp-file)
                                                         :rdb {:jdbcUrl (str "jdbc:h2:mem:" (:dbname h2-spec))
                                                               :driver  "org.h2.Driver"}}
                                               :dependencies ["dummy-ledger@main"]}))))

(deftest r2rml-simple-graph-test
  (let [sys (memory-conn)
        conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
        publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
        _ (publish-vg! publisher "vg/sql")
        query {:from ["vg/sql"]
               :select ['?s '?name]
               :where [[:graph "vg/sql" {"@id" "?s" "http://schema.org/name" "?name"}]]}]
    (let [res @(fluree/query-connection conn query)
          _ (println "R2RML test raw result:" res)
          names (set (map second res))]
      (is (contains? names "Alice"))
      (is (contains? names "Bob")))))


