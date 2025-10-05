(ns fluree.db.transact.assert-graph-serialization-test
  (:require [babashka.fs :as bfs :refer [with-temp-dir]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json]))

(defn- list-json-files [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (filter #(str/ends-with? (.getName %) ".json"))))

(defn- read-json [f]
  (json/parse (slurp f) false))

(defn- data-file? [m]
  ;; Data files use JSON-LD with f context and mark type as f:DB
  (let [types (get m "@type")]
    (and (vector? types)
         (some #{"f:DB"} types))))

(defn- data-json-t [m]
  (get m "f:t"))

(defn- copy-dir-recursive [^String src ^String dst]
  (let [src-path (.toPath (io/file src))
        dst-path (.toPath (io/file dst))]
    (doseq [^java.io.File f (file-seq (io/file src))]
      (let [rel (.relativize src-path (.toPath f))
            tgt (.resolve dst-path rel)]
        (if (.isDirectory f)
          (java.nio.file.Files/createDirectories tgt (make-array java.nio.file.attribute.FileAttribute 0))
          (do
            (java.nio.file.Files/createDirectories (.getParent tgt) (make-array java.nio.file.attribute.FileAttribute 0))
            (java.nio.file.Files/copy (.toPath f) tgt (into-array java.nio.file.CopyOption [java.nio.file.StandardCopyOption/REPLACE_EXISTING])))))))
  dst)

(deftest ^:integration f-assert-contains-only-inserted-nodes
  (testing "f:assert should only contain inserted domain nodes (no query/insert/@graph scaffolding)"
    (with-temp-dir [storage-path {}]
      (let [storage (str storage-path)
            conn    @(fluree/connect-file {:storage-path storage
                                           :defaults {:indexing {:reindex-min-bytes 100
                                                                 :reindex-max-bytes 100000}}})
            ledger  "assert-check"

            _create @(fluree/create conn ledger)
            insert  {"@context" {"ex" "http://example.org/"}
                     "@graph"   [{"@id"  "ex:newData"
                                  "@type" "ex:ExampleClass"
                                  "ex:name" "New Item"
                                  "ex:description" "This is a new item"}]}]

        @(fluree/insert! conn ledger insert)

        ;; Give time for indexing to complete, so the next load reads from index
        (Thread/sleep 3000)

        ;; Create a second connection and load the same ledger before the second txn
        (let [conn2  @(fluree/connect-file {:storage-path storage
                                            :defaults {:indexing {:reindex-min-bytes 100
                                                                  :reindex-max-bytes 100000}}})]
          @(fluree/load conn2 ledger)
          (let [insert2 {"@context" {"ex" "http://example.org/"}
                         "@graph"   [{"@id"  "ex:newData2"
                                      "@type" "ex:ExampleClass"
                                      "ex:name" "New Item 2"
                                      "ex:description" "This is a second new item"}]}]
            @(fluree/insert! conn2 ledger insert2))

          (let [commit-dir (str storage "/" ledger "/commit")
                files      (list-json-files commit-dir)
                data-json  (->> files
                                (map read-json)
                                (filter data-file?)
                                (sort-by data-json-t)
                                last)]

            (is data-json "Data JSON should be found in commit directory")

          ;; Copy entire ledger directory to a stable, easily discoverable location
            (let [out-dir "/tmp/fluree-assert-inspect-latest"
                  src-ledger (str storage "/" ledger)
                  marker (str out-dir "/_SOURCE.txt")]
              (when (bfs/exists? out-dir)
                (bfs/delete-tree out-dir))
              (bfs/create-dirs out-dir)
              (copy-dir-recursive src-ledger out-dir)
              (spit marker (str "Copied from: " src-ledger "\n"))
              (println "Copied ledger storage to:" out-dir))

            (let [assert-items (get data-json "f:assert")]
              (is (vector? assert-items) "f:assert should be a vector of subject nodes")
              (is (= 1 (count assert-items)) "Our insert created exactly one subject node")

              (let [node (first assert-items)
                    node-id (get node "@id")
                    has-prop (fn [k]
                               (or (contains? node k)
                                   (contains? node (str "http://example.org/" (name k)))))]
                (is (string? node-id) "Inserted node should have an @id")
                (is (contains? #{"ex:newData" "http://example.org/newData"
                                 "ex:newData2" "http://example.org/newData2"}
                               node-id)
                    "@id should be one of the inserted subject IRIs")

              ;; Should not contain scaffolding artifacts
                (is (not (contains? node "query")) "Subject node should not include 'query'")
                (is (not (contains? node "insert")) "Subject node should not include 'insert'")
                (is (not (contains? node "@graph")) "Subject node should not include '@graph'")

              ;; Should contain our inserted properties (compact or expanded)
                (is (has-prop :name) "Subject should include name property")
                (is (has-prop :description) "Subject should include description property")))

          ;; Dump a full SPO query result to /tmp for inspection
            (let [results @(fluree/query-connection conn2 {"from" [ledger]
                                                           "@context" {"ex" "http://example.org/"}
                                                           "select" ["?s" "?p" "?o"]
                                                           "where"  [{"@id" "?s" "?p" "?o"}]})
                  out "/tmp/fluree-assert-inspect-latest/query_results.json"]
              (spit out (json/stringify results))
              (println "Wrote SPO query results to:" out))))))))
