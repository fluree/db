(ns fluree.db.nameservice.virtual-graph-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.connection :as connection]
            [fluree.db.nameservice.virtual-graph :as ns-vg]))

(deftest create-virtual-graph-test
  (testing "Creating a BM25 virtual graph via API"
    (let [conn @(fluree/connect-memory {})
          _ledger @(fluree/create conn "test-vg")]

      ;; Insert some test data
      @(fluree/insert! conn "test-vg"
                       {"@context" {"ex" "http://example.org/ns/"}
                        "@graph" [{"@id" "ex:article1"
                                   "ex:title" "Introduction to Fluree"
                                   "ex:content" "Fluree is a graph database"}
                                  {"@id" "ex:article2"
                                   "ex:title" "Advanced Queries"
                                   "ex:content" "Learn about complex queries"}]})

      (testing "Create BM25 virtual graph"
        (let [vg-obj @(fluree/create-virtual-graph
                       conn
                       {:name "article-search"
                        :type :bm25
                        :config {:stemmer "snowballStemmer-en"
                                 :stopwords "stopwords-en"
                                 :ledgers ["test-vg"]
                                 :query {"@context" {"ex" "http://example.org/ns/"}
                                         "where" [{"@id" "?x"
                                                   "@type" "ex:Article"}]
                                         "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})
              vg-name (:vg-name vg-obj)]
          (is (= "article-search" vg-name))

          ;; Verify the virtual graph exists
          (is (true? (async/<!! (ns-vg/virtual-graph-exists?
                                 (connection/primary-publisher conn)
                                 "article-search"))))

          ;; Verify we can retrieve the VG record
          (let [vg-record (async/<!! (ns-vg/get-virtual-graph
                                      (connection/primary-publisher conn)
                                      "article-search"))]
            (is (not= :not-found vg-record))
            (is (= "article-search" (get vg-record "@id")))
            (is (contains? (set (get vg-record "@type")) "fidx:BM25"))
            (is (= "ready" (get vg-record "f:status")))
            (is (= "article-search" (get vg-record "f:name")))
            (is (= [{"@id" "test-vg@main"}] (get vg-record "f:dependencies"))))))

      (testing "Cannot create duplicate virtual graph"
        (let [result @(fluree/create-virtual-graph
                       conn
                       {:name "article-search"
                        :type :bm25
                        :config {:ledgers ["test-vg"]}})]
          (is (instance? Exception result))
          (is (re-find #"Virtual graph already exists" (.getMessage ^Exception result)))))

      (testing "List all virtual graphs"
        (let [vgs (async/<!! (ns-vg/list-virtual-graphs
                              (connection/primary-publisher conn)))]
          (is (= 1 (count vgs)))
          (is (= "article-search" (get (first vgs) "@id")))))

      @(fluree/disconnect conn))))