(ns fluree.db.transact.default-context-test
  (:require [clojure.test :refer :all]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))


(deftest ^:integration default-context-update
  (let [conn            (test-utils/create-conn)
        ledger          @(fluree/create conn "default-context-update" {:defaultContext ["", {:ex "http://example.org/ns/"}]})
        db1             @(test-utils/transact ledger [{:id   :ex/foo
                                                       :ex/x "foo-1"
                                                       :ex/y "bar-1"}])
        ledger1-load    @(fluree/load conn "default-context-update")
        db1-load        (fluree/db ledger1-load)

        ;; change "ex" alias in default context to "ex-new"
        db-update-ctx   (fluree/update-default-context db1-load (-> (dbproto/-default-context db1-load)
                                                                    (dissoc "ex")
                                                                    (assoc "ex-new" "http://example.org/ns/")))
        db-update-cmt   (->> [{:id       :ex-new/foo2
                               :ex-new/x "foo-2"
                               :ex-new/y "bar-2"}]
                             (fluree/stage db-update-ctx)
                             deref
                             (fluree/commit! ledger)
                             deref)

        db-updated-load @(fluree/load conn "default-context-update")]

    (testing "Default context on db is correct."
      (is (= {"ex"     "http://example.org/ns/"
              "f"      "https://ns.flur.ee/ledger#"
              "id"     "@id"
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"
              "schema" "http://schema.org/"
              "sh"     "http://www.w3.org/ns/shacl#"
              "skos"   "http://www.w3.org/2008/05/skos#"
              "type"   "@type"
              "wiki"   "https://www.wikidata.org/wiki/"
              "xsd"    "http://www.w3.org/2001/XMLSchema#"}
             (dbproto/-default-context db1))))

    (testing "Default context on original db and loaded db are identical."
      (is (= (dbproto/-default-context db1-load)
             (dbproto/-default-context db1))))

    (testing "Default context working as expected with a query.."
      (is (= [{:ex/x "foo-1"
               :ex/y "bar-1"
               :id   :ex/foo}]
             @(fluree/query db1-load `{:select {?s [:*]}
                                       :where  [[?s :ex/x nil]]}))))

    (testing "Updated default context is correct"
      (is (= {"ex-new" "http://example.org/ns/"
              "f"      "https://ns.flur.ee/ledger#"
              "id"     "@id"
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "rdfs"   "http://www.w3.org/2000/01/rdf-schema#"
              "schema" "http://schema.org/"
              "sh"     "http://www.w3.org/ns/shacl#"
              "skos"   "http://www.w3.org/2008/05/skos#"
              "type"   "@type"
              "wiki"   "https://www.wikidata.org/wiki/"
              "xsd"    "http://www.w3.org/2001/XMLSchema#"}
             (dbproto/-default-context db-update-ctx))))

    (testing "Updated context db loaded is same as one before commit."
      (is (= (dbproto/-default-context (fluree/db db-updated-load))
             (dbproto/-default-context db-update-ctx))))


    (testing "Updated context commit db has all data expected"
      (is (= [{:ex-new/x "foo-2"
               :ex-new/y "bar-2"
               :id       :ex-new/foo2}
              {:ex-new/x "foo-1"
               :ex-new/y "bar-1"
               :id       :ex-new/foo}]
             @(fluree/query (fluree/db db-updated-load)
                            `{:select {?s [:*]}
                              :where  [[?s :ex-new/x nil]]}))))))
