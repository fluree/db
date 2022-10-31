(ns json-ld.query
  (:require [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.api :as fdb]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.json-ld.transact :as jld-tx]
            [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.async :refer [<?? go-try channel?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.did :as did]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.db.indexer.default :as indexer]
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.util.log :as log]
            [fluree.db.index :as index]
            [criterium.core :as criterium]
            [clojure.data.avl :as avl]
            [clojure.tools.reader.edn :as edn]))



(comment

  (def ipfs-conn @(fluree/connect-ipfs
                    {:server   nil                          ;; use default
                     ;; ledger defaults used for newly created ledgers
                     :defaults {:ipns    {:key "self"}      ;; publish to ipns by default using the provided key/profile
                                :indexer {:reindex-min-bytes 9000
                                          :reindex-max-bytes 10000000}
                                :context {:id     "@id"
                                          :type   "@type"
                                          :xsd    "http://www.w3.org/2001/XMLSchema#"
                                          :schema "http://schema.org/"
                                          :sh     "http://www.w3.org/ns/shacl#"
                                          :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                          :rdfs   "http://www.w3.org/2000/01/rdf-schema#"
                                          :wiki   "https://www.wikidata.org/wiki/"
                                          :skos   "http://www.w3.org/2008/05/skos#"
                                          :f      "https://ns.flur.ee/ledger#"}
                                :did     (did/private->did-map "8ce4eca704d653dec594703c81a84c403c39f262e54ed014ed857438933a2e1c")}}))

  (def ledger @(fluree/create ipfs-conn "query/b" {:context {:ex "http://example.org/ns/"}}))


  ;; should work OK
  (def db
    @(fluree/stage
       ledger
       [{:id           :ex/brian,
         :type         :ex/User,
         :schema/name  "Brian"
         :ex/last      "Platz"
         :schema/email "brian@example.org"
         :schema/age   50
         :ex/favNums   7
         :ex/scores    [76 80 15]}
        {:id           :ex/alice,
         :type         :ex/User,
         :schema/name  "Alice"
         :ex/last      "Blah"
         :schema/email "alice@example.org"
         :schema/age   42
         :ex/favNums   [42, 76, 9]
         :ex/scores    [102 92.5 90]}
        {:id           :ex/cam,
         :type         :ex/User,
         :schema/name  "Cam"
         :ex/last      "Platz"
         :schema/email "cam@example.org"
         :schema/age   34
         :ex/favNums   [5, 10]
         :ex/scores    [97.2 100 80]
         :ex/friend    [:ex/brian :ex/alice]}]))

  @fluree.db.query.fql/LAST-PARSED

  @(fluree/query db {:context  {:ex "http://example.org/ns/"}
                     :select   ['?name '?favNums]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/favNums '?favNums]]
                     :group-by '?name})

  (=
     [["Alice" 127]
      ["Brian" 7]
      ["Cam" 15]])


  @(fluree/query db {:context  {:ex "http://example.org/ns/"}
                     :select   ['?name '(sum ?favNums)]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/last '?last]
                                ['?s :ex/favNums '?favNums]
                                ['?s :ex/scores '?scores]]
                     :group-by '?name})

  @(fluree/query db {:context  {:ex "http://example.org/ns/"}
                     :select   ['?name '?favNums]
                     :where    [['?s :schema/name '?name]
                                ['?s :ex/favNums '?favNums]]
                     :order-by ['?name '(desc ?favNums)]})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?name '?favNums]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/favNums '?favNums]]})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?name "(sum ?favNums)"]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/favNums '?favNums]]
                     :groupBy '?name})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?name '?favNums]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/favNums '?favNums]]})


  @(fluree/query db {:context   {:ex "http://example.org/ns/"}
                     :selectOne ['?name '?age '?email]
                     :where     [['?s :schema/name "Cam"]
                                 ['?s :ex/friend '?f]
                                 ['?f :schema/name '?name]
                                 ['?f :schema/age '?age]
                                 ['?f :schema/email '?email]]})


  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?age {'?f [:*]}]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/friend '?f]
                               ['?f :schema/age '?age]]
                     :vars    {'?name "Cam"}})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?f '?age]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/friend '?f]
                               ['?f :schema/age '?age]]
                     :vars    {'?name "Cam"}})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  ['?fname '?email {'?f [:*]}]
                     :where   [['?s :schema/name '?name]
                               ['?s :ex/friend '?f]
                               ['?f :schema/name '?fname]
                               ['?f :schema/email '?email]]
                     :vars    {'?name "Cam"}})


  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  [:*]
                     :from    :ex/brian})

  @(fluree/query db {:context {:ex "http://example.org/ns/"}
                     :select  {'?s [:* {:ex/friend [:*]}]}
                     :where   [['?s :rdf/type :ex/User]]})

  )
