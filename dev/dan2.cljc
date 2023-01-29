(ns dan2
  (:require [fluree.connector.api :as conn]
            [fluree.crypto :as crypto]
            [clojure.core.async :as async]
            [fluree.transactor.api :as txr]))

(def kp
  {:private "cbe947b1d64d7c031b904ec5dd438a2b6754fd8e77f2157825b69217d919590b",
   :public "032843a4cb103ab81d7a4568348d76c7776e951036f2a1ebb570c08ed99db6b9b0"})

(comment
  (def conn (conn/connect {:conn/store-config {:store/method :memory}
                           :conn/indexer-config {:reindex-min-bytes 10}}))

  (def ledger-name "dan1")

  (conn/create conn ledger-name {})

  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/dan
                                                          :ex/foo "bar"})

  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/kp
                                                          :ex/foo "bar"})

  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/ap
                                                          :ex/foo "bar"})

  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/pp
                                                          :ex/foo "bar"})

  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/mp
                                                          :ex/foo "bar"})
  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/sp
                                                          :ex/foo "bar"})
  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/sp
                                                          :ex/foo "foo"})
  (conn/transact conn ledger-name {:context {:ex "http://example.com/"}
                                                          "@id" :ex/sp
                                                          :ex/foo "wherefore"})

  (def dbs (-> conn :store :storage-atom deref))
  (def db (-> dbs (get (fluree.connector.core/head-db-address conn ledger-name))) )

  (conn/list conn)

  (conn/load conn ledger-name)

  (conn/load conn "wontwork")

  (conn/query conn ledger-name {:context {:ex "http://example.com/"}
                                                       :select {'?s [:*]}
                                                       :where [['?s :ex/foo '?f]]})


  [{"@id" :ex/sp, :ex/foo "bar"}
   {"@id" :ex/mp, :ex/foo "bar"}
   {"@id" :ex/pp, :ex/foo "bar"}
   {"@id" :ex/np, :ex/foo "bar"}
   {"@id" :ex/ap, :ex/foo "bar"}
   {"@id" :ex/kp, :ex/foo "bar"}
   {"@id" :ex/dan, :ex/foo "bar"}]

  (conn/close conn)



  ,)
