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

  (conn/create conn "dan1" {})
  "fluree:ledger:memory:head/dan1"
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/dan
                                                        :ex/foo "bar"})

  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/kp
                                                        :ex/foo "bar"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/ap
                                                        :ex/foo "bar"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/pp
                                                        :ex/foo "bar"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/mp
                                                        :ex/foo "bar"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/sp
                                                        :ex/foo "bar"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/sp
                                                        :ex/foo "foo"})
  (conn/transact conn "fluree:ledger:memory:head/dan1" {:context {:ex "http://example.com/"}
                                                        "@id" :ex/sp
                                                        :ex/foo "wherefore"})

  (def dbs (-> conn :store :storage-atom deref))
  (def db (-> dbs (get "fluree:db:memory:f7b93a3a-aa0d-461f-b03b-19fc673cd204")) )

  (fluree.connector.core/head-db-address conn "fluree:ledger:memory:head/dan1")

  (conn/list conn)

  (conn/load conn "fluree:ledger:memory:head/dan1")

  (let [{txr :transactor pub :publisher idxr :indexer} conn
        commit   (txr/resolve txr "fluree:commit:memory:dan1/commit/9c1c9d36140f26c67cf83bd66b67e6f8741526f0836ef4bd919b47249d0de648")
        {:keys [commit/assert commit/retract]} (:value commit)]
    commit)


  (conn/query conn "fluree:ledger:memory:head/dan2" {:context {:ex "http://example.com/"}
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
