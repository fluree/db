(ns remote-conn
  (:require [clojure.core.async :as async]
            [fluree.db.api :as fluree]
            [fluree.db.util.xhttp :as xhttp]))

(comment

  (def conn @(fluree/connect {:method   :remote
                              :servers  "http://localhost:58090"
                              :defaults {}}))

  (def ledger @(fluree/load conn "my/test"))

  (def db (fluree/db ledger))



  @(fluree/query db {"@context" {"ex" "http://example.org/"}
                     "select"   {"?s" ["*"]}
                     "where"    {"@id"     "?s"
                                 "ex:name" nil}})


  )