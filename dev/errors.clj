(ns errors
  (:require [fluree.db.json-ld.api :as fluree]
            [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn :refer [refresh refresh-all]]
            [clojure.core.async :as async]
            [malli.util :as mu]
            [malli.error :as me]
            [fluree.db.validation :as v]
            [malli.core :as m]
            [fluree.db.did :as did]
            [fluree.db.util.async :refer [<? <?? go-try merge-into?]]
            [fluree.db.flake :as flake]
            [fluree.db.util.json :as json]
            [fluree.db.serde.json :as serdejson]
            [fluree.db.storage :as storage]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.range :as query-range]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.json-ld :as json-ld]
            [clojure.string :as str]
            [criterium.core :refer [bench]]))


(comment
;;history errors
  (do
    (require '[fluree.db.test-utils :as test-utils])
    (require '[fluree.db.query.history :as history])
    (def conn        (test-utils/create-conn))
    (def ledger      @(fluree/create conn "historytest" {:defaultContext ["" {:ex "http://example.org/ns/"}]}))
    (def db1         @(test-utils/transact ledger [{:id   :ex/dan
                                                    :ex/x "foo-1"
                                                    :ex/y "bar-1"}
                                                   {:id   :ex/cat
                                                    :ex/x "foo-1"
                                                    :ex/y "bar-1"}
                                                   {:id   :ex/dog
                                                    :ex/x "foo-1"
                                                    :ex/y "bar-1"}])))


  ;;empty request
  (ex-message   @(fluree/history ledger {}))

  ;;invalid commit-details
  (ex-message   @(fluree/history ledger {:history [:ex/cat] :commit-details "I like cats"
                                         :t {:at :latest}}))

  ;;missing subject
  (ex-message   @(fluree/history ledger {:history nil}))

  ;;missing subject
  (ex-message   @(fluree/history ledger {:history []}))

  ;;invalid flake
  (ex-message   @(fluree/history ledger {:history [1 2] :t {:from 1}}))

  ;;invalid t
  (ex-message   @(fluree/history ledger {:history [:ex/cat] :t {:from 2 :to 0}}))

  ;;invalid t
  (ex-message   @(fluree/history ledger {:history [:ex/cat] :t {:from -2 :to -1}}))

  ;;missing t values
  (ex-message @(fluree/history ledger {:history [:ex/cat] :t {}}))

;;invalid t values
  (ex-message @(fluree/history ledger {:history [:ex/cat] :t {:at 1 :from 1}}))


;;fql

;;missing select
  (ex-message @(fluree/query {} '{:where  [[?s ?p ?o ]]}))

;;multiple selects
  (ex-message @(fluree/query {} '{:select [?s]
                                  :selectOne [?s ?p ]
                                  :where  [[?s ?p ?o ]]}))
;;invalid select var
  (ex-message @(fluree/query {} '{:select [+]
                                  :where  [[?s ?p ?o ]]}))

;;unknown key
  (ex-message @(fluree/query {} '{:select [?s]
                                  :where  [[?s ?p ?o ]]
                                  :foo [?o]}))

;;extra k/v in where map
  (ex-message @(fluree/query {} '{:select ['?name '?email]
                                  :where  [['?s :type :ex/User]
                                           ['?s :schema/age '?age]
                                           ['?s :schema/name '?name]
                                           {:union  [[['?s :ex/email '?email]]
                                                     [['?s :schema/email '?email]]]
                                            :filter ["(> ?age 30)"]}]}))

;;unrecognized op in where map
  (ex-message @(fluree/query {} {:select ['?name '?age]
                                 :where  [['?s :type :ex/User]
                                          ['?s :schema/age '?age]
                                          ['?s :schema/name '?name]
                                          {:foo "(> ?age 45)"}]}))

;;invalid where
  (ex-message @(fluree/query {} '{:select [?s ?o]
                                  :where  ?s}))

;;invalid where
  (ex-message @(fluree/query {} '{:select [?s ?o]
                                  :where  [?s ?p ?o]}))

;;invalid group-by
  (ex-message @(fluree/query {} '{:select   [?s]
                                  :where    [[?s ?p ?o]]
                                  :group-by {}}))

;;invalid order-by
  (ex-message @(fluree/query {} '{:select  ['?name '?favNums]
                                  :where   [['?s :schema/name '?name]
                                            ['?s :schema/age '?age]
                                            ['?s :ex/favNums '?favNums]]
                                  :orderBy [(foo  ?favNums)]}))

;;invalid bind
  (ex-message @(fluree/query {} '{:select [?firstLetterOfName ?name ?canVote]
                                  :where  [[?s :schema/age ?age]
                                           [?s :schema/name ?name]
                                           {:bind [?canVote           (>= ?age 18)]}]}))

;;invalid filter
  (ex-message @(fluree/query {} '{:select ['?name '?age]
                                  :where  [['?s :type :ex/User]
                                           ['?s :schema/age '?age]
                                           ['?s :schema/name '?name]
                                           {:filter "(> ?age 45)"}]}))

;;invalid filter
  (ex-message @(fluree/query {} '{:select ['?name '?age]
                                  :where  [['?s :type :ex/User]
                                           ['?s :schema/age '?age]
                                           ['?s :schema/name '?name]
                                           {:filter :foo}]}))

  )
