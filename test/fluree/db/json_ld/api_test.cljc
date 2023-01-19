(ns fluree.db.json-ld.api-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer-macros [deftest is testing async]])
            #?@(:cljs [[clojure.core.async :refer [go <!]]
                       [clojure.core.async.interop :refer [<p!]]])
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest exists?-test
  (testing "returns true after committing data to a ledger"
    #?(:clj
       (let [conn         (test-utils/create-conn)
             ledger-alias "testledger"
             ledger       @(fluree/create conn ledger-alias)
             db           @(fluree/stage (fluree/db ledger)
                                         [{:id           :f/me
                                           :type         :schema/Person
                                           :schema/fname "Me"}])]
         @(fluree/commit! ledger db)
         (is @(fluree/exists? conn ledger-alias))
         (is (not @(fluree/exists? conn "notaledger"))))

       :cljs
       (async done
         (go
           (let [conn         (<! (test-utils/create-conn))
                 ledger-alias "testledger"
                 ledger       (<p! (fluree/create conn ledger-alias))
                 db           (<p! (fluree/stage (fluree/db ledger)
                                                 [{:id           :f/me
                                                   :type         :schema/Person
                                                   :schema/fname "Me"}]))]
             (<p! (fluree/commit! ledger db))
             (is (<p! (fluree/exists? conn ledger-alias)))
             (is (not (<p! (fluree/exists? conn "notaledger"))))
             (done)))))))
