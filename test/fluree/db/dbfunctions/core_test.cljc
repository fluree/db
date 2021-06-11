(ns fluree.db.dbfunctions.core-test
  (:require #?@(:clj  [[clojure.test :refer :all]
                       [clojure.core.async :refer [go <!] :as async]]
                :cljs [[cljs.test :refer-macros [deftest is testing]]
                       [cljs.core.async :refer [go <!] :as async]])
            [clojure.string :as str]
            [fluree.db.dbfunctions.core :as f]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.core :as uc]
            [fluree.db.memorydb :as mdb]
            [test-helpers :refer [test-async]])
  #?(:clj  (:import (clojure.lang ExceptionInfo Symbol))))


;TODO: define invoice-db in separate helper library
(declare invoice-flakes)

(def ^:const test-network "test")
(def ^:const test-dbid (uc/random-uuid))
(def ^:const test-ledger (str test-network "/" test-dbid))
;#?(:clj (def test-db-ch
;          (let [conn (mdb/fake-conn)
;                db*  (mdb/new-db conn "test/one")]
;            (mdb/transact-flakes db* invoice-flakes))))
;#?(:clj (def test-db (async/<!! test-db-ch)))

;TODO: define utility functions in helper library
(defn- contains-many? [m & ks]
  (every? #(contains? m %) ks))

(defn- in?
  "true if coll contains elm; otherwise nil is returned"
  [coll elm]
  (some #(= elm %) coll))

(defn- in-many?
  "returns true when every elm is included in collection"
  [coll & elm]
  (every? #(in? coll %) elm))


(deftest db-dbfunctions-core-test
  (testing "clear-db-fn-cache"
    (let [fn-name "test-fn"
          res     {:key1 "value1"}
          _ (swap! f/db-fn-cache assoc [test-network test-dbid fn-name] res)
          ce      (get @f/db-fn-cache [test-network test-dbid fn-name])
          _ (f/clear-db-fn-cache)]
      (is (= res ce))
      (is (nil? (get @f/db-fn-cache [test-network test-dbid fn-name])))))

  (testing "tx-fn?"
    (is (false? (f/tx-fn? {:_id "chat"
                           :message "I have something to say!"
                           :person {:_id "person$jBond"
                                    :handle "jBond"
                                    :fullName "Jess Bond"}})))
    (is (nil? (f/tx-fn? "string")))
    (is (some? (f/tx-fn? "#(now)"))))

  (testing "resolve-local-fn"
    (let [local-fn (get f/default-fn-map (symbol "now"))
          fn-map   (f/resolve-local-fn local-fn)]
      (is (= local-fn (:f fn-map)))
      (is (contains-many? fn-map :f :params :arity :&args? :spec :code))))

  (testing "find-fn"
    (testing "default-fn-map function"
      (when-let [fn-map #?(:cljs nil
                           :clj  (-> (f/find-fn nil "now") async/<!! ))]
        (is (map? fn-map))
        (is (contains-many? fn-map :f :params :arity :&args? :spec :code))))
    (testing "_fn function"
      (when-let [fn-map #?(:cljs nil
                           :clj  (let [conn (mdb/fake-conn)
                                       db*  (mdb/new-db conn "test/one")
                                       db** (async/<!! (mdb/transact-flakes db* invoice-flakes))]
                                   (-> (f/find-fn db** "invoiceBuyer") async/<!!)))]
        (is (map? fn-map))
        (is (contains-many? fn-map :f :params :arity :&args? :spec :code))))
    (testing "exception"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj  (let [conn (mdb/fake-conn)
                                        db*  (<! (mdb/new-db conn "test/one"))]
                                    (-> (f/find-fn db* "jinkies") <!)))]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-data
                    :error
                    (= :db/invalid-fn))))
          (when-let [db* #?(:clj  nil
                            :cljs (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")))]
            (is (thrown-with-msg?
                  ExceptionInfo
                  #"DB functions not yet supported in javascript!"
                  (-> (f/find-fn db* "jinkies") <!))))))))

  (testing "combine-fns"
    (testing "collection of >1 functions"
      (let [input    ["(>= 0 count(:_id))" "(string? :value-1)"]
            expected (str "(and " (first input) " " (second input) ")")]
        (is (= expected (f/combine-fns input)))))
    (testing "collection of singleton function"
      (let [input ["(string? :value-1)"]]
        (is (= (first input) (f/combine-fns input)))))
    (testing "empty collection"
      (let [input []]
        (is (= nil (f/combine-fns input)))))
    (testing "collection of maps"
      (let [input [{:key-1 :value-1} {:key-2 :value-2}]
            expected (str "(and " (first input) " " (second input) ")")]
        (is (= expected (f/combine-fns input))))))

  ;TODO - test list of functions as element of vector
  (testing "parse-vector"
    (testing "valid values"
      (test-async
        (go
          ; testing valid inputs: string, number, booleans and nil
          ; parse-vector drops white-listed symbols (e.g. ?auth-id)
          ; as well as vectors)
          (let [v ["string" 12345 true false nil ["message" 123] '?auth-id]
                conn (mdb/fake-conn)
                db*  (mdb/new-db conn "test/one")
                db** (<! (mdb/transact-flakes db* invoice-flakes))
                res  (-> db** (f/parse-vector v) <!)]
            (is (in-many? res "string" 12345 true false))
            (is (nil? (in? res '?auth-id)))
            (is (nil? (in? res ["message" 123])))))))
    (testing "invalid value - symbol"
      (test-async
        (go
          (let [v [(symbol "foo" "bar")]
                conn (mdb/fake-conn)
                db*  (mdb/new-db conn "test/one")
                db** (<! (mdb/transact-flakes db* invoice-flakes))
                res  (-> db** (f/parse-vector v) <!)]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-data
                    :error
                    (= :db/invalid-fn)))))))
    (testing "invalid value - map"
      (test-async
        (go
          (let [v [{:key-1 "value-1"}]
                conn (mdb/fake-conn)
                db*  (mdb/new-db conn "test/one")
                db** (<! (mdb/transact-flakes db* invoice-flakes))
                res  (-> db** (f/parse-vector v) <!)]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-message
                    (str/starts-with? "Illegal element"))))))))

  ;TODO passing keyword as arg to function triggers java.lang.NullPointerException
  ;instead of Illegal element
  (testing "resolve-function"
    (testing "out-of-the-box function now"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj (-> mdb/fake-conn
                                     (mdb/new-db "test/one")
                                     (f/resolve-fn ["now"])
                                     <!)
                                     )]
            (is (= 2 (count res)))
            (is (-> res second symbol?)))
          (when-let [res #?(:clj   nil
                            :cljs  (-> mdb/fake-conn
                                      (mdb/new-db "test/one")
                                      (f/resolve-fn ["now"])
                                      <!))]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-message
                    (str/starts-with? "DB functions not yet supported in javascript!")))))))
    (testing "user-defined function, with type & params"
      (test-async
        (go
          (when-let [fn-def #?(:cljs nil
                               :clj  (-> (mdb/fake-conn)
                                         (mdb/new-db "test/one")
                                         (mdb/transact-flakes invoice-flakes)
                                         <!
                                         (f/resolve-fn ["invoiceBuyer"] "functionDec")
                                         <!))]
            (is (= 2 (count fn-def)))
            (is (-> fn-def second symbol?))))))
    (testing "user-defined function with invalid arity"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                             :clj (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")
                                      (mdb/transact-flakes invoice-flakes)
                                      <!
                                      (f/resolve-fn ["invoiceBuyer" "Fluree"])
                                      <!))]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-message
                    (str/starts-with? "Incorrect arity")))))))
    (testing "function with valid parameters"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj  (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")
                                      (mdb/transact-flakes invoice-flakes)
                                      <!
                                      (f/resolve-fn ["relationship?" "?sid" ["invoice/seller" "Fluree"] 1234567])
                                      <!))]
            (is (= 5 (count res)))
            (is (-> res second symbol?))))))
    (testing "function with list, boolean parameters"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj  (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")
                                      (mdb/transact-flakes invoice-flakes)
                                      <!
                                      (f/resolve-fn ["relationship?" (list '?auth_id) ["invoice/seller" "Fluree"] true])
                                      <!))]
            (is (= 5 (count res)))
            (is (-> res second symbol?))))))
    (testing "functionDec type function referencing non-whitelisted function - valid"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj  (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")
                                      (mdb/transact-flakes invoice-flakes)
                                      <!
                                      (f/resolve-fn ["relationship?"
                                                     (symbol "foo" "bar")
                                                     ["invoice/seller" "Fluree"]
                                                     '?auth_id] "functionDec")
                                      <!))]
            (is (= 5 (count res)))
            (is (-> res second symbol?))))))
    (testing "exception, reference non-whitelisted symbol"
      (test-async
        (go
          (when-let [res #?(:cljs nil
                            :clj  (-> (mdb/fake-conn)
                                      (mdb/new-db "test/one")
                                      (mdb/transact-flakes invoice-flakes)
                                      <!
                                      (f/resolve-fn ["relationship?"
                                                     (symbol "foo" "bar")
                                                     ["invoice/seller" "Fluree"]
                                                     '?auth_id])
                                      <!))]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-data
                    :error
                    (= :db/invalid-fn))))))))

  (testing "parse-fn"
    (testing "function true"
      (test-async
        (go
          (let [conn (mdb/fake-conn)
                db*  (mdb/new-db conn "test/one")
                db** (<! (mdb/transact-flakes db* invoice-flakes))
                fn-def (-> (f/parse-fn db** "true" "txn") <!)]
            #?(:clj (is (= (var f/true-or-false) fn-def)))
            #?(:cljs (is (fn? fn-def)))))))
    (testing "function (- 15 3)"
      (test-async
        (go
          (when-let [fn-def #?(:cljs nil
                               :clj  (-> (mdb/fake-conn)
                                         (mdb/new-db "test/one")
                                         <!
                                         (f/parse-fn "(- 15 3)" "txn")
                                         <!))]
            (is (fn? fn-def))))))
    (testing "invalid function"
      (test-async
        (go
          (let [res  (-> (mdb/fake-conn)
                         (mdb/new-db "test/one")
                         <!
                         (f/parse-fn "badFunction" "txn")
                         <!)]
            (is (instance? ExceptionInfo res))
            (is (-> res
                    ex-data
                    :error
                    (= :db/invalid-fn))))))))

(testing "execute-tx-fn"
  (testing "valid function"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj (-> (mdb/fake-conn)
                                   (mdb/new-db "test/one")
                                   (mdb/transact-flakes invoice-flakes)
                                   <!
                                   (f/execute-tx-fn 531245667555
                                                    nil
                                                    351843720888320
                                                    1000
                                                    "#(relationship? (?sid) [\"invoice/buyer\", \"org/employees\", \"_user/auth\"] (?auth_id))"
                                                    (atom {:stack   []
                                                           :credits 1000000
                                                           :spent   0})
                                                    7777777777)
                                   <!))]
          (is (true? res))))))
  (testing "invalid function"
    (test-async
      (go
        (let [res (-> (mdb/fake-conn)
                      (mdb/new-db "test/one")
                      <!
                      (f/execute-tx-fn 531245667555
                                       nil
                                       351843720888320
                                       1000
                                       "#invoiceBuyer"
                                       (atom {:stack   []
                                              :credits 1000000
                                              :spent   0})
                                       7777777777)
                      <!)]
          (is (instance? ExceptionInfo res))
          (is (-> res
                  ex-data
                  :error
                  (= :db/invalid-fn)))))))))

(def ^:const invoice-tuples
  [
   [140737488356329 80 "invoiceBuyer" -9 true nil]
   [140737488356329 81 "Only allow access if an employee of invoice buyer." -9 true nil]
   [140737488356329 82 "invoice" -9 true nil]
   [140737488356329 84 70368744178664 -9 true nil]
   [140737488356329 85 52776558133280 -9 true nil]
   [140737488356329 86 true -9 true nil]
   [140737488356328 80 "viewOrgs" -9 true nil]
   [140737488356328 81 "Can view all orgs." -9 true nil]
   [140737488356328 82 "org" -9 true nil]
   [140737488356328 84 70368744177664 -9 true nil]
   [140737488356328 85 52776558133280 -9 true nil]
   [140737488356328 86 true -9 true nil]
   [123145302311912 72 140737488356328 -9 true nil]
   [123145302311912 72 140737488356329 -9 true nil]
   [70368744178664 90 "invoiceBuyer" -9 true nil]
   [70368744178664 92 "(relationship? (?sid) [\"invoice/buyer\", \"org/employees\", \"_user/auth\"] (?auth_id))" -9 true nil]
   [70368744178664 93 "Only allow access if an employee of invoice buyer." -9 true nil]
   [-9 99 "3a168c2a29ae4d57c5a45cad604c7464b91702c6fd30b4a8102dab0f127966fd" -9 true nil]
   [-9 100 "f898bc4c2246cec25997f9fd28bce1bb07aef4286e6e5fb2ee4589fc8066e5b0" -9 true nil]
   [-9 101 105553116266496 -9 true nil]
   [-9 103 1619018141342 -9 true nil]
   [-9 106 "{\"type\":\"tx\",\"db\":\"fc/invoice\",\"tx\":[{\"_id\":\"_rule$viewOrgs\",\"id\":\"viewOrgs\",\"doc\":\"Can view all orgs.\",\"collection\":\"org\",\"collectionDefault\":true,\"fns\":[[\"_fn/name\",\"true\"]],\"ops\":[\"query\"]},{\"_id\":\"_rule$invoiceBuyer\",\"_rule/id\":\"invoiceBuyer\",\"_rule/doc\":\"Only allow access if an employee of invoice buyer.\",\"_rule/collection\":\"invoice\",\"_rule/fns\":[\"_fn$invoiceBuyer\"],\"_rule/ops\":[\"query\"],\"_rule/collectionDefault\":true},{\"_id\":\"_fn$invoiceBuyer\",\"_fn/name\":\"invoiceBuyer\",\"_fn/code\":\"(relationship? (?sid) [\\\"invoice/buyer\\\", \\\"org/employees\\\", \\\"_user/auth\\\"] (?auth_id))\",\"_fn/doc\":\"Only allow access if an employee of invoice buyer.\"},{\"_id\":[\"_role/id\",\"level1User\"],\"rules\":[\"_rule$viewOrgs\",\"_rule$invoiceBuyer\"]}],\"nonce\":1619018141342,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1619018171344}" -9 true nil]
   [-9 107 "1b304402203844e0b291d6a5c20320852f87f78c195756ecab152f3a974f91067ad90677ac0220753728607b30fbbd2f336e9003cdc5d6e1cecadb6eee5a8a6289de059325cd05" -9 true nil]
   [-9 108 "{\"_rule$viewOrgs\":140737488356328,\"_rule$invoiceBuyer\":140737488356329,\"_fn$invoiceBuyer\":70368744178664}" -9 true nil]
   [-10 1 "315fbb559a51998bc57b1be3fbc3bb4a4c043f826317a498e01cd6eb78ce1331" -10 true nil]
   [-10 2 "825e83e7c8ef0fc72e3cb74723155a9daed229ef1a711fb8180b4761345b0585" -10 true nil]
   [-10 3 -10 -10 true nil]
   [-10 3 -9 -10 true nil]
   [-10 4 105553116266496 -10 true nil]
   [-10 5 1619018141351 -10 true nil]
   [-10 6 5 -10 true nil]
   [-10 7 "1b3045022100b802d5be2a2469338144d0e41624fb91abe640b171f685657c1a1313da1de7100220470ec0b56e99068b3406e041e1f63fa527b5263cc59226e0c91410c339b9cc3b" -10 true nil]
   [-10 99 "24749093307f3f44c27b6635f76edfbc351079f0a76e8195ddbe4c8edbcd74c8" -10 true nil]
   [351843720888320 1000 "A" -7 true nil]
   [351843720888320 1001 369435906932738 -7 true nil]
   [351843720888320 1002 369435906932737 -7 true nil]
   [351843720888320 1003 52776558134248 -7 true nil]
   [351843720888320 1003 52776558134249 -7 true nil]
   [52776558134249 30 "invoice/items:app" -7 true nil]
   [52776558134248 30 "invoice/items:database" -7 true nil]
   [-7 99 "190c41ab9263c68436a44f417065f77f83d2ecf6e2cd7c438774c3096216c60d" -7 true nil]
   [-7 100 "527cb55bb29c5865859816f9b891c8689b1901b71456ec278fcad256299a59d3" -7 true nil]
   [-7 101 105553116266496 -7 true nil]
   [-7 103 1619018131512 -7 true nil]
   [-7 106 "{\"type\":\"tx\",\"db\":\"fc/invoice\",\"tx\":[{\"_id\":\"invoice\",\"id\":\"A\",\"buyer\":[\"org/name\",\"NewCo\"],\"seller\":[\"org/name\",\"Fluree\"],\"items\":[\"database\",\"app\"]}],\"nonce\":1619018131512,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1619018161514}" -7 true nil]
   [-7 107 "1c3044022059654bc0dcead71f95b996028531b2e746e90793732e55e978d591ce45e0c52602205b0e383334906d5c320a20781efc82a1d6298296bbbf51987a65f9224cd64b84" -7 true nil]
   [-7 108 "{\"invoice$1\":351843720888320,\"_tag$1\":52776558134248,\"_tag$2\":52776558134249}" -7 true nil]
   [-8 1 "825e83e7c8ef0fc72e3cb74723155a9daed229ef1a711fb8180b4761345b0585" -8 true nil]
   [-8 2 "e95af8ded47a19ed3e1c258b2538cb9f8211aa68e9ed89ea09ca676df46c76e6" -8 true nil]
   [-8 3 -8 -8 true nil]
   [-8 3 -7 -8 true nil]
   [-8 4 105553116266496 -8 true nil]
   [-8 5 1619018131521 -8 true nil]
   [-8 6 4 -8 true nil]
   [-8 7 "1b3044022005806bb9bad80deb474768c638c5e8892cad151b55f9cc0f68fa21484c6397ea022077728929374a14ad9bc28ed550445e3d5755bfa5b27bac47196961ea6bc14e1d" -8 true nil]
   [-8 99 "da07ef649baaf56e9d128f0c9f73361b5f108f457985c104e44f9ba7878fd295" -8 true nil]
   [369435906932738 1006 "NewCo" -5 true nil]
   [369435906932738 1007 87960930223081 -5 true nil]
   [369435906932737 1006 "Fluree" -5 true nil]
   [369435906932737 1007 87960930223080 -5 true nil]
   [369435906932737 1007 87960930223081 -5 true nil]
   [369435906932736 1006 "TechCo" -5 true nil]
   [369435906932736 1007 87960930223082 -5 true nil]
   [105553116267498 60 "TfLK8tUpjYpabx7p64c9ZRwSgNUCCvxAAWG" -5 true nil]
   [105553116267498 65 123145302311912 -5 true nil]
   [105553116267497 60 "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2" -5 true nil]
   [105553116267497 65 123145302311912 -5 true nil]
   [105553116267496 60 "TfKqMRbSU7cFzX9UthQ7Ca4GoEZg7KJWue9" -5 true nil]
   [105553116267496 65 123145302310912 -5 true nil]
   [87960930223082 50 "antonio" -5 true nil]
   [87960930223082 51 105553116267498 -5 true nil]
   [87960930223082 1010 "0x981ACbf8CFA4049FD3c71231F60a5Cfc9a0424A5" -5 true nil]
   [87960930223081 50 "scott" -5 true nil]
   [87960930223081 51 105553116267497 -5 true nil]
   [87960930223081 1010 "0x18C5B16138bcE633Af84b30250a4104d85434A23" -5 true nil]
   [87960930223080 50 "brian" -5 true nil]
   [87960930223080 51 105553116267496 -5 true nil]
   [87960930223080 1010 "0x4C3F081c0a2a3AB3faa90C2D6e2EFd7fdB6e0429" -5 true nil]
   [-5 99 "d3d7b2a6247bc1889e22927bef7577d12c9a197523297f9ed489d04e1d717569" -5 true nil]
   [-5 100 "917dd4a1b2e7f3eca4059e2646b23d209b659089ab69d648db6a912752fcc33b" -5 true nil]
   [-5 101 105553116266496 -5 true nil]
   [-5 103 1619018115225 -5 true nil]
   [-5 106 "{\"type\":\"tx\",\"db\":\"fc/invoice\",\"tx\":[{\"_id\":\"org\",\"name\":\"TechCo\",\"employees\":[\"_user$antonio\"]},{\"_id\":\"org\",\"name\":\"Fluree\",\"employees\":[\"_user$brian\",\"_user$scott\"]},{\"_id\":\"org\",\"name\":\"NewCo\",\"employees\":[\"_user$scott\"]},{\"_id\":\"_user$brian\",\"username\":\"brian\",\"auth\":[\"_auth$brian\"],\"ethID\":\"0x4C3F081c0a2a3AB3faa90C2D6e2EFd7fdB6e0429\"},{\"_id\":\"_user$scott\",\"username\":\"scott\",\"auth\":[\"_auth$scott\"],\"ethID\":\"0x18C5B16138bcE633Af84b30250a4104d85434A23\"},{\"_id\":\"_user$antonio\",\"username\":\"antonio\",\"auth\":[\"_auth$antonio\"],\"ethID\":\"0x981ACbf8CFA4049FD3c71231F60a5Cfc9a0424A5\"},{\"_id\":\"_auth$brian\",\"id\":\"TfKqMRbSU7cFzX9UthQ7Ca4GoEZg7KJWue9\",\"roles\":[[\"_role/id\",\"root\"]]},{\"_id\":\"_auth$scott\",\"id\":\"TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2\",\"roles\":[[\"_role/id\",\"level1User\"]]},{\"_id\":\"_auth$antonio\",\"id\":\"TfLK8tUpjYpabx7p64c9ZRwSgNUCCvxAAWG\",\"roles\":[[\"_role/id\",\"level1User\"]]}],\"nonce\":1619018115225,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1619018145228}" -5 true nil]
   [-5 107 "1c3045022100ea2cf7708191a3d1e66cd9bdfa6a46a1da4c20136d273a880a7cbabb1e19e08702201fb419ef978a5a72c482815549ff042f573eca69bdf0786de721187bb334d3e7" -5 true nil]
   [-5 108 "{\"_auth$antonio\":105553116267498,\"_user$brian\":87960930223080,\"_user$antonio\":87960930223082,\"_auth$scott\":105553116267497,\"org$2\":369435906932737,\"_user$scott\":87960930223081,\"org$3\":369435906932738,\"_auth$brian\":105553116267496,\"org$1\":369435906932736}" -5 true nil]
   [-6 1 "e95af8ded47a19ed3e1c258b2538cb9f8211aa68e9ed89ea09ca676df46c76e6" -6 true nil]
   [-6 2 "ed703614048888b68f0994812b97a6081c2435bc188a8901815858cb7c33073c" -6 true nil]
   [-6 3 -6 -6 true nil]
   [-6 3 -5 -6 true nil]
   [-6 4 105553116266496 -6 true nil]
   [-6 5 1619018115235 -6 true nil]
   [-6 6 3 -6 true nil]
   [-6 7 "1b304402201b2b5cace086e4379d34e7f93aae58939acd459f8c00c39301d8a478a6884416022065dbe91ae19d0c7d7bd11e7bbb3a8648496035b0491120368915ae15b3c6c22e" -6 true nil]
   [-6 99 "906f9aeb43955045ef2be707860cf31b4adc8b2c4b73c93d51d6df819066a918" -6 true nil]
   [123145302311912 70 "level1User" -3 true nil]
   [123145302311912 71 "A level 1 user. Can view orgs, some invoices." -3 true nil]
   [17592186044438 40 "invoiceReceipt" -3 true nil]
   [17592186044438 41 "Receipt acknowledgment of an invoice." -3 true nil]
   [17592186044437 40 "org" -3 true nil]
   [17592186044436 40 "invoice" -3 true nil]
   [1010 10 "_user/ethID" -3 true nil]
   [1010 12 52776558133249 -3 true nil]
   [1010 13 true -3 true nil]
   [1010 16 true -3 true nil]
   [1009 10 "invoiceReceipt/by" -3 true nil]
   [1009 11 "Who received the invoice." -3 true nil]
   [1009 12 52776558133250 -3 true nil]
   [1009 19 "_user" -3 true nil]
   [1008 10 "invoiceReceipt/date" -3 true nil]
   [1008 12 52776558133253 -3 true nil]
   [1008 15 true -3 true nil]
   [1007 10 "org/employees" -3 true nil]
   [1007 12 52776558133250 -3 true nil]
   [1007 14 true -3 true nil]
   [1007 19 "_user" -3 true nil]
   [1006 10 "org/name" -3 true nil]
   [1006 12 52776558133249 -3 true nil]
   [1006 13 true -3 true nil]
   [1005 10 "invoice/receipt" -3 true nil]
   [1005 12 52776558133250 -3 true nil]
   [1005 14 true -3 true nil]
   [1005 17 true -3 true nil]
   [1005 19 "invoiceReceipt" -3 true nil]
   [1004 10 "invoice/cost" -3 true nil]
   [1004 12 52776558133257 -3 true nil]
   [1004 15 true -3 true nil]
   [1003 10 "invoice/items" -3 true nil]
   [1003 12 52776558133263 -3 true nil]
   [1003 14 true -3 true nil]
   [1002 10 "invoice/seller" -3 true nil]
   [1002 12 52776558133250 -3 true nil]
   [1002 19 "org" -3 true nil]
   [1001 10 "invoice/buyer" -3 true nil]
   [1001 12 52776558133250 -3 true nil]
   [1001 19 "org" -3 true nil]
   [1000 10 "invoice/id" -3 true nil]
   [1000 12 52776558133249 -3 true nil]
   [1000 13 true -3 true nil]
   [-3 99 "3573a50d6be4c8e0d2830a4d110219dd58c015fb689d0bdbccee8b739397888b" -3 true nil]
   [-3 100 "e1bf1392549929d5bd9b7c27528b190bc16d6eddc2214cc386f741b79505f78b" -3 true nil]
   [-3 101 105553116266496 -3 true nil]
   [-3 103 1619018064749 -3 true nil]
   [-3 106 "{\"type\":\"tx\",\"db\":\"fc/invoice\",\"tx\":[{\"_id\":\"_collection\",\"name\":\"invoice\"},{\"_id\":\"_predicate\",\"name\":\"invoice/id\",\"type\":\"string\",\"unique\":true},{\"_id\":\"_predicate\",\"name\":\"invoice/buyer\",\"type\":\"ref\",\"restrictCollection\":\"org\"},{\"_id\":\"_predicate\",\"name\":\"invoice/seller\",\"type\":\"ref\",\"restrictCollection\":\"org\"},{\"_id\":\"_predicate\",\"name\":\"invoice/items\",\"multi\":true,\"type\":\"tag\"},{\"_id\":\"_predicate\",\"name\":\"invoice/cost\",\"type\":\"int\",\"index\":true},{\"_id\":\"_predicate\",\"name\":\"invoice/receipt\",\"type\":\"ref\",\"component\":true,\"multi\":true,\"restrictCollection\":\"invoiceReceipt\"},{\"_id\":\"_collection\",\"name\":\"org\"},{\"_id\":\"_predicate\",\"name\":\"org/name\",\"type\":\"string\",\"unique\":true},{\"_id\":\"_predicate\",\"name\":\"org/employees\",\"type\":\"ref\",\"multi\":true,\"restrictCollection\":\"_user\"},{\"_id\":\"_collection\",\"name\":\"invoiceReceipt\",\"doc\":\"Receipt acknowledgment of an invoice.\"},{\"_id\":\"_predicate\",\"name\":\"invoiceReceipt/date\",\"type\":\"instant\",\"index\":true},{\"_id\":\"_predicate\",\"name\":\"invoiceReceipt/by\",\"doc\":\"Who received the invoice.\",\"type\":\"ref\",\"restrictCollection\":\"_user\"},{\"_id\":\"_predicate\",\"name\":\"_user/ethID\",\"type\":\"string\",\"unique\":true,\"upsert\":true},{\"_id\":\"_role\",\"id\":\"level1User\",\"doc\":\"A level 1 user. Can view orgs, some invoices.\"}],\"nonce\":1619018064749,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1619018094754}" -3 true nil]
   [-3 107 "1b304402205fa2d121c31c3c3d320a820b3d6359a34be22f89faaeb417a2fcaa2f67c34df1022000e6876b471e6d3520630edd749a4b934c261d19950155969a5e9b6f3e55f532" -3 true nil]
   [-3 108 "{\"_predicate$2\":1001,\"_predicate$10\":1009,\"_collection$1\":17592186044436,\"_predicate$9\":1008,\"_predicate$7\":1006,\"_predicate$8\":1007,\"_role$1\":123145302311912,\"_predicate$5\":1004,\"_collection$2\":17592186044437,\"_predicate$4\":1003,\"_predicate$3\":1002,\"_predicate$1\":1000,\"_predicate$11\":1010,\"_predicate$6\":1005,\"_collection$3\":17592186044438}" -3 true nil]
   [-4 1 "ed703614048888b68f0994812b97a6081c2435bc188a8901815858cb7c33073c" -4 true nil]
   [-4 2 "4e8152706b63275091302d6a3c4a30b0c983b02ac8d771177194cbe5204bfd49" -4 true nil]
   [-4 3 -4 -4 true nil]
   [-4 3 -3 -4 true nil]
   [-4 4 105553116266496 -4 true nil]
   [-4 5 1619018064784 -4 true nil]
   [-4 6 2 -4 true nil]
   [-4 7 "1c304402206f8a57f9ef652ed532baa24e53d45e4c8fdf4d01cec3e3131a95cbcc6d6aedac022005f554e20ff38d28ffdcbe88c3ac8ff266411d311692f96d769c091e7b915499" -4 true nil]
   [-4 99 "8880873fc8c5aac8f914764b1e8c28eca6a34a5cce31767f19eed3ea12b22ebf" -4 true nil]])


(def ^:const invoice-flakes (mapv flake/parts->Flake invoice-tuples))