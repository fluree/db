(ns fluree.db.util.tx-test
  (:require [clojure.test :refer :all]
            [fluree.crypto :as crypto]
            [fluree.db.util.core :as fc]
            [fluree.db.util.json :as fj]
            [fluree.db.util.tx :as ft]
            [fluree.db.flake :as flake])
  (:import (clojure.lang ExceptionInfo)))


(def ledger-auth {:private "a603e772faec02056d4ec3318187487d62ec46647c0cba7320c7f2a79bed2615"
                  :auth    "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2"
                  :sid     105553116266496})

(def user-auth {:private "602798c87164f0c1e1b2fe0f7f229d32218d828cc51ef78b462eccaa05983e4c"
                :auth    "Tf3sgBQ9G6EsrG65DXdWWamWXX3AxiDaq4z"})

(defn- contains-many? [m & ks]
  (every? #(contains? m %) ks))

(deftest db.util.txt-test
  (testing "validate-command"
    (testing "missing signature"
      (let [cmd (-> {:type        :default-key
                     :network     "test"
                     :dbid        "dl-1"
                     :private-key (:private user-auth)}
                    fj/stringify)]
        (is (thrown? NullPointerException (ft/validate-command {:cmd cmd})))))
    (testing "command contains authority but not auth"
      (let [cmd (-> {:type        :default-key
                     :network     "test"
                     :dbid        "dl-1"
                     :private-key (:private user-auth)
                     :authority   (:auth user-auth)}
                    fj/stringify)
            sig (crypto/sign-message cmd (:private user-auth))]
        (is (thrown-with-msg? ExceptionInfo
                              #"An authority without an auth is not allowed."
                              (ft/validate-command {:sig sig :cmd cmd})))))
    (testing "transaction is expired"
      (let [cmd (-> {:type        :default-key
                     :network     "test"
                     :dbid        "dl-1"
                     :private-key (:private user-auth)
                     :authority   (:auth user-auth)
                     :expire      (- (fc/current-time-millis) 1000)}
                    fj/stringify)
            sig (crypto/sign-message cmd (:private user-auth))]
        (is (thrown-with-msg? ExceptionInfo
                              #"Transaction is expired."
                              (ft/validate-command {:sig sig :cmd cmd})))))
    (testing "signing authority does not match command authority"
      (let [cmd (-> {:type        :default-key
                     :network     "test"
                     :dbid        "dl-1"
                     :private-key (:private user-auth)
                     :auth        (:auth user-auth)
                     :authority   (:auth ledger-auth)}
                    fj/stringify)
            sig (crypto/sign-message cmd (:private user-auth))]
        (is (thrown-with-msg? ExceptionInfo
                              #"does not match command authority"
                              (ft/validate-command {:sig sig :cmd cmd})))))
    (testing "valid construct"
      (let [cmd (-> {:type        :default-key
                     :network     "test"
                     :dbid        "dl-1"
                     :private-key (:private user-auth)
                     :auth        (:auth user-auth)}
                    fj/stringify)
            sig (crypto/sign-message cmd (:private user-auth))]
        (is (-> {:sig sig :cmd cmd}
                (ft/validate-command)
                map?)))))

  (testing "gen-tx-hash"
    (testing "with flakes, sorted?=false"
      (let [flakes '[[351843720888323,1002,70,-11,false,nil],
                     [351843720888323,1002,71,-11,true,nil],
                     [-11,100,"b93415dad2d742251a0424742dbadc458b000d6cc1eb6294650920894ede7c16",-11,true,nil],
                     [-11,101,105553116266496,-11,true,nil],
                     [-11,103,1617824831559,-11,true,nil],
                     [-11,106,"{\"type\":\"tx\",\"db\":\"test/chat\",\"tx\":[{\"_id\":[\"person/handle\",\"dsanchez\"],\"age\":71}],\"nonce\":1617824831559,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1617824861559}",-11,true,nil],
                     [-11,107,"1c30440220464a73c0fff6e6422880c62273819a8f6d542a85e05c46c1f460d5efe260e1f802203b755fec7b896117c0720e4b771003188b2c9d50f61889d3ba7c9fed37588eac",-11,true,nil]]]
        (is (->> flakes
                 (map flake/parts->Flake)
                 ft/gen-tx-hash
                 (= "3ce27f8ffdcffd39f9be9a2c9665b08bd6cae91defcd2531e201493d4c5fe301"))))
      (let [flakes '[[351843720888323,1002,70,-11,false,nil],
                     [351843720888323,1002,71,-11,true,nil],
                     [-11,99,"3ce27f8ffdcffd39f9be9a2c9665b08bd6cae91defcd2531e201493d4c5fe301",-11,true,nil],
                     [-11,100,"b93415dad2d742251a0424742dbadc458b000d6cc1eb6294650920894ede7c16",-11,true,nil],
                     [-11,101,105553116266496,-11,true,nil],
                     [-11,103,1617824831559,-11,true,nil],
                     [-11,106,"{\"type\":\"tx\",\"db\":\"test/chat\",\"tx\":[{\"_id\":[\"person/handle\",\"dsanchez\"],\"age\":71}],\"nonce\":1617824831559,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1617824861559}",-11,true,nil],
                     [-11,107,"1c30440220464a73c0fff6e6422880c62273819a8f6d542a85e05c46c1f460d5efe260e1f802203b755fec7b896117c0720e4b771003188b2c9d50f61889d3ba7c9fed37588eac",-11,true,nil]]]
        (is (->> flakes
                 (map flake/parts->Flake)
                 ft/gen-tx-hash
                 (not= "3ce27f8ffdcffd39f9be9a2c9665b08bd6cae91defcd2531e201493d4c5fe301")))))
    (testing "with flakes, sorted?=true"
      (let [flakes '[[351843720888323,1002,70,-11,false,nil],
                     [351843720888323,1002,71,-11,true,nil],
                     [-11,100,"b93415dad2d742251a0424742dbadc458b000d6cc1eb6294650920894ede7c16",-11,true,nil],
                     [-11,101,105553116266496,-11,true,nil],
                     [-11,103,1617824831559,-11,true,nil],
                     [-11,106,"{\"type\":\"tx\",\"db\":\"test/chat\",\"tx\":[{\"_id\":[\"person/handle\",\"dsanchez\"],\"age\":71}],\"nonce\":1617824831559,\"auth\":\"TfC8s3vD6CoFCgyPWWJgcMSmfwMWuvx9T5J\",\"expire\":1617824861559}",-11,true,nil],
                     [-11,107,"1c30440220464a73c0fff6e6422880c62273819a8f6d542a85e05c46c1f460d5efe260e1f802203b755fec7b896117c0720e4b771003188b2c9d50f61889d3ba7c9fed37588eac",-11,true,nil]]]
        (is (-> (map flake/parts->Flake flakes)
                (ft/gen-tx-hash true)
                (= "3ce27f8ffdcffd39f9be9a2c9665b08bd6cae91defcd2531e201493d4c5fe301"))))))

  (testing "generate-merkle-root"
    (let [block-tx-hash "86c8f426495523ece0aaec750cbf86e882795fb717f34ec53c6d71a88e56ffff"
          txn-hashes    '("9b94a3e764d5e008c01a68403460a2e0ecc8d9449ea530ef2d7b0e2923a391c7")]
      (is (-> (conj txn-hashes block-tx-hash)
              ft/generate-merkle-root
              (= "d418db9273c0647eca7c1e77aa8204e7d8820bdca2b2bacb429bec65bba31db9"))))
    (let [block-tx-hash "86c8f426495523ece0aaec750cbf86e882795fb717f34ec53c6d71a88e56ffff"]
      (is (-> block-tx-hash
              ft/generate-merkle-root
              (not= block-tx-hash)))))

  (testing "create-new-db-tx"
    (testing "minimal tx-map"
      (let [expire (+ (fc/current-time-millis) 1200000)
            nonce  1622637429403
            db-id  "test/dl-1"
            tx     [{ :db/id db-id }]
            cmd    (-> {:type        :new-db
                        :db          db-id
                        :tx          tx
                        :nonce       nonce
                        :auth        (:auth ledger-auth)
                        :expire      expire}
                       fj/stringify)
            tx-map {:tx-permissions {:root? true, :auth (:sid ledger-auth)},
                    :txid (crypto/sha3-256 cmd),
                    :db db-id,
                    :auth-sid (:sid ledger-auth),
                    :authority-sid nil,
                    :type "tx",
                    :expire expire,
                    :auth (:auth ledger-auth),
                    :sig (crypto/sign-message cmd (:private ledger-auth)),
                    :tx tx,
                    :cmd cmd,
                    :nonce nonce}
            ndb-tx  (-> tx-map ft/create-new-db-tx first)]
        (is (contains-many? ndb-tx :_action :alias :_id :id :root))
        (is (= (:auth ledger-auth) (:root ndb-tx)))
        (is (= :insert (:_action ndb-tx)))
        (is (= "db$newdb" (:_id ndb-tx)))
        (is (= db-id (:id ndb-tx) (:alias ndb-tx)))))
    (testing "tx-map with fork and doc"
      (let [expire  (+ (fc/current-time-millis) 1200000)
            nonce   1622637429403
            db-id   "test/dl-1"
            doc     "new database documentation"
            fork-db "test/original-db"
            block   5
            tx      [{ :db/id db-id :fork fork-db :forkBlock block :doc doc}]
            cmd     (-> {:type        :tx
                         :db          db-id
                         :tx          tx
                         :nonce       nonce
                         :auth        (:auth ledger-auth)
                         :expire      expire}
                        fj/stringify)
            tx-map {:tx-permissions {:root? true, :auth (:sid ledger-auth)},
                    :txid (crypto/sha3-256 cmd),
                    :db db-id,
                    :auth-sid (:sid ledger-auth),
                    :authority-sid nil,
                    :type "tx",
                    :expire expire,
                    :auth (:auth ledger-auth),
                    :sig (crypto/sign-message cmd (:private ledger-auth)),
                    :tx tx,
                    :cmd cmd,
                    :nonce nonce
                    :doc doc
                    :fork fork-db
                    :forkBlock block}
            ndb-tx  (-> tx-map ft/create-new-db-tx first)]
        (is (contains-many? ndb-tx :_action :alias :_id :id :root :fork :doc :forkBlock))
        (is (= (:auth ledger-auth) (:root ndb-tx)))
        (is (= :insert (:_action ndb-tx)))
        (is (= "db$newdb" (:_id ndb-tx)))
        (is (= db-id (:id ndb-tx) (:alias ndb-tx)))
        (is (= doc (:doc ndb-tx)))
        (is (= fork-db (:fork ndb-tx)))
        (is (= block (:forkBlock ndb-tx))))))

  (testing "make-candidate-db"
    (let [indexes [:spot :psot :post :opst]
          db (-> {:network "test" :dbid "db-1" :block 2 :t -4
                  :stats {:flakes 437, :size 46630, :indexed 1}
                  :permissions {:root? true, :collection {:all? true} :predicate {:all? true}}
                  :spot  {:block 1 :config {:idx-type :spot} :tempid nil}
                  :psot  {:block 1 :config {:idx-type :psot} :tempid nil}
                  :post  {:block 1 :config {:idx-type :post} :tempid nil}
                  :opst  {:block 1 :config {:idx-type :opst} :tempid nil}}
                 ft/make-candidate-db)]
      (is (->> indexes
               (mapv #(contains? db %))
               (every? true?)))
      (is (->> indexes
               (mapv #(->> [% :tempid]
                           (get-in db)
                           .toString))
               (apply =))))))

(comment
  [{:_id "db$newdb",
    :_action :insert,
    :id "test/db-1",
    :alias "test/db-1",
    :root "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2"}]


  )