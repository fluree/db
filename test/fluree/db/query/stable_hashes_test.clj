(ns fluree.db.query.stable-hashes-test
  (:require [clojure.test :refer :all]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest stable-hashes-test
  (with-redefs [fluree.db.util.core/current-time-iso (constantly "1970-01-01T00:12:00.00000Z")]
    (let [conn   (test-utils/create-conn)
          ledger @(fluree/create conn "stable-commit-id"
                                 {:defaultContext ["" {:ex "http://example.org/ns/"}]})
          db0    @(fluree/stage
                   (fluree/db ledger)
                   [{:id           :ex/alice
                     :type         :ex/User
                     :schema/name  "Alice"
                     :schema/email "alice@flur.ee"
                     :schema/age   42}
                    {:id          :ex/bob,
                     :type        :ex/User,
                     :schema/name "Bob"
                     :schema/age  22}
                    {:id           :ex/jane,
                     :type         :ex/User,
                     :schema/name  "Jane"
                     :schema/email "jane@flur.ee"
                     :schema/age   30}])
          db1    @(fluree/commit! ledger db0)]
      (testing "stable commit id"
        (is (= "fluree:commit:sha256:bbxggb4sn2r6i73kk6gqeosfloq3dn33fnsdaadmjxrxkmkwxi2ya"
               (get-in db1 [:commit :id]))))
      (testing "stable commit address"
        (is (= "fluree:memory://fa19452088bf0e41f78668b3619097bc31340908891fdfdd0f3f425ece753981"
               (get-in db1 [:commit :address]))))
      (testing "stable default context id"
        (is (= "fluree:context:b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
               (get-in db1 [:commit :defaultContext :id]))))
      (testing "stable context address"
        (is (= "fluree:memory://b6dcf8968183239ecc7a664025f247de5b7859ac18cdeaace89aafc421eeddee"
               (get-in db1 [:commit :defaultContext :address]))))
      (testing "stable db id"
        (is (= "fluree:db:sha256:bbeducmbtm7ducvewuufjhl26p2a7v2mb5dasv5ykwdti2uamegm4"
               (get-in db1 [:commit :data :id]))))
      (testing "stable db address"
        (is (= "fluree:memory://2a0a2bcf83cd202649b3f3418116ccffe7857f03b8d3c5432e49907b667d06c0"
               (get-in db1 [:commit :data :address])))))))
