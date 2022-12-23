(ns fluree.store.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.db.util.async :refer [<? <?? go-try]]
            [fluree.store.api :as store]))

#?(:clj
   (deftest file-store
     (testing "json"
       (let [storage-path "dev/data/"
             file-store   (store/start {:store/method            :file
                                        :file-store/serialize-to :json
                                        :file-store/storage-path storage-path})

             out-path  "store-test/"
             out-file1 (str out-path "file1")
             out1      "memory1"
             out-file2 (str out-path "file2")
             out2      "memory2"]
         (is (= {:path    "store-test/file1"
                 :address "fluree::file:store-test/file1"
                 :hash    "e27228f856bc2c87ab496cfa21d658b199b4f0ff3b46fd1d364227c22ac3f6bc"}
                (<?? (store/write file-store out-file1 out1))))
         (is (= out1 (<?? (store/read file-store out-file1))))
         (is (= {:path    "store-test/file2",
                 :address "fluree::file:store-test/file2",
                 :hash    "d8d10740f7b7b16830f652ed9faa81e81f126c14fc3d924f87b2b7d776bc1667"}
                (<?? (store/write file-store out-file2 out2))))
         (is (= out2 (<?? (store/read file-store out-file2))))

         (is (= #{"store-test/file1" "store-test/file2"}
                (into #{} (<?? (store/list file-store out-path)))))

         (is (= :deleted
                (<?? (store/delete file-store out-file1))))
         (is (nil? (<?? (store/read file-store out-file1))))

         (is (= :deleted (<?? (store/delete file-store out-file2))))
         (is (nil? (<?? (store/read file-store out-file2))))

         (is (= []
                (<?? (store/list file-store out-path))))

         (is (= :stopped (store/stop file-store)))))
     (testing "edn"
       (let [storage-path "dev/data/"
             file-store   (store/start {:store/method            :file
                                        :file-store/serialize-to :edn
                                        :file-store/storage-path storage-path})

             out-path  "store-test/"
             out-file1 (str out-path "file1")
             out1      "memory1"
             out-file2 (str out-path "file2")
             out2      "memory2"]
         (is (= {:path    "store-test/file1"
                 :address "fluree::file:store-test/file1"
                 :hash    "e27228f856bc2c87ab496cfa21d658b199b4f0ff3b46fd1d364227c22ac3f6bc"}
                (<?? (store/write file-store out-file1 out1))))
         (is (= out1 (<?? (store/read file-store out-file1))))
         (is (= {:path    "store-test/file2",
                 :address "fluree::file:store-test/file2",
                 :hash    "d8d10740f7b7b16830f652ed9faa81e81f126c14fc3d924f87b2b7d776bc1667"}
                (<?? (store/write file-store out-file2 out2))))
         (is (= out2 (<?? (store/read file-store out-file2))))

         (is (= #{"store-test/file1" "store-test/file2"}
                (into #{} (<?? (store/list file-store out-path)))))

         (is (= :deleted
                (<?? (store/delete file-store out-file1))))
         (is (nil? (<?? (store/read file-store out-file1))))

         (is (= :deleted (<?? (store/delete file-store out-file2))))
         (is (nil? (<?? (store/read file-store out-file2))))

         (is (= [] (<?? (store/list file-store out-path))))

         (is (= :stopped (store/stop file-store)))))))

(deftest memory-store
  (let [mem-store (store/start {:store/method :memory})

        out-path  "store-test/"
        out-file1 (str out-path "file1")
        out1      "memory1"
        out-file2 (str out-path "file2")
        out2      "memory2"]
    (is (= {:path "store-test/file1"
            :address "fluree::memory:store-test/file1"
            :hash "e27228f856bc2c87ab496cfa21d658b199b4f0ff3b46fd1d364227c22ac3f6bc"}
           (<?? (store/write mem-store out-file1 out1))))
    (is (= out1 (<?? (store/read mem-store out-file1))))
    (is (= {:path "store-test/file2",
            :address "fluree::memory:store-test/file2",
            :hash "d8d10740f7b7b16830f652ed9faa81e81f126c14fc3d924f87b2b7d776bc1667"}
           (<?? (store/write mem-store out-file2 out2))))
    (is (= out2 (<?? (store/read mem-store out-file2))))

    (is (= #{"store-test/file1" "store-test/file2"}
           (into #{} (<?? (store/list mem-store out-path)))))

    (is (= :deleted
           (<?? (store/delete mem-store out-file1))))
    (is (nil? (<?? (store/read mem-store out-file1))))

    (is (= :deleted (<?? (store/delete mem-store out-file2))))
    (is (nil? (<?? (store/read mem-store out-file2))))

    (is (= []
           (<?? (store/list mem-store out-path))))

    (is (= :stopped (store/stop mem-store)))))
