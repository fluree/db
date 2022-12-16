(ns fluree.store.api-test
  (:require [clojure.test :as test :refer :all]
            [fluree.store.api :as store]))

#?(:clj
   (deftest file-store
     (testing "json"
       (let [file-store (store/start {:store/method :file
                                      :file-store/serialize-to :json
                                      :file-store/storage-path "dev/data"})

             out-file   "store.json"
             out        "hello"]
         (is (= :written (store/write file-store out-file out)))
         (is (= out (store/read file-store out-file)))

         (is (= :deleted (store/delete file-store out-file)))
         (is (nil? (store/read file-store out-file)))

         (is (= :stopped (store/stop file-store)))))
     (testing "edn"
       (let [file-store (store/start {:store/method :file
                                      :file-store/serialize-to :json
                                      :file-store/storage-path "dev/data"})

             out-file   "store.edn"
             out        "hello"]
         (is (= :written (store/write file-store out-file out)))
         (is (= out (store/read file-store out-file)))

         (is (= :deleted (store/delete file-store out-file)))
         (is (nil? (store/read file-store out-file)))

         (is (= :stopped (store/stop file-store)))))))

(deftest memory-store
  (let [mem-store (store/start {:store/method :memory})

        out-file "out.text"
        out "hello"]
    (is (= :written (store/write mem-store out-file out)))
    (is (= out (store/read mem-store out-file)))

    (is (= :deleted (store/delete mem-store out-file)))
    (is (nil? (store/read mem-store out-file)))

    (is (= :stopped (store/stop mem-store)))))
