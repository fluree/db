(ns fluree.db.storage.file-test
  (:require [babashka.fs :as bfs :refer [with-temp-dir]]
            [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.storage :as storage]
            [fluree.db.storage.file :as file-storage]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]))

(deftest encryption-test
  (testing "FileStore encryption functionality"
    (with-temp-dir [test-dir {}]
      (let [test-dir-str (str test-dir)
            aes-key "test-key-32-bytes-exactly!!!!!!"
            test-data "This is sensitive data that should be encrypted"
            test-path "test/data.txt"
            test-bytes (bytes/string->UTF8 test-data)]

        (testing "Writing and reading with encryption"
          (let [encrypted-store (file-storage/open "test-encrypted" test-dir-str aes-key)]
            ;; Write data
            (async/<!! (storage/write-bytes encrypted-store test-path test-bytes))

            ;; Read it back
            (let [read-back (async/<!! (storage/read-bytes encrypted-store test-path))]
              (is (string? read-back) "Should return string")
              (is (= test-data read-back)
                  "Data should be readable with correct key"))))

        (testing "Raw encrypted file is not readable as plaintext"
          (let [encrypted-store (file-storage/open "test-encrypted" test-dir-str aes-key)]
            ;; Write data
            (async/<!! (storage/write-bytes encrypted-store test-path test-bytes))

            ;; Read the raw file directly
            (let [raw-file-data (async/<!! (fs/read-file (str test-dir-str "/" test-path)))]
              (is (string? raw-file-data) "fs/read-file returns a string")
              (is (not= test-data raw-file-data)
                  "Raw file data should be encrypted, not match plaintext"))))

        (testing "Cannot read encrypted data with wrong key"
          (let [encrypted-store (file-storage/open "test-encrypted" test-dir-str aes-key)
                wrong-key-store (file-storage/open "test-wrong" test-dir-str "wrong-key-32-bytes-exactly!!!!!!")
                _ (async/<!! (storage/write-bytes encrypted-store test-path test-bytes))
                ;; AES decryption with wrong key should throw BadPaddingException
                result (async/<!! (storage/read-bytes wrong-key-store test-path))]
            (is (instance? Throwable result) "Should return an exception")
            (is (or (instance? javax.crypto.BadPaddingException (.getCause ^Throwable result))
                    (re-find #"BadPaddingException" (str result)))
                "Should throw BadPaddingException when using wrong key")))

        (testing "Content-addressable storage with encryption"
          (let [encrypted-store (file-storage/open "test-cas" test-dir-str aes-key)
                json-data "{\"test\": \"data\"}"
                result (async/<!! (storage/-content-write-bytes encrypted-store "content" json-data))]

            (is (:hash result) "Should return hash")
            (is (:path result) "Should return path")
            (is (= (count json-data) (:size result)) "Should return original size, not encrypted size")

            ;; Verify the file is encrypted on disk
            (let [full-path (str test-dir-str "/" (:path result))
                  raw-data (async/<!! (fs/read-file full-path))]

              (is (not= json-data raw-data)
                  "Content-addressed file should be encrypted"))))))))

(deftest api-integration-test
  (testing "connect-file passes encryption key to FileStore"
    (with-temp-dir [test-dir {}]
      (let [test-dir-str (str test-dir)
            aes-key "my-secure-32-byte-aes256-key!!!!"
            ;; Create a file store directly to verify it works
            file-store (file-storage/open "test-id" test-dir-str aes-key)]

        (testing "FileStore created with encryption key"
          (is (instance? fluree.db.storage.file.FileStore file-store)
              "Should create FileStore")
          (is (= aes-key (:encryption-key file-store))
              "FileStore should have the encryption key"))

        (testing "FileStore can encrypt and decrypt data"
          (let [test-data "Test data for encryption"
                test-path "test/encrypted-data.txt"
                test-bytes (bytes/string->UTF8 test-data)]

            ;; Write and read through the FileStore
            (async/<!! (storage/write-bytes file-store test-path test-bytes))
            (let [read-back (async/<!! (storage/read-bytes file-store test-path))]
              (is (= test-data read-back)
                  "Data should be readable through FileStore"))

            ;; Verify raw file is encrypted
            (let [raw-content (async/<!! (fs/read-file (str test-dir-str "/" test-path)))]
              (is (not= test-data raw-content)
                  "Raw file should be encrypted"))))))))