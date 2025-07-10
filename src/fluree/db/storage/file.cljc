(ns fluree.db.storage.file
  (:require [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.crypto.aes :as aes]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]))

(def method-name "file")

(defn full-path
  [root relative-path]
  (str (fs/local-path root) "/" relative-path))

(defn storage-path
  [root address]
  (let [relative-path (storage/get-local-path address)]
    (full-path root relative-path)))

(defn file-address
  [identifier path]
  (storage/build-fluree-address identifier method-name path))

(defrecord FileStore [identifier root encryption-key]
  storage/Addressable
  (location [_]
    (storage/build-location storage/fluree-namespace identifier method-name))

  storage/Identifiable
  (identifiers [_]
    #{identifier})

  storage/JsonArchive
  (-read-json [_ address keywordize?]
    (go-try
      (let [path (storage-path root address)]
        #?(:clj
           ;; For Clojure, read raw bytes
           (try
             (let [file (java.io.File. path)]
               (when (.exists file)
                 (let [raw-bytes (java.nio.file.Files/readAllBytes (.toPath file))
                       data (if encryption-key
                              (String. (aes/decrypt raw-bytes encryption-key
                                                    {:input-format :none
                                                     :output-format :none}) "UTF-8")
                              (String. raw-bytes "UTF-8"))]
                   (json/parse data keywordize?))))
             (catch java.io.FileNotFoundException _
               nil))
           :cljs
           ;; For ClojureScript, use existing fs/read-file
           (when-let [raw-data (<? (fs/read-file path))]
             (let [data (if encryption-key
                          (aes/decrypt raw-data encryption-key
                                       {:input-format :none
                                        :output-format :string})
                          raw-data)]
               (json/parse data keywordize?)))))))

  storage/EraseableStore
  (delete [_ address]
    (let [path (storage-path root address)]
      (fs/delete-file path)))

  storage/ContentAddressedStore
  (-content-write-bytes [_ dir data]
    (go-try
      (when (not (storage/hashable? data))
        (throw (ex-info "Must serialize data before writing to FileStore."
                        {:root root
                         :path dir
                         :data data})))
      (let [hash          (crypto/sha2-256 data :base32)
            filename      (str hash ".json")
            path          (str/join "/" [dir filename])
            absolute      (full-path root path)
            original-size (count (if (string? data)
                                   (bytes/string->UTF8 data)
                                   data))
            bytes         (cond-> data
                            (string? data) bytes/string->UTF8
                            encryption-key (aes/encrypt encryption-key {:output-format :none}))]
        (<? (fs/write-file absolute bytes))
        {:path    path
         :address (file-address identifier path)
         :hash    hash
         :size    original-size})))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (let [final-bytes (if encryption-key
                        (aes/encrypt bytes encryption-key {:output-format :none})
                        bytes)]
      (-> root
          (full-path path)
          (fs/write-file final-bytes))))

  (read-bytes [_ path]
    (go-try
      #?(:clj
         ;; For Clojure, we need to read raw bytes
         (let [file-path (full-path root path)]
           (try
             (let [file (java.io.File. file-path)]
               (when (.exists file)
                 (let [raw-bytes (java.nio.file.Files/readAllBytes (.toPath file))]
                   (if encryption-key
                     (aes/decrypt raw-bytes encryption-key
                                  {:input-format :none
                                   :output-format :none})
                     raw-bytes))))
             (catch java.io.FileNotFoundException _
               nil)))
         :cljs
         ;; For ClojureScript, fs/read-file returns a string, need to handle differently
         (when-let [raw-data (<? (fs/read-file (full-path root path)))]
           (let [raw-bytes (bytes/string->UTF8 raw-data)]
             (if encryption-key
               (aes/decrypt raw-bytes encryption-key
                            {:input-format :none
                             :output-format :none})
               raw-bytes)))))))

(defn open
  ([root-path]
   (open nil root-path nil))
  ([identifier root-path]
   (open identifier root-path nil))
  ([identifier root-path encryption-key]
   (->FileStore identifier root-path encryption-key)))
