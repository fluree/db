(ns fluree.db.storage.file
  (:require #?(:clj [clojure.java.io :as io])
            #?@(:cljs [[fluree.db.platform :as platform]
                       ["fs" :as node-fs]
                       ["path" :as node-path]])
            [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.crypto :as crypto]
            [fluree.crypto.aes :as aes]
            [fluree.db.storage :as storage]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]))

#?(:clj (set! *warn-on-reflection* true))

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

(defn list-files-recursive
  "Recursively list all files (not directories) under a directory. Returns a channel with the results."
  [dir]
  #?(:clj
     (async/thread
       (let [^java.io.File dir-file (io/file dir)]
         (when (.exists dir-file)
           (->> (file-seq dir-file)
                (filter #(.isFile ^java.io.File %))
                (map #(.getPath ^java.io.File %))))))
     :cljs
     (async/go
       (if platform/BROWSER
         (throw (ex-info "Recursive file listing not supported in browser environment" {}))
         ;; Node.js implementation
         (let [find-files (fn find-files [current-dir acc]
                            (let [entries (node-fs/readdirSync current-dir #js {:withFileTypes true})]
                              (reduce (fn [acc entry]
                                        (let [entry-name (.-name entry)
                                              full-path (node-path/join current-dir entry-name)]
                                          (if (.isDirectory entry)
                                            (find-files full-path acc)
                                            (conj acc full-path))))
                                      acc
                                      entries)))]
           (find-files dir []))))))

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
        (when-let [data (<? (fs/read-file path encryption-key))]
          (json/parse data keywordize?)))))

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

  storage/ContentArchive
  (-content-read-bytes [_ address]
    (let [path (storage-path root address)]
      (fs/read-file path encryption-key)))

  (get-hash [_ address]
    (go
      (-> address
          storage/split-address
          last
          (str/split #"/")
          last
          storage/strip-extension)))

  storage/ByteStore
  (write-bytes [_ path bytes]
    (let [final-bytes (if encryption-key
                        (aes/encrypt bytes encryption-key {:output-format :none})
                        bytes)]
      (-> root
          (full-path path)
          (fs/write-file final-bytes))))

  (read-bytes [_ path]
    (-> root
        (full-path path)
        (fs/read-file encryption-key)))

  (swap-bytes [_ path f]
    (-> root
        (full-path path)
        (fs/with-file-lock f)))

  storage/RecursiveListableStore
  (list-paths-recursive [_ prefix]
    (go-try
      (let [prefix-path (full-path root prefix)]
        (if (<? (fs/exists? prefix-path))
          (let [all-files (<? (list-files-recursive prefix-path))
                base-path (str (fs/local-path root) "/")
                relative-files (map #(str/replace % base-path "") all-files)
                ;; Filter for .json files only
                json-files (filter #(str/ends-with? % ".json") relative-files)]
            json-files)
          [])))))

(defn open
  ([root-path]
   (open nil root-path nil))
  ([identifier root-path]
   (open identifier root-path nil))
  ([identifier root-path encryption-key]
   (->FileStore identifier root-path encryption-key)))
