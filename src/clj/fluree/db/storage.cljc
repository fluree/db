(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.json-ld :as json-ld]))

(defn hashable?
  [x]
  (or (string? x)
      #?(:clj (bytes? x))))

(defn sanitize-path
  [path]
  (if (str/starts-with? path "//")
    path
    (str "//" path)))

(defn build-address
  [ns method path]
  (let [path* (sanitize-path path)]
    (str/join ":" [ns method path*])))

(def fluree-namespace "fluree")

(defn build-fluree-address
  [method path]
  (build-address fluree-namespace method path))

(defn parse-address
  [address]
  (let [[ns method path] (str/split address #":")
        local            (if (str/starts-with? path "//")
                           (subs path 2)
                           path)]
    {:ns     ns
     :method method
     :local  local}))

(defn parse-local-path
  [address]
  (-> address parse-address :local))

(defprotocol JsonArchive
  (-read-json [store address keywordize?] "Returns value associated with `address`."))

(defprotocol ContentAddressedStore
  (-content-write [store k v]
    "Writes pre-serialized data `v` to store associated with key `k` and the
    hashed value of `v`. Returns value's address."))

(defprotocol ByteStore
  "ByteStore is used by consensus to replicate files across servers"
  (write-bytes [store path bytes] "Async writes bytes to path in store.")
  (read-bytes [store path] "Async read bytes from path in store."))

(defprotocol EraseableStore
  (delete [store address] "Remove value associated with `address` from the store."))

(defn content-write-json
  [store path data]
  (go-try
    (let [json   (json-ld/normalize-data data)
          bytes  (bytes/string->UTF8 json)
          result (<? (-content-write store path bytes))]
      (assoc result :json json))))

(defn read-json
  ([store address]
   (-read-json store address false))
  ([store address keywordize?]
   (-read-json store address keywordize?)))
