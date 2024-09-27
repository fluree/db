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

(defn build-location
  ([ns identifier method]
   (build-location ns identifier method []))
  ([ns identifier method auxiliary]
   (->> [ns identifier (name method) (not-empty auxiliary)]
        (remove nil?)
        flatten
        (str/join ":"))))

(defn sanitize-path
  [path]
  (if (str/starts-with? path "//")
    path
    (str "//" path)))

(defn build-address
  ([ns method path]
   (build-address ns nil method path))
  ([ns identifier method path]
   (build-address ns identifier method path nil))
  ([ns identifier method path auxiliary]
   (let [location (build-location ns identifier method auxiliary)
         path*    (sanitize-path path)]
     (str/join ":" [location path*]))))

(def fluree-namespace "fluree")

(def build-fluree-address
  (partial build-address fluree-namespace))

(defn split-address
  "Splits `address` into the fully qualified storage method and local path."
  [address]
  (str/split address #":(?!.*:)" 2))

(defn valid-identifier?
  [x]
  (str/includes? x "/"))

(defn parse-location
  [location]
  (let [components   (str/split location #":")
        address-ns   (nth components 0)
        id-or-method (nth components 1)]
    (if (valid-identifier? id-or-method)
      (let [identifier id-or-method
            method     (nth components 2)
            auxiliary  (-> components (subvec 3) not-empty)]
        (cond-> {:ns address-ns :identifier identifier, :method method}
          auxiliary (assoc :auxiliary auxiliary)))
      (let [method     id-or-method
            auxiliary  (-> components (subvec 2) not-empty)]
        (cond-> {:ns address-ns, :method method}
          auxiliary (assoc :auxiliary auxiliary))))))

(defn get-identifier
  [location]
  (-> location parse-location :identifier))

(defn parse-address
  [address]
  (let [[location path] (split-address address)
        parsed          (parse-location location)
        local           (if (str/starts-with? path "//")
                          (subs path 2)
                          path)]
    (assoc parsed :local local)))

(defn get-local-path
  [address]
  (-> address parse-address :local))

(defprotocol Addressable
  (location [store]))

(defprotocol Identifiable
  (identifiers [store]))

(defprotocol JsonArchive
  (-read-json [store address keywordize?] "Returns value associated with `address`."))

(defprotocol ContentAddressedStore
  (-content-write-bytes [store k v]
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
          result (<? (-content-write-bytes store path bytes))]
      (assoc result :json json))))

(defn read-json
  ([store address]
   (-read-json store address false))
  ([store address keywordize?]
   (-read-json store address keywordize?)))
