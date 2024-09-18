(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

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
  ([ns method path]
   (build-address ns nil method path))
  ([ns identifier method path]
   (let [path* (sanitize-path path)
         components (if identifier
                      [ns identifier method path*]
                      [ns method path*])]
     (str/join ":" components))))

(def fluree-namespace "fluree")

(def build-fluree-address
  (partial build-address fluree-namespace))

(defn split-address
  "Splits `address` into the fully qualified storage method and local path."
  [address]
  (str/split address #":(?!.*:)" 2))

(defn split-location
  [location]
  (let [components (str/split location #":")]
    (cond (= (count components) 3)
          components

          (= (count components) 2)
          (let [[ns method] components
                identifier  nil]
            [ns identifier method])

          :else (throw (ex-info (str "Invalid address location:" location)
                                {:status 500, :error :db/unexpected-error})))))

(defn parse-address
  [address]
  (let [[location path]   (split-address address)
        [ns identifier method] (split-location location)
        local             (if (str/starts-with? path "//")
                            (subs path 2)
                            path)]
    (cond-> {:ns     ns
             :method method
             :local  local}
      identifier (assoc :identifier identifier))))

(defn parse-local-path
  [address]
  (-> address parse-address :local))

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

(defrecord Catalog [])

#?(:clj
   (defmethod print-method Catalog [^Catalog clg, ^Writer w]
     (.write w (str "#fluree/Catalog "))
     (binding [*out* w]
       (pr (->> clg keys vec))))
   :cljs
     (extend-type Catalog
       IPrintWithWriter
       (-pr-writer [clg w opts]
         (-write w "#fluree/Catalog ")
         (-write w (pr (->> clg keys vec))))))

(defmethod pprint/simple-dispatch Catalog [^Catalog clg]
  (pr clg))

(defn catalog
  [& address-mappings]
  (into (->Catalog) (partition-all 2) address-mappings))

(defn async-location-error
  [location]
  (let [ex (ex-info (str "Unrecognized storage location:" location)
                    {:status 500, :error :db/unexpected-error})]
    (doto (async/chan)
      (async/put! ex))))

(defn read-address-json
  [clg address]
  (let [[location local-path] (split-address address)]
    (if-let [store (get clg location)]
      (read-json store local-path)
      (async-location-error location))))

(defn content-write-location-json
  [clg location path data]
  (if-let [store (get clg location)]
    (content-write-json store path data)
    (async-location-error location)))
