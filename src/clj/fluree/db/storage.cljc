(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.remote-system :as remote-system]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

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
  (-location [section]))

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

(defn section-entry
  [section]
  (let [loc (-location section)]
    [loc section]))

(defn with-remote-system
  [remote-section remote-system]
  (reduce (fn [sec address-identifier]
            (assoc sec address-identifier remote-system))
          remote-section (remote-system/address-identifiers remote-system)))

(defn remote-systems->section
  [remote-systems]
  (reduce with-remote-system {} remote-systems))

(defn catalog
  [local-stores remote-systems]
  (let [remote-section (remote-systems->section remote-systems)]
    (-> (->Catalog)
        (into (map section-entry) local-stores)
        (assoc ::remote remote-section))))

(defn get-local-store
  [ctlg location]
  (get ctlg location))

(defn get-remote-system
  [ctlg location]
  (when-let [identifier (get-identifier location)]
    (-> ctlg ::remote (get identifier))))

(defn async-location-error
  [address]
  (let [ex (ex-info (str "Unrecognized storage location:" address)
                    {:status 500, :error :db/unexpected-error})]
    (doto (async/chan)
      (async/put! ex))))

(defn locate-address
  [ctlg address]
  (let [[location _local-path] (split-address address)]
    (or (get-local-store ctlg location)
        (get-remote-system ctlg location))))

(defn read-address-json
  [ctlg address]
  (if-let [store (locate-address ctlg address)]
    (read-json store address)
    (async-location-error address)))

(defn content-write-location-json
  [clg location path data]
  (if-let [store (get clg location)]
    (content-write-json store path data)
    (async-location-error location)))
