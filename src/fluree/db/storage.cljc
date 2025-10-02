(ns fluree.db.storage
  (:refer-clojure :exclude [read list exists?])
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json])
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

(defn unsanitize-path
  [path]
  (if (str/starts-with? path "//")
    (subs path 2)
    path))

(defn build-address
  ([location path]
   (let [path* (sanitize-path path)]
     (str/join ":" [location path*])))
  ([ns method path]
   (build-address ns nil method path))
  ([ns identifier method path]
   (build-address ns identifier method path nil))
  ([ns identifier method path auxiliary]
   (let [location (build-location ns identifier method auxiliary)]
     (build-address location path))))

(def fluree-namespace "fluree")

(def build-fluree-address
  (partial build-address fluree-namespace))

(defn split-address
  "Splits `address` into the fully qualified storage method and local path."
  [address]
  (let [i (str/index-of address "://")]
    (if i
      ;; For addresses with ://, the location is everything before ://
      ;; and the path is everything after ://
      [(subs address 0 i) (subs address (+ i 3))]
      ;; Fallback for addresses without ://
      (let [i (str/last-index-of address ":")]
        [(subs address 0 i) (subs address (inc i))]))))

(defn strip-extension
  [filename]
  (if-let [idx (str/last-index-of filename ".")]
    (subs filename 0 idx)
    filename))

(defn valid-identifier?
  [x]
  (and (str/includes? x "/")
       (not (str/includes? x ":"))))

(defn parse-location
  [location]
  (let [components (str/split location #":")
        address-ns (nth components 0)]
    (if (> (count components) 1)
      (let [id-or-method (nth components 1)]
        (if (valid-identifier? id-or-method)
          (let [identifier id-or-method
                method     (nth components 2)
                auxiliary  (-> components (subvec 3) not-empty)]
            (cond-> {:ns address-ns :identifier identifier, :method method}
              auxiliary (assoc :auxiliary auxiliary)))
          (let [method    id-or-method
                auxiliary (-> components (subvec 2) not-empty)]
            (cond-> {:ns address-ns, :method method}
              auxiliary (assoc :auxiliary auxiliary)))))
      {:ns address-ns})))

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

(defprotocol ContentArchive
  (-content-read-bytes [store address]
    "Reads the data associated with `address` within `store`.")

  (get-hash [store address]
    "Returns the hash of the data associated with `address` within `store`."))

(defprotocol ByteStore
  "ByteStore is used by consensus to replicate files across servers"
  (write-bytes [store path bytes] "Async writes bytes to path in store.")
  (read-bytes [store path] "Async read bytes from path in store.")
  (write-bytes-ext [store path bytes extension]
    "Async writes bytes to path with specified extension (e.g., 'cbor', 'bin').
    Path should NOT include extension - it will be appended.
    Returns address with extension.")
  (read-bytes-ext [store path extension]
    "Async reads bytes from path with specified extension.
    Returns raw bytes without text decoding.")
  (swap-bytes [store path f]
    "Atomically replace the contents at `path` using the supplied function `f`.
    `f` is called with the current contents at `path` (or `nil` if the path
    doesn't exist) and should return the new bytes to store."))

(defprotocol EraseableStore
  (delete [store address] "Remove value associated with `address` from the store."))

(defprotocol RecursiveListableStore
  (list-paths-recursive [store prefix]
    "Recursively returns all file paths that start with the given prefix. Excludes directories."))

(defn content-write-json
  [store path data]
  (go-try
    (let [json   (json/stringify data)
          bytes  (bytes/string->UTF8 json)
          result (<? (-content-write-bytes store path bytes))]
      (assoc result :json json))))

(defn content-read-json
  [store address]
  (go-try
    (let [bytes (<? (-content-read-bytes store address))]
      (json/parse bytes false))))

(defn read-json
  ([store address]
   (-read-json store address false))
  ([store address keywordize?]
   (-read-json store address keywordize?)))

(defn ->json-string
  [s]
  (or (not-empty s)
      "null"))

(defn <-json-string
  [json]
  (if (= json "null")
    ""
    json))

(defn swap-json
  [store path f]
  (let [f* (fn [bs]
             (-> bs
                 bytes/UTF8->string
                 ->json-string
                 (json/parse false)
                 f
                 json/stringify
                 <-json-string
                 bytes/string->UTF8))]
    (swap-bytes store path f*)))

(defrecord Catalog [])

(defn display-catalog
  [clg]
  (let [locations (-> clg (dissoc ::default ::read-only) keys vec)
        ro-ids    (-> clg ::read-only keys vec)]
    {:content-stores locations, :read-only-archives ro-ids}))

#?(:clj
   (defmethod print-method Catalog [^Catalog clg, ^Writer w]
     (.write w "#fluree/Catalog ")
     (binding [*out* w]
       (pr (display-catalog clg))))
   :cljs
   (extend-type Catalog
     IPrintWithWriter
     (-pr-writer [clg w _opts]
       (-write w "#fluree/Catalog ")
       (-write w (pr (display-catalog clg))))))

(defmethod pprint/simple-dispatch Catalog [^Catalog clg]
  (pr clg))

(defn section-entry
  [section]
  (let [loc (location section)]
    [loc section]))

(defn with-read-only-archive
  [read-only-section read-only-archive]
  (reduce (fn [sec address-identifier]
            (assoc sec address-identifier read-only-archive))
          read-only-section (identifiers read-only-archive)))

(defn read-only-archives->section
  [read-only-archives]
  (reduce with-read-only-archive {} read-only-archives))

(defn catalog
  ([content-stores]
   (catalog content-stores []))
  ([content-stores read-only-archives]
   (catalog content-stores read-only-archives []))
  ([content-stores read-only-archives byte-stores]
   (let [default-location (-> content-stores first location)]
     (catalog content-stores read-only-archives byte-stores default-location)))
  ([content-stores read-only-archives byte-stores default-location]
   (let [read-only-section (read-only-archives->section read-only-archives)]
     (-> (->Catalog)
         (into (map section-entry) (concat content-stores byte-stores))
         (assoc ::default default-location, ::read-only read-only-section)))))

(defn get-content-store
  [clg location]
  (let [location* (if (= location ::default)
                    (get clg ::default)
                    location)]
    (get clg location*)))

(defn get-read-only-archive
  [clg location]
  (when-let [identifier (get-identifier location)]
    (-> clg ::read-only (get identifier))))

(defn locate-address
  [clg address]
  (let [[location _content-path] (split-address address)]
    (or (get-content-store clg location)
        (get-read-only-archive clg location))))

(defn async-location-error
  [address]
  (let [ex (ex-info (str "Unrecognized storage location:" address)
                    {:status 500, :error :db/unexpected-error})]
    (doto (async/chan)
      (async/put! ex))))

(extend-type Catalog
  JsonArchive
  (-read-json [clg address keywordize?]
    (if-let [store (locate-address clg address)]
      (-read-json store address keywordize?)
      (async-location-error address)))

  ContentAddressedStore
  (-content-write-bytes [clg k v]
    (let [store (get-content-store clg ::default)]
      (-content-write-bytes store k v)))

  ContentArchive
  (-content-read-bytes [clg address]
    (if-let [store (locate-address clg address)]
      (-content-read-bytes store address)
      (async-location-error address)))

  (get-hash [clg address]
    (if-let [store (locate-address clg address)]
      (get-hash store address)
      (async-location-error address)))

  EraseableStore
  (delete [clg address]
    (if-let [store (locate-address clg address)]
      (delete store address)
      (async-location-error location))))

(defn content-write-catalog-json
  [clg location path data]
  (if-let [store (get-content-store clg location)]
    (content-write-json store path data)
    (async-location-error location)))

;; TODO: Segregate content stores and byte stores if some catalog components
;;       don't implement both protocols
(defn write-catalog-bytes
  [clg address data]
  (if-let [store (locate-address clg address)]
    (let [path (get-local-path address)]
      (write-bytes store path data))
    (async-location-error location)))
