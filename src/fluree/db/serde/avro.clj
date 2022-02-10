(ns fluree.db.serde.avro
  (:require [abracad.avro :as avro]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [clojure.string :as str])
  (:import (java.net URI)
           (java.util UUID)))

(set! *warn-on-reflection* true)

;;; -----------------------------------------
;;;
;;; Supporting avro records
;;;
;;; -----------------------------------------


;; bigint support
(def avro-bigint
  (avro/parse-schema
    {:type   :record
     :name   "BigInteger"
     :fields [{:name "val", :type :string}]}))

(defn ->BigInteger
  [val]
  (bigint val))

(extend-type java.math.BigInteger
  avro/AvroSerializable
  (schema-name [_] "BigInteger")
  (field-get [this field] (str this))
  (field-list [_] #{:val}))

(extend-type clojure.lang.BigInt
  avro/AvroSerializable
  (schema-name [_] "BigInteger")
  (field-get [this field] (str this))
  (field-list [_] #{:val}))


;; bigdec support
(def avro-bigdec
  (avro/parse-schema
    {:type   :record
     :name   "BigDecimal"
     :fields [{:name "val", :type :string}]}))

(defn ->BigDecimal
  [val]
  (bigdec val))

(extend-type java.math.BigDecimal
  avro/AvroSerializable
  (schema-name [_] "BigDecimal")
  (field-get [this field] (str this))
  (field-list [_] #{:val}))


;; uri support
(def avro-uri
  (avro/parse-schema
    {:type   :record
     :name   "URI"
     :fields [{:name "val", :type :string}]}))

(defn ->URI
  [val]
  (URI. val))

(extend-type URI
  avro/AvroSerializable
  (schema-name [_] "URI")
  (field-get [this field] (str this))
  (field-list [_] #{:val}))

;; uuid support
(def avro-uuid
  (avro/parse-schema
    {:type   :record
     :name   "UUID"
     :fields [{:name "val", :type :string}]}))

(defn ->UUID
  [val]
  (UUID/fromString val))

(extend-type UUID
  avro/AvroSerializable
  (schema-name [_] "UUID")
  (field-get [this field] (str this))
  (field-list [_] #{:val}))


(def avro-Flake
  (avro/parse-schema
    avro-bigint avro-bigdec avro-uri avro-uuid
    {:type           :record
     :name           'fluree.Flake
     :abracad.reader "vector"
     :fields         [{:name "s", :type :long}
                      {:name "p", :type :long}
                      {:name "o", :type [:long :int :string :boolean :float :double "BigInteger" "BigDecimal" "URI" "UUID"]}
                      {:name "t", :type :long}
                      {:name "op", :type :boolean}
                      {:name "m", :type [:string :null]}]}))

(def avro-FdbChildNode
  (avro/parse-schema
    avro-Flake
    {:type      :record
     :name      "FdbChildNode"
     :namespace "fluree"
     :fields    [{:name "id", :type :string}
                 {:name "leaf" :type :boolean}              ;; is this a leaf (data) node?
                 {:name "first", :type "fluree.Flake"}
                 {:name "rhs", :type [:null "fluree.Flake"]}
                 {:name "size", :type :long}]}))


;;; -----------------------------------------
;;;
;;; Main avro records
;;;
;;; -----------------------------------------


(def FdbBranchNode-schema
  (avro/parse-schema
    avro-FdbChildNode
    {:type      :record
     :name      "FdbBranchNode"
     :namespace "fluree"
     :fields    [{:name "children", :type {:type  :array
                                           :items "fluree.FdbChildNode"}}
                 {:name "rhs", :type [:null "fluree.Flake"]}]}))

(def FdbLeafNode-schema
  (avro/parse-schema
    avro-Flake
    {:type      :record
     :name      "FdbLeafNode"
     :namespace "fluree"
     :fields    [{:name "flakes", :type {:type  :array
                                         :items "fluree.Flake"}}]}))


(def FdbRootDb-schema
  (avro/parse-schema
    avro-FdbChildNode
    {:type      :record
     :name      "FdbRoot"
     :namespace "fluree"
     :fields    [{:name "dbid", :type :string}
                 {:name "block", :type :long}
                 {:name "t", :type :long}
                 {:name "ecount", :type {:type "map", :values :long}}
                 {:name "stats", :type {:type "map", :values :long}}
                 {:name "fork", :type [:null :string]}
                 {:name "forkBlock", :type [:null :long]}
                 {:name "spot", :type "fluree.FdbChildNode"} ;; spot
                 {:name "psot", :type "fluree.FdbChildNode"} ;; psot
                 {:name "post", :type "fluree.FdbChildNode"} ;; post
                 {:name "opst", :type "fluree.FdbChildNode"} ;; opst
                 {:name "tspo", :type "fluree.FdbChildNode"} ;; tspo
                 {:name "timestamp" :type [:null :long]}
                 {:name "prevIndex" :type [:null :long]}]}))


(def FdbGarbage-schema
  (avro/parse-schema
    {:type      :record
     :name      "FdbGarbage"
     :namespace "fluree"
     :fields    [{:name "dbid", :type :string}
                 {:name "block", :type :long}
                 {:name "garbage", :type {:type  :array
                                          :items :string}}]}))

;; points to the last db root block for the DB
;; can also hold status codes (i.e. ready, forking, etc)
;; and also status messages
(def FdbDbPointer-schema
  (avro/parse-schema
    {:type      :record
     :name      "FdbDbPointer"
     :namespace "fluree"
     :fields    [{:name "dbid", :type :string}
                 {:name "block", :type :long}
                 {:name "status", :type [:null :string]}    ;; status code
                 {:name "message", :type [:null :string]}   ;; status message
                 {:name "fork", :type [:null :string]}      ;; db-ident of db this was forked from
                 {:name "forkBlock", :type [:null :long]}]}))   ;; if forked, what block point is the fork at



(def FdbBlock-schema
  (avro/parse-schema
    avro-Flake
    {:type      :record
     :name      "FdbBlock"
     :namespace "fluree"
     :fields    [{:name "block", :type :long}
                 {:name "t", :type :long}
                 {:name "flakes", :type {:type  :array
                                         :items "fluree.Flake"}}]}))



;;; -----------------------------------------
;;;
;;; Avro serializer
;;;
;;; -----------------------------------------


(defn convert-keys
  [m conv]
  (reduce-kv (fn [m* k v]
               (assoc m* (conv k) v))
             {} m))

(defn convert-ecount-integer-keys
  "Avro makes all map keys into strings. ecount usese integer keys,
  so they need to get converted back."
  [ecount]
  (convert-keys ecount (fn [s]
                         (Integer/parseInt s))))

(defn convert-stats-keywords
  "Avro makes all map keys into strings. Stats use keywords as keys."
  [stats]
  (convert-keys stats keyword))

(defn convert-stats-strings
  [stats]
  (convert-keys stats util/keyword->str))

(def ^:const bindings {'fluree/Flake #'flake/->Flake
                       'BigInteger   #'->BigInteger
                       'BigDecimal   #'->BigDecimal
                       'URI          #'->URI
                       'UUID         #'->UUID})


(defn decode-key
  "Given a key, figures out what type of data it is and decodes it with the
  appropriate schema."
  [k data]
  (log/warn "AVRO decode key:" k)
  (cond
    (str/includes? k "_block_")
    (avro/decode FdbBlock-schema data)

    (str/includes? k "_root_")
    (avro/decode FdbRootDb-schema data)

    (str/ends-with? k "-b")
    (avro/decode FdbBranchNode-schema data)

    (str/ends-with? k "-l")
    (avro/decode FdbLeafNode-schema data)

    (str/ends-with? k "-l-his")
    (avro/decode FdbLeafNode-schema data)))

(defn serialize-block
  [block-data]
  (try
    (avro/binary-encoded FdbBlock-schema (select-keys block-data [:block :t :flakes]))
    (catch Exception e (log/error e "Error serializing block data: " (pr-str (select-keys block-data [:block :t :flakes])))
           (throw (ex-info (str "Unexpected error, unable to serialize block data due to error: " (.getMessage e))
                           {:status 500 :error :db/unexpected-error})))))

(defn deserialize-block
  [block]
  (binding [avro/*avro-readers* bindings]
    (avro/decode FdbBlock-schema block)))

(defn serialize-db-root
  [db-root]
  ;; turn stats keys into proper strings
  (->> (update db-root :stats convert-stats-strings)
       (avro/binary-encoded FdbRootDb-schema)))

(defn deserialize-db-root
  [db-root]
  ;; avro serializes all keys into strings, need to make ecount back into
  ;; integer keys and stats into keyword keys
  (let [db-root* (avro/decode FdbRootDb-schema db-root)]
    (-> db-root*
        (update :ecount convert-ecount-integer-keys)
        (update :stats convert-stats-keywords))))

(defn serialize-branch
  [branch-data]
  (try
    (avro/binary-encoded FdbBranchNode-schema branch-data)
    (catch Exception e
      (log/error e "Error serializing index branch data:"
                 (pr-str branch-data))
      (throw (ex-info "Unexpected error serializing index branch."
                      {:status 500 :error :db/unexpected-error})))))

(defn deserialize-branch
  [branch]
  (binding [avro/*avro-readers* bindings]
    (avro/decode FdbBranchNode-schema branch)))

(defn serialize-leaf
  [leaf-data]
  (try
    (avro/binary-encoded FdbLeafNode-schema leaf-data)
    (catch Exception e
      (log/error e "Error serializing index leaf data:"
                 (pr-str leaf-data))
      (throw (ex-info "Unexpected error serializing index leaf."
                      {:status 500 :error :db/unexpected-error})))))

(defn deserialize-leaf
  [leaf]
  (binding [avro/*avro-readers* bindings]
    (avro/decode FdbLeafNode-schema leaf)))

(defn serialize-garbage
  [garbage-data]
  (try
    (avro/binary-encoded FdbGarbage-schema garbage-data)
    (catch Exception e
      (log/error e "Error serializing index garbage data:"
                 (pr-str garbage-data))
      (throw (ex-info "Unexpected error serializing index branch."
                      {:status 500 :error :db/unexpected-error})))))

(defn deserialize-garbage
  [garbage]
  (avro/decode FdbGarbage-schema garbage))

(defn serialize-db-pointer
  [pointer-data]
  (try
    (avro/binary-encoded FdbDbPointer-schema pointer-data)
    (catch Exception e
      (log/error e "Error serializing db index pointer:"
                 (pr-str pointer-data))
      (throw (ex-info "Unexpected error serializing index branch."
                      {:status 500 :error :db/unexpected-error})))))

(defn deserialize-db-pointer
  [pointer]
  (avro/decode FdbDbPointer-schema pointer))

(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-block [_ block]
    (serialize-block block))
  (-deserialize-block [_ block]
    (deserialize-block block))
  (-serialize-db-root [_ db-root]
    (serialize-db-root db-root))
  (-deserialize-db-root [_ db-root]
    (deserialize-db-root db-root))
  (-serialize-branch [_ branch]
    (serialize-branch branch))
  (-deserialize-branch [_ branch]
    (deserialize-branch branch))
  (-serialize-leaf [_ leaf-data]
    (serialize-leaf leaf-data))
  (-deserialize-leaf [_ leaf]
    (deserialize-leaf leaf))
  (-serialize-garbage [_ garbage]
    (serialize-garbage garbage))
  (-deserialize-garbage [_ garbage]
    (deserialize-garbage garbage))
  (-serialize-db-pointer [_ pointer]
    (serialize-db-pointer pointer))
  (-deserialize-db-pointer [_ pointer]
    (deserialize-db-pointer pointer)))


(defn avro-serde
  "Returns an Avro serializer / deserializer."
  ^Serializer []
  (->Serializer))
