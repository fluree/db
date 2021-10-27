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
                 {:name "size", :type :int}]}))


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
                                         :items "fluree.Flake"}}
                 {:name "his", :type [:null :string]}]}))


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
                 {:name "psot" :type "fluree.FdbChildNode"} ;; psot
                 {:name "post" :type "fluree.FdbChildNode"} ;; post
                 {:name "opst" :type "fluree.FdbChildNode"} ;; opst
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


(defn convert-ecount-integer-keys
  "Avro makes all map keys into strings. ecount usese integer keys,
  so they need to get converted back."
  [ecount]
  (->> ecount
       (map #(vector (Integer/parseInt (key %)) (val %)))
       (into {})))


(defn convert-stats-keywords
  "Avro makes all map keys into strings. Stats use keywords as keys."
  [stats]
  (reduce-kv
    #(assoc %1 (keyword %2) %3)
    {}
    stats))

(def ^:const bindings {'fluree/Flake #'flake/->Flake
                       'BigInteger   #'->BigInteger
                       'BigDecimal   #'->BigDecimal
                       'URI          #'->URI
                       'UUID         #'->UUID})


(defn serialize-block
  [block-data]
  (try
    (avro/binary-encoded FdbBlock-schema (select-keys block-data [:block :t :flakes]))
    (catch Exception e (log/error e "Error serializing block data: " (pr-str (select-keys block-data [:block :t :flakes])))
                       (throw (ex-info (str "Unexpected error, unable to serialize block data due to error: " (.getMessage e))
                                       {:status 500 :error :db/unexpected-error})))))

(defn decode-key
  "Given a key, figures out what type of data it is and decodes it with the appropriate schema."
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


(defrecord Serializer []
  serdeproto/StorageSerializer
  (-serialize-block [_ block]
    (serialize-block block))
  (-deserialize-block [_ block]
    (binding [avro/*avro-readers* bindings]
      (avro/decode FdbBlock-schema block)))
  (-serialize-db-root [_ db-root]
    ;; turn stats keys into proper strings
    (->> (assoc db-root :stats (reduce-kv #(assoc %1 (util/keyword->str %2) %3) {} (:stats db-root)))
         (avro/binary-encoded FdbRootDb-schema)))
  (-deserialize-db-root [_ db-root]
    ;; avro serializes all keys into strings, need to make ecount back into integer keys and stats into keyword keys
    (let [db-root* (avro/decode FdbRootDb-schema db-root)]
      (assoc db-root* :ecount (convert-ecount-integer-keys (:ecount db-root*))
                      :stats (convert-stats-keywords (:stats db-root*)))))
  (-serialize-branch [_ branch]
    (try
      (avro/binary-encoded FdbBranchNode-schema branch)
      (catch Exception e (log/error e (str "Error serializing index branch data: " (pr-str branch)))
                         (throw (ex-info "Unexpected error serializing index branch."
                                         {:status 500 :error :db/unexpected-error})))))
  (-deserialize-branch [_ branch]
    (binding [avro/*avro-readers* bindings]
      (avro/decode FdbBranchNode-schema branch)))
  (-serialize-leaf [_ leaf]
    (try
      (avro/binary-encoded FdbLeafNode-schema leaf)
      (catch Exception e (log/error e (str "Error serializing index leaf data: " (pr-str leaf)))
                         (throw (ex-info "Unexpected error serializing index branch."
                                         {:status 500 :error :db/unexpected-error})))))
  (-deserialize-leaf [_ leaf]
    (binding [avro/*avro-readers* bindings]
      (avro/decode FdbLeafNode-schema leaf)))
  (-serialize-garbage [_ garbage]
    (try
      (avro/binary-encoded FdbGarbage-schema garbage)
      (catch Exception e (log/error e (str "Error serializing index garbage data: " (pr-str garbage)))
                         (throw (ex-info "Unexpected error serializing index branch."
                                         {:status 500 :error :db/unexpected-error})))))
  (-deserialize-garbage [_ garbage]
    (avro/decode FdbGarbage-schema garbage))
  (-serialize-db-pointer [_ pointer]
    (try
      (avro/binary-encoded FdbDbPointer-schema pointer)
      (catch Exception e (log/error e (str "Error serializing db index pointer: " (pr-str pointer)))
                         (throw (ex-info "Unexpected error serializing index branch."
                                         {:status 500 :error :db/unexpected-error})))))
  (-deserialize-db-pointer [_ pointer]
    (avro/decode FdbDbPointer-schema pointer)))


(defn avro-serde
  "Returns an Avro serializer / deserializer."
  ^Serializer []
  (->Serializer))