(ns fluree.db.json-ld.iri
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.bytes :as bytes]
            [clojure.string :as str]
            [clojure.set :refer [map-invert]]
            [nano-id.core :refer [nano-id]]
            #?(:cljs [fluree.db.sid :refer [SID]]))
  #?(:clj (:import (fluree.db SID))))

#?(:clj (set! *warn-on-reflection* true))


(def ^:const f-ns "https://ns.flur.ee/ledger#")
(def ^:const f-t-ns "https://ns.flur.ee/ledger/transaction#")
(def ^:const f-idx-ns "https://ns.flur.ee/index#")
(def ^:const f-did-ns "did:fluree:")
(def ^:const f-commit-256-ns "fluree:commit:sha256:")
(def ^:const fdb-256-ns "fluree:db:sha256:")
(def ^:const f-mem-ns "fluree:memory://")
(def ^:const f-file-ns "fluree:file://")
(def ^:const f-ipfs-ns "fluree:ipfs://")
(def ^:const f-s3-ns "fluree:s3://")

(def ^:const shacl-ns "http://www.w3.org/ns/shacl#")

(def ^:const type-iri "@type")
(def ^:const json-iri "@json")
(def ^:const rdf:type-iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
(def ^:const rdf:JSON-iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON")

(defn normalize
  [iri]
  (cond
    (= iri type-iri)
    rdf:type-iri

    (= iri json-iri)
    rdf:JSON-iri

    :else
    iri))

(def default-namespaces
  "iri namespace mapping. 0 signifies relative iris. 1-100 are reserved; user
  supplied namespaces start at 101."
  {""                                            0
   "@"                                           1
   "http://www.w3.org/2001/XMLSchema#"           2
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#" 3
   "http://www.w3.org/2000/01/rdf-schema#"       4
   shacl-ns                                      5
   "http://www.w3.org/2002/07/owl#"              6
   "https://www.w3.org/2018/credentials#"        7
   f-ns                                          8
   f-t-ns                                        9
   fdb-256-ns                                    10
   f-did-ns                                      11
   f-commit-256-ns                               12
   f-mem-ns                                      13
   f-file-ns                                     14
   f-ipfs-ns                                     15
   f-s3-ns                                       16
   "http://schema.org/"                          17
   "https://www.wikidata.org/wiki/"              18
   "http://xmlns.com/foaf/0.1/"                  19
   "http://www.w3.org/2008/05/skos#"             20
   "urn:uuid"                                    21
   "urn:isbn:"                                   22
   "urn:issn:"                                   23
   "_:"                                          24
   f-idx-ns                                      25})


(def default-namespace-codes
  (map-invert default-namespaces))

(def last-default-code 100)

(def commit-namespaces
  #{f-commit-256-ns})

(def commit-namespace-codes
  (into #{}
        (map default-namespaces)
        commit-namespaces))

(defn decompose-by-char
  [iri c limit]
  (when-let [char-idx (some-> iri
                              (str/last-index-of c)
                              inc)]
    (when (< char-idx limit)
      (let [ns  (subs iri 0 char-idx)
            nme (subs iri char-idx)]
        [ns nme]))))

(defn decompose
  [iri]
  (when iri
    (let [iri*   (normalize iri)
          length (count iri*)]
      (or (decompose-by-char iri* \@ length)
          (decompose-by-char iri* \# length)
          (decompose-by-char iri* \? length)
          (decompose-by-char iri* \/ length)
          (decompose-by-char iri* \: length)
          ["" iri*]))))

(def name-code-xf
  (comp (partition-all 8)
        (map bytes/UTF8->long)))

#?(:clj (defn name->codes
          [nme]
          (->> nme
               bytes/string->UTF8
               (into [] name-code-xf))))

#?(:clj (defn codes->name
          [nme-codes]
          (->> nme-codes
               (mapcat bytes/long->UTF8)
               bytes/UTF8->string)))

(defn ->sid
  [ns-code nme]
  (let [ns-int (int ns-code)
        nme*   #?(:clj (-> nme name->codes long-array)
                  :cljs nme)]
    (SID. ns-int nme*)))

(defn get-ns-code
  [^SID sid]
  #?(:clj (.getNamespaceCode sid)
     :cljs (:namespace-code sid)))

(defn get-namespace
  ([sid]
   (get-namespace sid default-namespace-codes))
  ([sid namespace-codes]
   (let [ns-code (get-ns-code sid)]
     (get namespace-codes ns-code))))

(defn get-name
  [^SID sid]
  #?(:clj (->> sid .getNameCodes codes->name)
     :cljs (:name sid)))

(defn deserialize-sid
  [[ns-code nme]]
  (->sid ns-code nme))

(defn measure-sid
  "Returns the size of an SID."
  [sid]
  (+ 12 ; 12 bytes for object header
     4  ; 4 bytes for namespace code
     (* 2 (count (get-name sid)))))

(def serialize-sid
  (juxt get-ns-code get-name))

(defn serialized-sid?
  [x]
  (and (vector? x)
       (= (count x) 2)
       (number? (nth x 0))
       (string? (nth x 1))))

#?(:clj (defmethod print-method SID [^SID sid ^java.io.Writer w]
          (doto w
            (.write "#fluree/SID ")
            (.write (-> sid serialize-sid pr-str)))))

#?(:clj (defmethod print-dup SID
          [^SID sid ^java.io.Writer w]
          (let [ns-code (get-ns-code sid)
                nme     (get-name sid)]
            (.write w (str "#=" `(->sid ~ns-code ~nme))))))

(defn sid?
  [x]
  (instance? SID x))

(defn blank-node-sid?
  [x]
  (and (sid? x)
       (= (get-namespace x) "_:")))

(def min-sid
  (->sid 0 ""))

(def max-sid
  (->sid util/max-integer ""))

(defn iri->sid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  ([iri]
   (iri->sid iri default-namespaces))
  ([iri namespaces]
   (let [[ns nme] (decompose iri)]
     (when-let [ns-code (get namespaces ns)]
       (->sid ns-code nme)))))

(defn get-max-namespace-code
  [ns-codes]
  (->> ns-codes keys (apply max last-default-code)))

(defn next-namespace-code
  [ns-codes]
  (-> ns-codes get-max-namespace-code inc))

(def type-sid
  (iri->sid "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))

(defn sid->iri
  "Converts an sid back into a string iri."
  ([sid]
   (sid->iri sid default-namespace-codes))
  ([sid namespace-codes]
   (if (= type-sid sid)
     type-iri
     (str (get-namespace sid namespace-codes)
          (get-name sid)))))

(defprotocol IRICodec
  (encode-iri [codec iri])
  (decode-sid [codec sid]))

(defn namespace-codec
  [namespace-codes]
  (let [namespaces (map-invert namespace-codes)]
    (reify
      IRICodec
      (encode-iri [_ iri]
        (iri->sid iri namespaces))
      (decode-sid [_ sid]
        (sid->iri sid namespace-codes)))))

(defn fluree-iri
  [nme]
  (str f-ns nme))

(defn fluree-idx-iri
  [nme]
  (str f-idx-ns nme))

(def blank-node-prefix
  "_:fdb")

(defn blank-node-id?
  [s]
  (str/starts-with? s blank-node-prefix))

(defn blank-node?
  [node]
  (when-let [id (util/get-id node)]
    (if (string? id)
      (blank-node-id? id)
      (throw (ex-info (str "JSON-LD node improperly formed, @id values must be strings, but found: " id
                           " in node: " node ".")
                      {:status 500
                       :error  :db/unexpected-error})))))

(defn new-blank-node-id
  []
  (let [now (util/current-time-millis)
        suf (nano-id 8)]
    (str/join "-" [blank-node-prefix now suf])))
