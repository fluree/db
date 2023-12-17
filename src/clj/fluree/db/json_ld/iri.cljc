(ns fluree.db.json-ld.iri
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.util.bytes :as bytes]
            [clojure.string :as str]
            [clojure.set :refer [map-invert]])
  (:import (fluree.db SID)))

#?(:clj (set! *warn-on-reflection* true))

(def ^:const f-ns "https://ns.flur.ee/ledger#")
(def ^:const f-t-ns "https://ns.flur.ee/ledger/transaction#")
(def ^:const f-did-ns "did:fluree:")
(def ^:const f-commit-256-ns "fluree:commit:sha256:")
(def ^:const fdb-256-ns "fluree:db:sha256:")
(def ^:const f-mem-ns "fluree:memory://")
(def ^:const f-file-ns "fluree:file://")
(def ^:const f-ipfs-ns "fluree:ipfs://")
(def ^:const f-s3-ns "fluree:s3://")
(def ^:const f-ctx-ns "fluree:context:")

(def json-iri-keywords
  {"@type" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
   "@json" "http://www.w3.org/2001/XMLSchema#json"})

(defn normalize
  [iri]
  (or (get json-iri-keywords iri)
      iri))

(def default-namespaces
  "iri namespace mapping. 0 signifies relative iris. 1-100 are reserved; user
  supplied namespaces start at 101."
  {""                                            0
   "@"                                           1
   "http://www.w3.org/2001/XMLSchema#"           2
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#" 3
   "http://www.w3.org/2000/01/rdf-schema#"       4
   "http://www.w3.org/ns/shacl#"                 5
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
   f-ctx-ns                                      17
   "http://schema.org/"                          18
   "https://www.wikidata.org/wiki/"              19
   "http://xmlns.com/foaf/0.1/"                  20
   "http://www.w3.org/2008/05/skos#"             21
   "urn:uuid"                                    22
   "urn:isbn:"                                   23
   "urn:issn"                                    24
   "_:"                                          25})


(def default-namespace-codes
  (map-invert default-namespaces))

(def last-default-code 100)

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
    (let [length (count iri)]
      (or (decompose-by-char iri \@ length)
          (decompose-by-char iri \# length)
          (decompose-by-char iri \? length)
          (decompose-by-char iri \/ length)
          (decompose-by-char iri \: length)
          ["" iri]))))

(def name-code-xf
  (comp (partition-all 8)
        (map bytes/UTF8->long)))

(defn name->codes
  [nme]
  (->> nme
       bytes/string->UTF8
       (into [] name-code-xf)))

(defn append-name-codes
  [ns-sid nme]
  (into ns-sid
        name-code-xf
        (bytes/string->UTF8 nme)))

(defn codes->name
  [nme-codes]
  (->> nme-codes
       (mapcat bytes/long->UTF8)
       bytes/UTF8->string))

(defn ->sid
  [ns-code name-codes]
  (let [ns-int     (int ns-code)
        name-longs (long-array name-codes)]
    (SID. ns-int name-longs)))

(defn get-ns-code
  [^SID sid]
  (.getNamespaceCode sid))

(defn get-namespace
  ([sid]
   (get-namespace sid default-namespace-codes))
  ([sid namespace-codes]
   (let [ns-code (get-ns-code sid)]
     (get namespace-codes ns-code))))

(defn get-name-codes
  [^SID sid]
  (into [] (.getNameCodes sid)))

(defn get-name
  [sid]
  (->> sid get-name-codes codes->name))

(defn deserialize-sid
  [[ns-code nme]]
  (->sid ns-code (name->codes nme)))

(defn serialize-sid
  [^SID sid]
  ((juxt get-ns-code get-name) sid))

#?(:clj (defmethod print-method SID [^SID sid ^java.io.Writer w]
          (doto w
            (.write "#SID ")
            (.write (-> sid serialize-sid pr-str)))))

#?(:clj (defmethod print-dup SID
          [^SID sid ^java.io.Writer w]
          (let [ns-code    (get-ns-code sid)
                name-codes (get-name-codes sid)]
            (.write w (str "#=" `(->sid ~ns-code ~name-codes))))))

(defn sid?
  [x]
  (instance? SID x))

(def ^:const min-sid
  (->sid util/min-integer [0x0000]))

(def ^:const max-sid
  (->sid util/max-integer [0xFFFF]))

(defn iri->sid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  ([iri]
   (iri->sid iri default-namespaces))
  ([iri namespaces]
   (let [[ns nme] (-> iri normalize decompose)]
     (when-let [ns-code (get namespaces ns)]
       (let [name-codes (name->codes nme)]
         (->sid ns-code name-codes))))))

(defn next-namespace-code
  [namespaces]
  (->> namespaces
       vals
       (apply max last-default-code)
       inc))

(defprotocol SIDGenerator
  (generate-sid [g iri])
  (get-namespaces [g]))

(defn sid-generator!
  [initial-namespaces]
  (let [namespaces (volatile! initial-namespaces)]
    (reify SIDGenerator
      (generate-sid [_ iri]
        (let [[ns nme] (decompose iri)
              ns-code  (-> namespaces
                           (vswap! (fn [ns-map]
                                     (if (contains? ns-map ns)
                                       ns-map
                                       (let [new-ns-code (next-namespace-code ns-map)]
                                         (assoc ns-map ns new-ns-code)))))
                           (get ns))
              name-codes (name->codes nme)]
          (->sid ns-code name-codes)))

      (get-namespaces [_]
        @namespaces))))

(defn sid->iri
  "Converts an sid back into a string iri."
  ([sid]
   (sid->iri sid default-namespace-codes))
  ([^SID sid namespace-codes]
   (str (get-namespace sid namespace-codes)
        (get-name sid))))

(defn fluree-iri
  [nme]
  (str f-ns nme))

(defn tx-iri
  [t]
  (str f-t-ns (- t)))

(defn t->sid
  [t]
  (-> t tx-iri iri->sid))
