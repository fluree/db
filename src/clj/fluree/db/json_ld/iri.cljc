(ns fluree.db.json-ld.iri
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [fluree.db.util.bytes :as bytes]
            [clojure.string :as str]
            [clojure.set :refer [map-invert]]
            #?(:cljs [fluree.db.sid :refer [SID]]))
  #?(:clj (:import (fluree.db SID))))

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

(def type-iri "@type")

(def json-iri-keywords
  {type-iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
   "@json"  "http://www.w3.org/2001/XMLSchema#json"})

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

(defn serialize-sid
  [sid]
  ((juxt get-ns-code get-name) sid))

#?(:clj (defmethod print-method SID [^SID sid ^java.io.Writer w]
          (doto w
            (.write "#SID ")
            (.write (-> sid serialize-sid pr-str)))))

#?(:clj (defmethod print-dup SID
          [^SID sid ^java.io.Writer w]
          (let [ns-code (get-ns-code sid)
                nme     (get-name sid)]
            (.write w (str "#=" `(->sid ~ns-code ~nme))))))

(defn sid?
  [x]
  (instance? SID x))

(def ^:const min-sid
  (->sid 0 ""))

(def ^:const max-sid
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
                           (get ns))]
          (->sid ns-code nme)))

      (get-namespaces [_]
        @namespaces))))

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

(defn fluree-iri
  [nme]
  (str f-ns nme))
