(ns fluree.db.json-ld.iri
  (:require [fluree.db.util.bytes :as bytes]
            [clojure.string :as str]
            [clojure.set :refer [map-invert]]))

(def namespaces
  "iri namespace mapping. 0 signifies relative iris. 1-100 are reserved; user
  supplied namespaces start at 101."
  {"@"                                           1
   "https://ns.flur.ee/ledger#"                  2
   "http://www.w3.org/2001/XMLSchema#"           3
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#" 4
   "http://www.w3.org/2000/01/rdf-schema#"       5
   "http://www.w3.org/ns/shacl#"                 6
   "http://www.w3.org/2002/07/owl#"              7
   "http://www.w3.org/2008/05/skos#"             8
   "http://xmlns.com/foaf/0.1/"                  9
   "http://schema.org/"                          10
   "https://www.wikidata.org/wiki/"              11
   "urn:uuid"                                    12
   "urn:isbn:"                                   13
   "urn:issn"                                    14
   "_:"                                          15})

(def namespace-codes
  (map-invert namespaces))

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
  (let [length (count iri)]
    (or (decompose-by-char iri \@ length)
        (decompose-by-char iri \# length)
        (decompose-by-char iri \? length)
        (decompose-by-char iri \/ length)
        (decompose-by-char iri \: length)
        [nil iri])))

(defn namespace->code
  [db iri-ns]
  (or (get namespaces iri-ns)
      (-> db :namespaces (get iri-ns))))

(defn code->namespace
  [db ns-code]
  (or (get namespace-codes ns-code)
      (-> db :namespaces-codes (get ns-code))))

(def name-code-xf
  (comp (partition-all 8)
        (map bytes/UTF8->long)))

(defn name->codes
  [nme]
  (into []
        name-code-xf
        (bytes/string->UTF8 nme)))

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

(defn get-ns-code
  [sid]
  (nth sid 0))

(defn get-name-codes
  [sid]
  (subvec sid 1))

(defn get-namespace
  [db sid]
  (-> sid
      get-ns-code
      (as-> ns-code (code->namespace db ns-code))))

(defn get-name
  [sid]
  (->> sid get-name-codes codes->name))

(defn iri->sid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  ([iri]
   (let [[ns nme] (decompose iri)]
     (when-let [ns-code (get namespaces ns)]
       (append-name-codes [ns-code] nme))))
  ([db iri]
   (let [[ns nme] (decompose iri)]
     (when-let [ns-code (namespace->code db ns)]
       (append-name-codes [ns-code] nme)))))

(defn sid?
  [x]
  (vector? x))

(defn sid->iri
  "Converts a vector as would be returned by `iri->subid` back into a string iri."
  ([sid]
   (-> sid
       get-ns-code
       (as-> ns (get namespaces ns))
       (str (get-name sid))))
  ([db sid]
   (str (get-namespace db sid)
        (get-name sid))))
