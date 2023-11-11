(ns fluree.db.json-ld.iri
  (:require [fluree.db.constants :as const]
            [fluree.db.util.bytes :as bytes]
            [clojure.string :as str]))

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
  (or (get const/namespace->code iri-ns)
      (-> db :namespaces (get iri-ns))))

(defn code->namespace
  [db ns-code]
  (or (get const/code->namespace ns-code)
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
  (->> sid
       get-name-codes
       codes->name))

(defn iri->sid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  [db iri]
  (let [[ns nme] (decompose iri)]
    (when-let [ns-code (namespace->code db ns)]
      (append-name-codes [ns-code] nme))))

(defn sid->iri
  "Converts a vector as would be returned by `iri->subid` back into a string iri."
  [db sid]
  (str (get-namespace db sid)
       (get-name sid)))
