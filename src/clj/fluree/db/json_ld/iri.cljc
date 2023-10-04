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

(defn decompose-iri
  [iri]
  (let [length (count iri)]
    (or (decompose-by-char iri \# length)
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

(defn append-name-codes
  [ns-sid nme]
  (into ns-sid
        (comp (partition-all 8)
              (map bytes/UTF8->long))
        (bytes/string->UTF8 nme)))

(defn iri->subid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  [db iri]
  (let [[ns nme] (decompose-iri iri)]
    (when-let [ns-code (namespace->code db ns)]
      (append-name-codes [ns-code] nme))))

(defn subid->iri
  "Converts a vector as would be returned by `iri->subid` back into a string iri."
  [db sid]
  (let [ns-code   (nth sid 0)
        ns        (code->namespace db ns-code)
        nme-codes (subvec sid 1)]
    (->> nme-codes
         (map bytes/long->UTF8)
         (map bytes/UTF8->string)
         (apply str ns))))
