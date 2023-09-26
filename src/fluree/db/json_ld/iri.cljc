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

(defn iri-namespace-code
  [db iri-ns]
  (or (get const/namespace->code iri-ns)
      (-> db :namespaces (get iri-ns))))

(defn iri-namespace
  [db iri-ns-code]
  (or (get const/code->namespace iri-ns-code)
      (-> db :namespace-codes (get iri-ns-code))))

(defn split-iri-name
  "Splits the 'name' part of an iri string into 8-byte chunks."
  [iri-name]
  (re-seq #".{1,8}" iri-name))

(defn iri->subid
  "Converts a string iri into a vector of long integer codes. The first code
  corresponds to the iri's namespace, and the remaining codes correspond to the
  iri's name split into 8-byte chunks"
  [db iri]
  (let [[ns nme] (decompose-iri iri)]
    (when-let [ns-code (iri-namespace-code db ns)]
      (into [ns-code]
            (map bytes/long-encode-str)
            (split-iri-name nme)))))

(defn subid->iri
  "Converts a vector as would be returned by `iri->subid` back into a string iri."
  [db subid]
  (let [[ns-code & nme-codes] subid
        ns                    (iri-namespace db ns-code)]
    (->> nme-codes
         (map bytes/str-decode-long)
         (apply str ns))))
