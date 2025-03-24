(ns fluree.db.json-ld.iri
  (:require [clojure.set :refer [map-invert]]
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            [nano-id.core :refer [nano-id]]))

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

(declare compare-SIDs sid-equiv?)

;; TODO - verify sort order is same!!
;;
(deftype SID #?(:clj [^int namespace-code name]
                :cljs [^number namespace-code name])
  #?@(:clj  [Object
             (equals [this sid]
                     (sid-equiv? this sid))
             (hashCode [_]
                       (clojure.lang.Util/hashCombine namespace-code (hash name)))

             clojure.lang.IHashEq
             (hasheq [_]
                     (clojure.lang.Util/hashCombine namespace-code (hash name)))

             java.lang.Comparable
             (compareTo [this other] (compare-SIDs this other))]

      :cljs [IHash
             (-hash [_]
                    (hash-combine namespace-code (hash name)))

             IEquiv
             (-equiv [this sid] (sid-equiv? this sid))

             IComparable
             (-compare [this other] (compare-SIDs this other))

             IPrintWithWriter
             (-pr-writer [^SID sid writer opts]
                         (pr-sequential-writer writer pr-writer
                                               "#fluree/SID [" " " "]"
                                               opts [(.-namespace-code sid) (.-name sid)]))]))

(defn sid-equiv?
  [^SID sid ^SID other]
  (and (instance? SID other)
       (= (.-namespace-code sid) (.-namespace-code other))
       (= (.-name sid) (.-name other))))

(defn ->sid ^SID
  [ns-code nme]
  (->SID #?(:clj (int ns-code) :cljs ns-code) nme))

(defn get-ns-code
  [^SID sid]
  (.-namespace-code sid))

(defn get-name
  [^SID sid]
  (.-name sid))

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

#?(:clj (defmethod print-method SID [^SID sid ^java.io.Writer w]
          (doto w
            (.write "#fluree/SID ")
            (.write (-> sid serialize-sid pr-str)))))

; TODO - verify we don't need this
;#?(:clj (defmethod print-dup SID
;          [^SID sid ^java.io.Writer w]
;          (let [ns-code (get-ns-code sid)
;                nme     (get-name sid)]
;            (.write w (str "#=" `(->sid ~ns-code ~nme))))))

(defn compare-SIDs
  [sid1 sid2]
  (when-not (instance? SID sid2)
    (throw (ex-info "Can't compare an SID to another type"
                    {:status 500 :error :db/unexpected-error})))
  (let [ns-cmp (compare (get-ns-code sid1) (get-ns-code sid2))]
    (if-not (zero? ns-cmp)
      ns-cmp
      (compare (get-name sid1) (get-name sid2)))))

(defn sid?
  [x]
  (instance? SID x))

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

(defn get-namespace
  ([sid]
   (get-namespace sid default-namespace-codes))
  ([sid namespace-codes]
   (let [ns-code (get-ns-code sid)]
     (get namespace-codes ns-code))))

(defn serialized-sid?
  [x]
  (and (vector? x)
       (= (count x) 2)
       (number? (nth x 0))
       (string? (nth x 1))))

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
