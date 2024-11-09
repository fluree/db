(ns fluree.db.connection.config
  (:require [clojure.string :as str]
            [fluree.db.connection.vocab :as conn-vocab]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util :refer [get-id get-first get-first-value get-values]]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn type?
  [node kind]
  (-> node (get-first :type) (= kind)))

(defn connection?
  [node]
  (type? node conn-vocab/connection-type))

(defn system?
  [node]
  (type? node conn-vocab/system-type))

(defn publisher?
  [node]
  (type? node conn-vocab/publisher-type))

(defn storage-nameservice?
  [node]
  (and (publisher? node)
       (contains? node conn-vocab/storage)))

(defn ipns-nameservice?
  [node]
  (and (publisher? node)
       (contains? node conn-vocab/ipfs-endpoint)
       (contains? node conn-vocab/ipns-key)))

(defn storage?
  [node]
  (type? node conn-vocab/storage-type))

(defn memory-storage?
  [node]
  (and (storage? node)
       (-> node
           (dissoc :idx :id :type conn-vocab/address-identifier)
           empty?)))

(defn file-storage?
  [node]
  (and (storage? node)
       (contains? node conn-vocab/file-path)))

(defn s3-storage?
  [node]
  (and (storage? node)
       (contains? node conn-vocab/s3-bucket)))

(defn ipfs-storage?
  [node]
  (and (storage? node)
       (contains? node conn-vocab/ipfs-endpoint)))

(defn derive-node-id
  [node]
  (let [id (get-id node)]
    (cond
      (connection? node)           (derive id :fluree.db/connection)
      (system? node)               (derive id :fluree.db/remote-system)
      (memory-storage? node)       (derive id :fluree.db.storage/memory)
      (file-storage? node)         (derive id :fluree.db.storage/file)
      (s3-storage? node)           (derive id :fluree.db.storage/s3)
      (ipfs-storage? node)         (derive id :fluree.db.storage/ipfs)
      (ipns-nameservice? node)     (derive id :fluree.db.nameservice/ipns)
      (storage-nameservice? node)  (derive id :fluree.db.nameservice/storage))
    node))

(def component-exclusions
  #{conn-vocab/identity})

(defn exclude-component?
  [k]
  (contains? component-exclusions k))

(defn subject-node?
  [x]
  (and (map? x)
       (not (contains? x :value))))

(defn blank-node?
  [x]
  (and (subject-node? x)
       (not (contains? x :id))))

(defn ref-node?
  [x]
  (and (subject-node? x)
       (not (blank-node? x))
       (-> x
           (dissoc :idx)
           count
           (= 1))))

(defn split-subject-node
  [node]
  (let [node* (cond-> node
                (blank-node? node) (assoc :id (iri/new-blank-node-id))
                true               (dissoc :idx))]
    (if (ref-node? node*)
      [node*]
      (let [ref-node (select-keys node* [:id])]
        [ref-node node*]))))

(defn flatten-sequence
  [coll]
  (loop [[child & r]   coll
         child-nodes   []
         flat-sequence []]
    (if child
      (if (subject-node? child)
        (let [[ref-node child-node] (split-subject-node child)
              child-nodes*          (if child-node
                                      (conj child-nodes child-node)
                                      child-nodes)]
          (recur r child-nodes* (conj flat-sequence ref-node)))
        (recur r child-nodes (conj flat-sequence child)))
      [flat-sequence child-nodes])))

(defn flatten-node
  [node]
  (loop [[[k v] & r] (dissoc node :idx)
         children    []
         flat-node   {}]
    (if k
      (if (exclude-component? k)
        (recur r children (assoc flat-node k v))
        (if (sequential? v)
          (let [[flat-sequence child-nodes] (flatten-sequence v)]
            (recur r
                   (into children child-nodes)
                   (assoc flat-node k flat-sequence)))
          (if (and (subject-node? v)
                   (not (ref-node? v)))
            (let [[ref-node child-node] (split-subject-node v)]
              (recur r (conj children child-node) (assoc flat-node k ref-node)))
            (recur r children (assoc flat-node k v)))))
      [flat-node children])))

(defn flatten-nodes
  [nodes]
  (loop [remaining nodes
         flattened []]
    (if-let [node (peek remaining)]
      (let [[flat-node children] (flatten-node node)
            remaining*           (-> remaining
                                     pop
                                     (into children))
            flattened*           (conj flattened flat-node)]
        (recur remaining* flattened*))
      flattened)))

(defn encode-illegal-char
  [c]
  (case c
    "&" "<am>"
    "@" "<at>"
    "]" "<cb>"
    ")" "<cp>"
    ":" "<cl>"
    "," "<cm>"
    "$" "<dl>"
    "." "<do>"
    "%" "<pe>"
    "#" "<po>"
    "(" "<op>"
    "[" "<ob>"
    ";" "<sc>"
    "/" "<sl>"))

(defn kw-encode
  [s]
  (str/replace s #"[:#@$&%.,;~/\(\)\[\]]" encode-illegal-char))

(defn iri->kw
  [iri]
  (let [iri* (or iri (iri/new-blank-node-id))]
    (->> (iri/decompose iri*)
         (map kw-encode)
         (apply keyword))))

(defn keywordize-node-id
  [node]
  (if (subject-node? node)
    (update node :id iri->kw)
    node))

(defn keywordize-child-ids
  [node]
  (into {}
        (map (fn [[k v]]
               (if (exclude-component? k)
                 [k v]
                 (let [v* (if (coll? v)
                            (map keywordize-node-id v)
                            (keywordize-node-id v))]
                   [k v*]))))
        node))

(defn keywordize-node-ids
  [node]
  (-> node keywordize-node-id keywordize-child-ids))

(def base-config
  {:fluree.db.serializer/json {}})

(defn parse-string
  [cfg]
  (if (string? cfg)
    (json/parse cfg false)
    cfg))

(defn standardize
  [cfg]
  (->> cfg
       parse-string
       json-ld/expand
       util/sequential
       flatten-nodes))

(defn parse
  ([cfg]
   (parse cfg (map derive-node-id)))
  ([cfg derive-xf]
   (into base-config
         (comp (map keywordize-node-ids)
               derive-xf
               (map (juxt get-id identity)))
         (standardize cfg))))