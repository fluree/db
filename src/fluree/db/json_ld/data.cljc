(ns fluree.db.json-ld.data
  (:require
   [fluree.json-ld :as json-ld]
   [fluree.db.json-ld.reify :as jld-reify]
   [fluree.db.flake :as flake]
   [fluree.db.constants :as const]
   [fluree.db.util.async :refer [<? go-try]]
   [fluree.db.util.core :as util :refer [try* catch*]]
   [clojure.core.async :as async]
   [fluree.db.util.log :as log]
   [fluree.db.datatype :as datatype]
   [fluree.json-ld.processor.api :as jld-processor]))

(defn create-id-flake
  [sid iri t]
  (flake/create sid const/$xsd:anyURI iri const/$xsd:string t true nil))

(defn lookup-iri
  [{:keys [db-before iri-cache asserts] :as tx-state} iri]
  (go-try
    (or (<? (jld-reify/get-iri-sid iri db-before iri-cache))
        (some->> asserts
                 (filter (fn [f]
                           (and (= iri (flake/o f))
                                (= const/$xsd:anyURI (flake/p f)))))
                 (first)
                 (flake/s)))))

(defn bnode-id
  [sid]
  (str "_:" sid))

(declare insert-sid)
(defn insert-flake
  [sid pid m {:keys [db-before iri-cache next-sid t] :as tx-state}
   {:keys [value id type language list] :as v-map}]
  (go-try
    (cond list
          (loop [[[i list-item :as item] & r] (map vector (range) list)
                 tx-state tx-state]
            (if item
              (recur r (<? (insert-flake sid pid {:i i} tx-state list-item)))
              tx-state))

          ;; literal
          value
          (let [[o dt] (datatype/from-expanded v-map nil)]
            ;; TODO: support coercion of dt
            ;; TODO: add language to meta
            (update tx-state :asserts conj (flake/create sid pid o dt t true m)))

          ;; ref
          :else
          (<? (insert-sid tx-state v-map)))))

(defn insert-pid
  [sid {:keys [db-before iri-cache next-pid t] :as tx-state} [predicate values]]
  (go-try
    (let [existing-pid (<? (lookup-iri tx-state predicate))
          pid          (if existing-pid existing-pid (next-pid))]
      (loop [[v-map & r] values
             tx-state (cond-> tx-state
                        (not existing-pid) (update :asserts conj (create-id-flake pid predicate t)))]
        (println "DEP v-map" v-map)
        (if v-map
          (recur r (<? (insert-flake sid pid nil tx-state v-map)))
          tx-state)))))

(defn insert-sid
  [{:keys [db-before asserts iri-cache next-sid t] :as tx-state} {:keys [id] :as subject}]
  (go-try
    (let [existing-sid (when id (<? (lookup-iri tx-state id)))
          [sid iri]    (if (nil? id)
                         (let [bnode-sid (next-sid)]
                           [bnode-sid (bnode-id bnode-sid)])
                         ;; TODO: not handling pid generation
                         [(or existing-sid (next-sid)) id])]
      (loop [[entry & r] (dissoc subject :id :idx)
             tx-state    (cond-> tx-state
                           (not existing-sid) (update :asserts conj (create-id-flake sid iri t)))]
        (if entry
          (recur r (<? (insert-pid sid tx-state entry)))
          tx-state)))))

(defn insert-flakes
  [{:keys [default-context] :as tx-state} data]
  (reduce insert-sid
          tx-state
          (util/sequential (json-ld/expand data default-context))))
