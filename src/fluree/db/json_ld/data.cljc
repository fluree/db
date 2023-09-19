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
   [fluree.json-ld.processor.api :as jld-processor]
   [fluree.db.json-ld.shacl :as shacl]
   [fluree.db.query.range :as query-range]))

(defn create-id-flake
  [sid iri t]
  (flake/create sid const/$xsd:anyURI iri const/$xsd:string t true nil))

(defn lookup-iri
  [{:keys [db-before iri-cache flakes] :as tx-state} iri]
  (go-try
    (or (<? (jld-reify/get-iri-sid iri db-before iri-cache))
        (some->> flakes
                 (filter (fn [f]
                           (and (flake/op f)
                                (= const/$xsd:anyURI (flake/p f))
                                (= iri (flake/o f)))))
                 (first)
                 (flake/s)))))

(defn bnode-id
  [sid]
  (str "_:" sid))

(defn ref-dt
  "Some predicates are known to have references as objects."
  [pid]
  (get {const/$rdf:type const/$xsd:anyURI}
    pid))

(declare insert-subject)
(defn insert-flake
  [sid pid m {:keys [db-before iri-cache track-fuel next-sid t] :as tx-state}
   {:keys [value id type language list] :as v-map}]
  (go-try
    (cond list
          (loop [[[i list-item :as item] & r] (map vector (range) list)
                 tx-state                     tx-state]
            (if item
              (recur r (<? (insert-flake sid pid {:i i} tx-state list-item)))
              tx-state))

          ;; literal
          (some? value)
          (let [existing-dt  (when type (<? (lookup-iri tx-state type)))
                dt           (cond existing-dt  existing-dt
                                   (ref-dt pid) (ref-dt pid)
                                   type         (next-sid)
                                   :else        (datatype/infer value language))
                m*           (cond-> m
                               language (assoc :lang language))
                new-dt-flake (when (and type (not existing-dt)) (create-id-flake dt type t))
                new-flake    (flake/create sid pid value dt t true m*)]
            (-> tx-state
                (update :flakes into (comp (remove nil?) track-fuel) [new-dt-flake new-flake])))

          ;; ref
          :else
          (let [bnode-sid (when-not id (next-sid))
                bnode-iri (when-not id (bnode-id bnode-sid))

                v-map*    (cond-> v-map
                            bnode-iri (assoc :id bnode-iri))

                tx-state  (cond-> tx-state
                            bnode-iri (update :flakes into track-fuel [(create-id-flake bnode-sid bnode-iri t)]))

                tx-state* (<? (insert-subject tx-state v-map*))

                ref-sid   (if id
                            ;; sid was generated/found by `insert-subject`
                            (<? (lookup-iri tx-state* id))
                            bnode-sid)
                ref-flake (flake/create sid pid ref-sid const/$xsd:anyURI t true m)]
            (-> tx-state*
                (update :flakes into track-fuel [ref-flake]))))))

(defn insert-predicate
  [sid {:keys [db-before iri-cache track-fuel next-pid t shapes] :as tx-state} [predicate values]]
  (go-try
    (let [existing-pid        (<? (lookup-iri tx-state predicate))
          pid                 (if existing-pid existing-pid (next-pid))]
      (loop [[v-map & r] values
             tx-state    (cond-> tx-state
                           (not existing-pid) (update :flakes into track-fuel [(create-id-flake pid predicate t)]))]
        (if v-map
          (recur r (<? (insert-flake sid pid nil tx-state v-map)))
          tx-state)))))

(defn insert-subject
  [{:keys [db-before iri-cache track-fuel next-sid t] :as tx-state} {:keys [id] :as subject}]
  (go-try
    (let [existing-sid     (when id (<? (lookup-iri tx-state id)))
          [sid iri]        (if (nil? id)
                             (let [bnode-sid (next-sid)]
                               [bnode-sid (bnode-id bnode-sid)])
                             ;; TODO: not handling pid generation
                             [(or existing-sid (next-sid)) id])]
      (loop [[entry & r] (dissoc subject :id :idx)
             tx-state    (cond-> tx-state
                           (not existing-sid) (update :flakes into track-fuel [(create-id-flake sid iri t)]))]
        (if entry
          (recur r (<? (insert-predicate sid tx-state entry)))
          tx-state)))))

(defn insert-flakes
  [{:keys [default-ctx] :as tx-state} data]
  (go-try
    (loop [[subject & r] (when data (util/sequential (json-ld/expand data default-ctx)))
           tx-state tx-state]
      (if subject
        (recur r (<? (insert-subject tx-state subject)))
        tx-state))))

(declare delete-subject)
(defn delete-flake
  [sid pid m {:keys [db-before iri-cache track-fuel t] :as tx-state}
   {:keys [value id type language list] :as v-map}]
  (go-try
    (cond list
          (loop [[[i list-item :as item] & r] (map vector (range) list)
                 tx-state                     tx-state]
            (if item
              (recur r (<? (delete-flake sid pid {:i i} tx-state list-item)))
              tx-state))

          ;; literal
          (some? value)
          (if-let [existing-flake (first (<? (query-range/index-range db-before :spot = [sid pid value])))]
            (let [dt (or (when type (<? (lookup-iri tx-state type)))
                         (datatype/infer value))
                  match-flake (flake/create sid pid value dt (flake/t existing-flake) true m)]
              (update tx-state :flakes into track-fuel [(flake/flip-flake existing-flake t)]))
            tx-state)

          ;; ref
          :else
          (if-let [ref-sid (<? (lookup-iri tx-state id))]
            (if-let [ref-flake (first (<? (query-range/index-range db-before :spot = [sid pid ref-sid])))]
              (let [tx-state* (<? (delete-subject tx-state v-map))]
                (update tx-state* :flakes into track-fuel [(flake/flip-flake ref-flake t)]))
              tx-state)
            tx-state))))

(defn delete-predicate
  [sid tx-state [predicate values]]
  (go-try
    (if-let [existing-pid (<? (lookup-iri tx-state predicate))]
      (loop [[v-map & r] values
             tx-state tx-state]
        (if v-map
          (recur r (<? (delete-flake sid existing-pid nil tx-state v-map)))
          tx-state))
      tx-state)))

(defn delete-subject
  [tx-state {:keys [id] :as subject}]
  (go-try
    (if-let [existing-sid (when id (<? (lookup-iri tx-state id)))]
      (loop [[entry & r] (dissoc subject :id :idx)
             tx-state tx-state]
        (if entry
          (recur r (<? (delete-predicate existing-sid tx-state entry)))
          tx-state))
      tx-state)))

(defn delete-flakes
  [{:keys [default-ctx] :as tx-state} data]
  (go-try
    (loop [[subject & r] (when data (util/sequential (json-ld/expand data default-ctx)))
           tx-state tx-state]
      (if subject
        (recur r (<? (delete-subject tx-state subject)))
        tx-state))))

(defn upsert-predicate
  [sid s-flakes {:keys [track-fuel t] :as tx-state} [predicate values :as entry]]
  (go-try
    (if-let [existing-pid (<? (lookup-iri tx-state predicate))]
      (let [existing-p-flakes (into [] (filter #(= existing-pid (flake/p %))) s-flakes)]
        (loop [[v-map & r] values
               tx-state (cond-> tx-state
                          (not-empty existing-p-flakes)
                          (update :flakes into (comp track-fuel (map #(flake/flip-flake % t)))
                                  existing-p-flakes))]
          (if v-map
            (recur r (<? (insert-flake sid existing-pid nil tx-state v-map)))
            tx-state)))
      (<? (insert-predicate sid tx-state entry)))))

(defn upsert-subject
  [{:keys [db-before] :as tx-state} {:keys [id] :as subject}]
  (go-try
    (if-let [existing-sid (when id (<? (lookup-iri tx-state id)))]
      (let [s-flakes (<? (query-range/index-range db-before :spot = [existing-sid]))]
        (loop [[entry & r] (dissoc subject :id :idx)
               tx-state tx-state]
          (if entry
            (recur r (<? (upsert-predicate existing-sid s-flakes tx-state entry)))
            tx-state)))
      (<? (insert-subject tx-state subject)))))

(defn upsert-flakes
  [{:keys [default-ctx] :as tx-state} data]
  (go-try
    (loop [[subject & r] (when data (util/sequential (json-ld/expand data default-ctx)))
           tx-state tx-state]
      (if subject
        (recur r (<? (upsert-subject tx-state subject)))
        tx-state))))
