(ns fluree.db.json-ld.ledger
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [clojure.string :as str]))

;; methods to link/trace back a ledger and return flakes
#?(:clj (set! *warn-on-reflection* true))

(def class+property-iris #{"http://www.w3.org/2000/01/rdf-schema#Class"
                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"
                           "http://www.w3.org/2002/07/owl#Class"
                           "http://www.w3.org/2002/07/owl#ObjectProperty"
                           "http://www.w3.org/2002/07/owl#DatatypeProperty"})


(defn class-or-property?
  [{:keys [type] :as node}]
  (some class+property-iris type))

(def ^:const predefined-properties
  {"http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" const/$rdf:Property
   "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
   "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
   "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"     const/$rdf:type
   "https://ns.flur.ee/ledger#context"                   const/$fluree:context})

(def ^:const predefined-subjects
  {const/iri-default-context const/$fluree:default-context})

(defn flip-key-vals
  [map]
  (reduce #(assoc %1 (val %2) (key %2)) {} map))

(def predefined-sids
  (-> predefined-properties
      flip-key-vals
      ;; use @type json-ld shorthand instead of rdf:type full URL
      (assoc const/$rdf:type "@type")))

(defn predefined-sids-compact
  "Allows predefined sids to be mapped to values based on supplied compacting function
  generated from a context"
  [compact-fn]
  (reduce-kv
    (fn [acc k v]
      (let [v* (json-ld/compact v compact-fn)]
        (assoc acc k v*)))
    {}
    predefined-sids))

(defn last-pid
  [db]
  (-> db :ecount (get const/$_predicate)))

(defn last-sid
  "Last used sid - so next available would increment result of this by one."
  [db]
  (or (-> db :ecount (get const/$_default))
      ;; decrement because
      (dec (flake/->sid const/$_default 0))))

(defn last-commit-sid
  "Last used sid - so next available would increment result of this by one.
  Commits use a different address space than all other 'default' flakes."
  [db]
  (or (-> db :ecount (get const/$_shard))
      (dec (flake/->sid const/$_shard 0))))

(defn generate-new-sid
  [{:keys [id] :as node} iris next-pid next-sid]
  (let [new-sid (if (class-or-property? node)
                  (next-pid)
                  (or
                    (get predefined-subjects id)
                    (next-sid)))]
    (vswap! iris assoc id new-sid)
    new-sid))

(defn generate-new-pid
  "Generates a new pid for a property that has not yet been used.
  Optionally 'refs-v' is a volatile! set of refs that gets used in
  the pre-computed :schema of a db, and 'ref?', if truthy, indicates
  if this property should get added to the refs-v."
  [property-iri iris next-pid ref? refs-v]
  (let [new-pid (next-pid)]
    (vswap! iris assoc property-iri new-pid)
    (when ref?
      (vswap! refs-v conj new-pid))
    new-pid))

(defn get-iri-sid
  "Gets the IRI for any existing subject ID."
  [iri db iris]
  (if-let [cached (get @iris iri)]
    cached
    ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest add:true
    (when-let [sid (some-> (flake/match-post (get-in db [:novelty :post]) const/$iri iri)
                           first
                           :s)]
      (vswap! iris assoc iri sid)
      sid)))



(defn ledger-root
  "Returns a full ledger-root JSON-LD document for persistent storage."
  [{:keys [conn context state name reindex-min reindex-max] :as ledger}]
  (let [{:keys [branches branch]} @state
        {:keys [t dbs commit idx latest-db from]} (get branches branch)
        idx-map   {"reindexMin" reindex-min
                   "reindexMax" reindex-max
                   "t"          t
                   "idx"        idx
                   "schema"     nil
                   "context"    nil}

        branches* (not-empty
                    (->> (dissoc branches branch)
                         (map #(select-keys % [t commit idx]))))]
    {"@context" "https://ns.flur.ee/ledger/v1"
     "name"     name
     "branch"   branch
     "branches" branches*
     "t"        t
     "commit"   commit
     "idx"      idx-map
     "from"     from}))


