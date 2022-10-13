(ns fluree.db.json-ld.transact
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.reify :as jld-reify]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.datatype :as datatype]
            [fluree.db.json-ld.shacl :as shacl]))

#?(:clj (set! *warn-on-reflection* true))

(declare json-ld-node->flakes)

(defn node?
  "Returns true if a nested value is itself another node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (if (contains? mapx :value)
    false
    (let [n (count mapx)]
      (case n
        2 (not (and (contains? mapx :id)
                    (contains? mapx :idx)))
        1 (not (contains? mapx :id))                        ;; I think all nodes now contain :idx, so this is likely unnecessary check
        ;;else
        true))))


(defn json-ld-type-data
  "Returns two-tuple of [class-subject-ids class-flakes]
  where class-flakes will only contain newly generated class
  flakes if they didn't already exist."
  [class-iris {:keys [t next-pid ^clojure.lang.Volatile iris db-before] :as _tx-state}]
  (go-try
    (loop [[class-iri & r] (util/sequential class-iris)
           class-sids   #{}
           class-flakes #{}]
      (if class-iri
        (if-let [existing (<? (jld-reify/get-iri-sid class-iri db-before iris))]
          (recur r (conj class-sids existing) class-flakes)
          (let [type-sid (if-let [predefined-pid (get jld-ledger/predefined-properties class-iri)]
                           predefined-pid
                           (next-pid))]
            (vswap! iris assoc class-iri type-sid)
            (recur r
                   (conj class-sids type-sid)
                   (conj class-flakes
                         (flake/create type-sid const/$iri class-iri const/$xsd:string t true nil)
                         (flake/create type-sid const/$rdf:type const/$rdfs:Class const/$xsd:anyURI t true nil)))))
        [class-sids class-flakes]))))

(defn process-retractions
  "Processes all retractions at once from set of [sid pid] registered
  in retractions volatile! while creating new flakes."
  [{:keys [db-before retractions t] :as tx-state}]
  (go-try
    (loop [acc []
           [[sid pid] & r] @retractions]
      (if sid
        (->> (<? (query-range/index-range db-before :spot = [sid pid]))
             (map #(flake/flip-flake % t))
             (recur r))
        acc))))


(defn- new-pid
  "Generates a new property id (pid)"
  [property ref? {:keys [iris next-pid refs] :as tx-state}]
  (let [new-id (jld-ledger/generate-new-pid property iris next-pid ref? refs)]
    new-id))

(defn add-ref-flakes
  [])

(defn add-property
  "Adds property. Parameters"
  [sid pid {shacl-dt :dt, validate-fn :validate-fn} check-retracts? list? {:keys [id value] :as v-map}
   {:keys [iris next-sid t db-before] :as tx-state}]
  (go-try
    (let [retractions (when check-retracts?                 ;; don't need to check if generated pid during this transaction
                        (->> (<? (query-range/index-range db-before :spot = [sid pid]))
                             (map #(flake/flip-flake % t))))
          m           (when list?
                        {:i (-> v-map :idx last)})
          flakes      (cond
                        ;; a new node's data is contained, process as another node then link to this one
                        (node? v-map)
                        (let [node-flakes (<? (json-ld-node->flakes v-map tx-state))
                              node-sid    (get @iris id)]
                          (conj node-flakes (flake/create sid pid node-sid const/$xsd:anyURI t true m)))

                        ;; a literal value
                        (and value (not= shacl-dt const/$xsd:anyURI))
                        (let [[value* dt] (datatype/from-expanded v-map shacl-dt)]
                          (if validate-fn
                            (or (validate-fn value*)
                                (throw (ex-info (str "Value did not pass SHACL validation: " value)
                                                {:status 400 :error :db/shacl-validation}))))
                          [(flake/create sid pid value* dt t true m)])

                        ;; otherwise should be an IRI 'ref' either as an :id, or mis-cast as a value that needs coersion
                        :else
                        (let [iri (or id value)]
                          (let [blank? (str/starts-with? iri "_:")
                                [id-sid id-flake] (if-let [existing (<? (jld-reify/get-iri-sid iri db-before iris))]
                                                    [existing nil]
                                                    (let [id-sid (or (get jld-ledger/predefined-properties iri)
                                                                     (next-sid))]
                                                      (vswap! iris assoc iri id-sid)
                                                      [id-sid (flake/create id-sid const/$iri iri const/$xsd:string t true nil)]))]
                            (cond-> [(flake/create sid pid id-sid const/$xsd:anyURI t true m)]
                                    id-flake (conj id-flake)))))]
      (into flakes retractions))))

(defn list-value?
  "returns true if json-ld value is a list object."
  [v]
  (and (map? v)
       (= :list (-> v first key))))

(defn get-subject-types
  "Returns a set of all :rdf/type Class subject ids for the provided subject.
  new-types are a set of newly created types in the transaction."
  [db sid added-classes]
  (go-try
    (let [type-sids (->> (<? (query-range/index-range db :spot = [sid const/$rdf:type]))
                         (map flake/o))]
      (if (seq type-sids)
        (into added-classes type-sids)
        added-classes))))

(defn json-ld-node->flakes
  [{:keys [id type] :as node}
   {:keys [t next-pid next-sid iris db-before subj-mods] :as tx-state}]
  (go-try
    (let [existing-sid (when id
                         (<? (jld-reify/get-iri-sid id db-before iris)))
          new-subj?    (not existing-sid)
          [new-type-sids type-flakes] (when type
                                        (<? (json-ld-type-data type tx-state)))
          sid          (if new-subj?
                         (jld-ledger/generate-new-sid node iris next-pid next-sid)
                         existing-sid)
          classes      (if new-subj?
                         new-type-sids
                         (<? (get-subject-types db-before sid new-type-sids)))
          shacl-map    (<? (shacl/class-shapes db-before classes))
          id*          (if (and new-subj? (nil? id))
                         (str "_:f" sid)                    ;; create a blank node id
                         id)
          base-flakes  (cond-> []
                               new-subj? (conj (flake/create sid const/$iri id* const/$xsd:string t true nil))
                               new-type-sids (into (map #(flake/create sid const/$rdf:type % const/$xsd:anyURI t true nil) new-type-sids)))]
      ;; save SHACL, class data into atom for later validation
      (swap! subj-mods update sid
             (fn [existing]
               (if existing
                 (throw (ex-info (str "Subject " id " is being updated in more than one JSON-LD map. "
                                      "All items for a single subject should be consolidated.")
                                 {:status 400 :error :db/invalid-transaction}))
                 {:shacl   shacl-map
                  :new?    new-subj?
                  :classes classes})))
      (loop [[[k v] & r] (dissoc node :id :idx :type)
             property-flakes type-flakes                    ;; only used if generating new Class and Property flakes
             subj-flakes     base-flakes]
        (if k
          (let [list?            (list-value? v)
                v*               (if list?
                                   (let [list-vals (:list v)]
                                     (when-not (sequential? list-vals)
                                       (throw (ex-info (str "List values have to be vectors, provided: " v)
                                                       {:status 400 :error :db/invalid-transaction})))
                                     list-vals)
                                   (util/sequential v))
                ref?             (not (:value (first v*)))  ;; either a ref or a value
                existing-pid     (<? (jld-reify/get-iri-sid k db-before iris))
                pid              (or existing-pid
                                     (get jld-ledger/predefined-properties k)
                                     (new-pid k ref? tx-state))
                datatype-map     (get-in shacl-map [:datatype pid])
                property-flakes* (if existing-pid
                                   property-flakes
                                   (cond-> (conj property-flakes (flake/create pid const/$iri k const/$xsd:string t true nil))
                                           ref? (conj (flake/create pid const/$rdf:type const/$iri const/$xsd:anyURI t true nil))))
                ;; check-retracts? - a new subject or property don't require checking for flake retractions
                check-retracts?  (or (not new-subj?) existing-pid)
                flakes*          (loop [[v' & r] v*
                                        flakes* subj-flakes]
                                   (if v'
                                     (recur r (into flakes* (<? (add-property sid pid datatype-map check-retracts? list? v' tx-state))))
                                     (cond-> flakes*
                                             property-flakes (into property-flakes))))]
            (recur r property-flakes* flakes*))
          (into subj-flakes property-flakes))))))

(defn ->tx-state
  [db {:keys [bootstrap?] :as _opts}]
  (let [{:keys [block ecount schema branch ledger], db-t :t} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        commit-t (-> (ledger-proto/-status ledger branch) branch/latest-commit-t)
        t        (-> commit-t inc -)]                       ;; commit-t is always positive, need to make negative for internal indexing
    {:db-before     db
     :bootstrap?    bootstrap?
     :stage-update? (= t db-t)                              ;; if a previously staged db is getting updated again before committed
     :refs          (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t             t
     :new?          (zero? db-t)
     :block         block
     :last-pid      last-pid
     :last-sid      last-sid
     :next-pid      (fn [] (vswap! last-pid inc))
     :next-sid      (fn [] (vswap! last-sid inc))
     :subj-mods     (atom {})                               ;; holds map of subj ids (keys) for modified flakes map with shacl shape and classes
     :iris          (volatile! {})}))

(defn final-ecount
  [tx-state]
  (let [{:keys [db-before last-pid last-sid]} tx-state
        {:keys [ecount]} db-before]
    (assoc ecount const/$_predicate @last-pid
                  const/$_default @last-sid)))

(defn add-tt-id
  "Associates a unique tt-id for any in-memory staged db in their index roots.
  tt-id is used as part of the caching key, by having this in place it means
  that even though the 't' value hasn't changed it will cache each stage db
  data as its own entity."
  [db]
  (let [tt-id   (random-uuid)
        indexes [:spot :psot :post :opst :tspo]]
    (-> (reduce
          (fn [db* idx]
            (let [{:keys [children] :as node} (get db* idx)
                  children* (reduce-kv
                              (fn [children* k v]
                                (assoc children* k (assoc v :tt-id tt-id)))
                              {} children)]
              (assoc db* idx (assoc node :tt-id tt-id
                                         :children children*))))
          db indexes)
        (assoc :tt-id tt-id))))

(defn remove-tt-id
  "Removes a tt-id placed on indexes (opposite of add-tt-id)."
  [db]
  (let [indexes [:spot :psot :post :opst :tspo]]
    (reduce
      (fn [db* idx]
        (let [{:keys [children] :as node} (get db* idx)
              children* (reduce-kv
                          (fn [children* k v]
                            (assoc children* k (dissoc v :tt-id)))
                          {} children)]
          (assoc db* idx (-> node
                             (dissoc :tt-id)
                             (assoc :children children*)))))
      (dissoc db :tt-id) indexes)))

(defn update-novelty-idx
  [novelty-idx add remove]
  (-> (reduce disj novelty-idx remove)
      (into add)))

(defn base-flakes
  "Returns base set of flakes needed in any new ledger."
  [t]
  [(flake/create const/$rdf:type const/$iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" const/$xsd:string t true nil)
   (flake/create const/$rdfs:Class const/$iri "http://www.w3.org/2000/01/rdf-schema#Class" const/$xsd:string t true nil)
   (flake/create const/$iri const/$iri "@id" const/$xsd:string t true nil)])

(defn ref-flakes
  "Returns ref flakes from set of all flakes"
  [flakes schema]
  (->> flakes
       (sort-by flake/p)
       (partition-by flake/p)
       (reduce
         (fn [acc p-flakes]
           (if (get-in schema [:pred (flake/p (first p-flakes)) :ref?])
             (into acc p-flakes)
             acc))
         [])))

;; TODO - can use transient! below
(defn stage-update-novelty
  "If a db is staged more than once, any retractions in a previous stage will
  get completely removed from novelty. This returns flakes that must be added and removed
  from novelty."
  [novelty-flakes new-flakes]
  (loop [[f & r] new-flakes
         adds    new-flakes
         removes (empty new-flakes)]
    (if f
      (if (true? (flake/op f))
        (recur r adds removes)
        (let [flipped (flake/flip-flake f)]
          (if (contains? novelty-flakes flipped)
            (recur r (disj adds f) (conj removes flipped))
            (recur r adds removes))))
      [(not-empty adds) (not-empty removes)])))

(defn db-after
  [{:keys [add remove ref-add ref-remove size count schema] :as staged} {:keys [db-before bootstrap? t block] :as tx-state}]
  (let [{:keys [novelty]} db-before
        {:keys [spot psot post opst tspo]} novelty
        new-db (assoc db-before :ecount (final-ecount tx-state)
                                :t t
                                :block block
                                :novelty {:spot (update-novelty-idx spot add remove)
                                          :psot (update-novelty-idx psot add remove)
                                          :post (update-novelty-idx post add remove)
                                          :opst (update-novelty-idx opst ref-add ref-remove)
                                          :tspo (update-novelty-idx tspo add remove)
                                          :size (+ (:size novelty) size)}
                                :stats (-> (:stats db-before)
                                           (update :size + size)
                                           (update :flakes + count))
                                :schema schema)]
    (if bootstrap?
      new-db
      (add-tt-id new-db))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an updated db."
  [new-flakes {:keys [t stage-update? db-before refs class-mods] :as _tx-state}]
  (let [[add remove] (if stage-update?
                       (stage-update-novelty (get-in db-before [:novelty :spot]) new-flakes)
                       [new-flakes nil])
        vocab-flakes (jld-reify/get-vocab-flakes new-flakes)
        schema       (vocab/update-with db-before t @refs vocab-flakes)

        final-map    {:add        add
                      :remove     remove
                      :ref-add    (ref-flakes add schema)
                      :ref-remove (ref-flakes remove schema)
                      :count      (cond-> (when add (count add))
                                          remove (- (count remove)))
                      :size       (cond-> (flake/size-bytes add)
                                          remove (- (flake/size-bytes remove)))
                      :schema     schema}]
    (assoc final-map :db-after (db-after final-map _tx-state))))

(defn stage-flakes
  [json-ld {:keys [new? t] :as tx-state}]
  (go-try
    (let [ss (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                     new? (into (base-flakes t)))]
      (loop [[node & r] (util/sequential json-ld)
             flakes* ss]
        (if node
          (recur r (into flakes* (<? (json-ld-node->flakes node tx-state))))
          flakes*)))))

(defn validate-rules
  [{:keys [db-after add]} {:keys [subj-mods] :as tx-state}]
  (let [subj-mods' @subj-mods]
    (go-try
      (loop [[s-flakes & r] (partition-by flake/s add)
             all-classes #{}]
        (if s-flakes
          (let [sid        (flake/s (first s-flakes))
                {:keys [new? classes shacl]} (get subj-mods' sid)
                all-flakes (if new?
                             s-flakes
                             (<? (query-range/index-range db-after :spot = [sid])))]
            (when shacl
              (<? (shacl/validate-target db-after shacl all-flakes)))
            (recur r (into all-classes classes)))
          (let [new-shacl? (or (contains? all-classes const/$sh:NodeShape)
                               (contains? all-classes const/$sh:PropertyShape))]
            (when new-shacl?
              ;; TODO - PropertyShape class is often not specified for sh:property nodes - direct changes to those would not be caught here!
              (vocab/reset-shapes (:schema db-after)))
            db-after))))))

(defn stage
  "Stages changes, but does not commit.
  Returns async channel that will contain updated db or exception."
  [{:keys [ledger schema] :as db} json-ld opts]
  (go-try
    (let [tx-state (->tx-state db opts)
          db*      (-> json-ld
                       (json-ld/expand (:context schema))
                       (stage-flakes tx-state)
                       <?
                       (final-db tx-state)
                       (validate-rules tx-state)
                       <?)]
      (ledger-proto/-db-update ledger db*))))
