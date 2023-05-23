(ns fluree.db.json-ld.transact
  (:refer-clojure :exclude [vswap!])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.json-ld.reify :as jld-reify]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.policy.enforce-tx :as policy]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [vswap!]]
            [fluree.db.util.log :as log]
            [fluree.db.validation :as v]
            [fluree.json-ld :as json-ld]
            [malli.core :as m]))

#?(:clj (set! *warn-on-reflection* true))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   v/registry
   {::triple       ::v/triple
    ::txn-leaf-map [:map-of
                    [:orn [:string :string] [:keyword :keyword]]
                    :any]
    ::retract      [:and
                    [:map-of :keyword :any]
                    [:map [:retract ::txn-leaf-map]]]
    ::modification ::v/modification-txn
    ::txn-map      [:orn
                    [:retract ::retract]
                    [:modification ::modification]
                    [:assert ::txn-leaf-map]]
    ::txn          [:orn
                    [:single-map ::txn-map]
                    [:sequence-of-maps [:sequential ::txn-map]]]}))

(declare json-ld-node->flakes)

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
                         (flake/create type-sid const/$xsd:anyURI class-iri const/$xsd:string t true nil)
                         (flake/create type-sid const/$rdf:type const/$rdfs:Class const/$xsd:anyURI t true nil)))))
        [class-sids class-flakes]))))


(defn- new-pid
  "Generates a new property id (pid)"
  [property ref? {:keys [iris next-pid refs] :as _tx-state}]
  (let [new-id (jld-ledger/generate-new-pid property iris next-pid ref? refs)]
    new-id))


(defn add-property
  "Adds property. Parameters"
  [sid pid {shacl-dt :dt, validate-fn :validate-fn} check-retracts? list? {:keys [value] :as v-map}
   {:keys [t db-before] :as tx-state}]
  (go-try
    (let [retractions (when check-retracts? ;; don't need to check if generated pid during this transaction
                        (->> (<? (query-range/index-range db-before :spot = [sid pid]))
                             (map #(flake/flip-flake % t))))
          m           (when list?
                        {:i (-> v-map :idx last)})
          flakes      (cond
                        ;; a new node's data is contained, process as another node then link to this one
                        (jld-reify/node? v-map)
                        (let [[node-sid node-flakes] (<? (json-ld-node->flakes v-map tx-state pid))]
                          (conj node-flakes (flake/create sid pid node-sid const/$xsd:anyURI t true m)))

                        ;; a literal value
                        (and (some? value) (not= shacl-dt const/$xsd:anyURI))
                        (let [[value* dt] (datatype/from-expanded v-map shacl-dt)]
                          (when validate-fn
                            (or (validate-fn value*)
                                (throw (ex-info (str "Value did not pass SHACL validation: " value)
                                                {:status 400 :error :db/shacl-validation}))))
                          [(flake/create sid pid value* dt t true m)])

                        :else
                        (throw (ex-info (str "JSON-LD value must be a node or a value, instead found ambiguous value: " v-map)
                                        {:status 400 :error :db/invalid-transaction})))]
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

(defn iri-only?
  "Returns true if a JSON-LD node contains only an IRI and no actual property data.

  Note, this is only used if we already know the node is a subject (not a scalar value)
  so no need to check for presence of :id."
  [node]
  (= 2 (count node)))

(defn register-node
  "Registers nodes being created/updated in an atom to verify the same node isn't being
  manipulated multiple spots and also registering shacl rules that need further processing
  once completed."
  [subj-mods node sid node-meta-map]
  (swap! subj-mods update sid
         (fn [existing]
           (if-not existing
             node-meta-map
             (cond
               ;; if previously created, but this is just using the IRI it is OK
               (:iri-only? node-meta-map)
               existing

               ;; if previously updated, but prior updates were only the IRI then it is OK
               (:iri-only? existing)
               node-meta-map

               :else
               (throw (ex-info (str "Subject " (:id node) " is being updated in more than one JSON-LD map. "
                                    "All items for a single subject should be consolidated.")
                               {:status 400 :error :db/invalid-transaction})))))))

(defn json-ld-node->flakes
  "Returns two-tuple of [sid node-flakes] that will contain the top-level sid
  and all flakes from the target node and all children nodes that ultimately get processed.

  If property-id is non-nil, it can be checked when assigning new subject id for the node
  if it meets certain criteria. It will only be non-nil for nested subjects in the json-ld."
  [{:keys [id type] :as node}
   {:keys [t next-pid next-sid iris db-before subj-mods] :as tx-state}
   referring-pid]
  (go-try
    (let [existing-sid (when id
                         (<? (jld-reify/get-iri-sid id db-before iris)))
          new-subj?    (not existing-sid)
          [new-type-sids type-flakes] (when type
                                        (<? (json-ld-type-data type tx-state)))
          sid          (if new-subj?
                         ;; TODO - this will check if subject is rdfs:Class, but we already have the new-type-sids above and know that - this can be a little faster, but reify.cljc also uses this logic and they need to align
                         (jld-ledger/generate-new-sid node referring-pid iris next-pid next-sid)
                         existing-sid)
          classes      (if new-subj?
                         new-type-sids
                         (<? (get-subject-types db-before sid new-type-sids)))
          shacl-map    (<? (shacl/class-shapes db-before classes))
          id*          (if (and new-subj? (nil? id))
                         (str "_:f" sid) ;; create a blank node id
                         id)
          base-flakes  (cond-> []
                               new-subj? (conj (flake/create sid const/$xsd:anyURI id* const/$xsd:string t true nil))
                               new-type-sids (into (map #(flake/create sid const/$rdf:type % const/$xsd:anyURI t true nil) new-type-sids)))]
      ;; save SHACL, class data into atom for later validation - checks that same @id not being updated in multiple spots
      (register-node subj-mods node sid {:iri-only? (iri-only? node)
                                         :shacl     shacl-map
                                         :new?      new-subj?
                                         :classes   classes})
      (loop [[[k v] & r] (dissoc node :id :idx :type)
             property-flakes type-flakes ;; only used if generating new Class and Property flakes
             subj-flakes     base-flakes]
        (if k
          (let [list?            (list-value? v)
                retract?         (nil? v)
                v*               (if list?
                                   (let [list-vals (:list v)]
                                     (when-not (sequential? list-vals)
                                       (throw (ex-info (str "List values have to be vectors, provided: " v)
                                                       {:status 400 :error :db/invalid-transaction})))
                                     list-vals)
                                   (util/sequential v))
                ref?             (not (:value (first v*))) ;; either a ref or a value
                existing-pid     (<? (jld-reify/get-iri-sid k db-before iris))
                pid              (or existing-pid
                                     (get jld-ledger/predefined-properties k)
                                     (new-pid k ref? tx-state))
                datatype-map     (get-in shacl-map [:datatype pid])
                property-flakes* (if existing-pid
                                   property-flakes
                                   (conj property-flakes (flake/create pid const/$xsd:anyURI k const/$xsd:string t true nil)))
                ;; check-retracts? - a new subject or property don't require checking for flake retractions
                check-retracts?  (or (not new-subj?) existing-pid)
                flakes*          (if retract?
                                   (->> (<? (query-range/index-range db-before :spot = [sid pid]))
                                        (map #(flake/flip-flake % t)))
                                   (loop [[v' & r] v*
                                          flakes* subj-flakes]
                                     (if v'
                                       (recur r (into flakes* (<? (add-property sid pid datatype-map check-retracts? list? v' tx-state))))
                                       (cond-> flakes*
                                               property-flakes (into property-flakes)))))]
            (recur r property-flakes* flakes*))
          ;; return two-tuple of node's final sid (needed to link nodes together) and the resulting flakes
          [sid (into subj-flakes property-flakes)])))))

(defn ->tx-state
  [db {:keys [bootstrap? did context-type] :as _opts}]
  (let [{:keys [schema branch ledger policy], db-t :t} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        commit-t (-> (ledger-proto/-status ledger branch) branch/latest-commit-t)
        t        (-> commit-t inc -)] ;; commit-t is always positive, need to make negative for internal indexing
    {:did         did
     :db-before   (dbproto/-rootdb db)
     :policy      policy
     :bootstrap?  bootstrap?
     :default-ctx (if context-type
                    (dbproto/-context db ::dbproto/default-context context-type)
                    (dbproto/-context db))
     :stage-update? (= t db-t) ;; if a previously staged db is getting updated again before committed
     :refs          (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t             t
     :last-pid      last-pid
     :last-sid      last-sid
     :next-pid      (fn [] (vswap! last-pid inc))
     :next-sid      (fn [] (vswap! last-sid inc))
     :subj-mods     (atom {}) ;; holds map of subj ids (keys) for modified flakes map with shacl shape and classes
     :iris          (volatile! {})}))

(defn final-ecount
  [tx-state]
  (let [{:keys [db-before last-pid last-sid]} tx-state
        {:keys [ecount]} db-before]
    (assoc ecount const/$_predicate @last-pid
                  const/$_default @last-sid)))

(defn base-flakes
  "Returns base set of flakes needed in any new ledger."
  [t]
  [(flake/create const/$rdf:type const/$xsd:anyURI const/iri-rdf-type const/$xsd:string t true nil)
   (flake/create const/$rdfs:Class const/$xsd:anyURI const/iri-class const/$xsd:string t true nil)
   (flake/create const/$xsd:anyURI const/$xsd:anyURI "@id" const/$xsd:string t true nil)])

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
  [{:keys [add remove] :as _staged} {:keys [db-before policy bootstrap? t] :as tx-state}]
  (let [new-db (-> db-before
                   (assoc :ecount (final-ecount tx-state)
                          :policy policy ;; re-apply policy to db-after
                          :t t)
                   (commit-data/update-novelty add remove))]
    (if bootstrap?
      new-db
      ;; TODO - we used to add tt-id to break the cache, so multiple 'staged' dbs with same t value don't get cached as the same
      ;; TODO - now that each db should have its own unique hash, we can use the db's hash id instead of 't' or 'tt-id' for caching
      (commit-data/add-tt-id new-db))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [new-flakes {:keys [stage-update? db-before] :as tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db-before [:novelty :spot]) new-flakes)
                         [new-flakes nil])
          vocab-flakes (jld-reify/get-vocab-flakes new-flakes)
          staged-map   {:add    add
                        :remove remove}
          db-after     (cond-> (db-after staged-map tx-state)
                         vocab-flakes vocab/refresh-schema
                         vocab-flakes <?)]
      (assoc staged-map :db-after db-after))))

(defn stage-flakes
  [flakeset tx-state nodes]
  (go-try
    (loop [[node & r] nodes
           flakes* flakeset]
      (if node
        (if (empty? (dissoc node :idx :id))
          (throw (ex-info (str "Invalid transaction, transaction node contains no properties"
                               (some->> (:id node)
                                        (str " for @id: "))
                               ".")
                          {:status 400 :error :db/invalid-transaction}))
          (let [[_node-sid node-flakes] (<? (json-ld-node->flakes node tx-state nil))]
            (recur r (into flakes* node-flakes))))
        flakes*))))

(defn validate-rules
  [{:keys [db-after add] :as staged-map} {:keys [subj-mods] :as _tx-state}]
  (let [subj-mods' @subj-mods
        root-db    (dbproto/-rootdb db-after)]
    (go-try
      (loop [[s-flakes & r] (partition-by flake/s add)
             all-classes #{}]
        (if s-flakes
          (let [sid (flake/s (first s-flakes))
                {:keys [new? classes shacl]} (get subj-mods' sid)]
            (when shacl
              (let [all-flakes (if new?
                                 s-flakes
                                 (<? (query-range/index-range root-db :spot = [sid])))]
                (<? (shacl/validate-target shacl all-flakes))))
            (recur r (into all-classes classes)))
          (let [new-shacl? (or (contains? all-classes const/$sh:NodeShape)
                               (contains? all-classes const/$sh:PropertyShape))]
            (when new-shacl?
              ;; TODO - PropertyShape class is often not specified for sh:property nodes - direct changes to those would not be caught here!
              (vocab/reset-shapes (:schema db-after)))
            staged-map))))))

(defn init-db?
  [db]
  (-> db :t zero?))

(defn insert
  "Performs insert transaction. Returns async chan with resulting flakes."
  [{:keys [t] :as db} json-ld {:keys [default-ctx] :as tx-state}]
  (log/debug "insert default-ctx:" default-ctx)
  (let [nodes    (-> json-ld
                     (json-ld/expand default-ctx)
                     util/sequential)
        flakeset (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                         (init-db? db) (into (base-flakes t)))]
    (stage-flakes flakeset tx-state nodes)))

(defn into-flakeset
  [flake-ch]
  (let [flakeset (flake/sorted-set-by flake/cmp-flakes-spot)]
    (async/reduce into flakeset flake-ch)))

(defn modify
  [db json-ld {:keys [t] :as _tx-state}]
  (go
    (let [mdfn         (-> json-ld
                           syntax/coerce-modification
                           (q-parse/parse-modification db))
          error-ch     (async/chan)
          fuel-tracker (fuel/tracker)
          update-ch    (->> (where/search db mdfn fuel-tracker error-ch)
                            (update/modify db mdfn t fuel-tracker error-ch)
                            into-flakeset)]
      (async/alt!
        error-ch  ([e] e)
        update-ch ([flakes] flakes)))))

(defn flakes->final-db
  "Takes final set of proposed staged flakes and turns them into a new db value
  along with performing any final validation and policy enforcement."
  [tx-state flakes]
  (go-try
    (-> flakes
        (final-db tx-state)
        <?
        (validate-rules tx-state)
        <?
        (policy/allowed? tx-state)
        <?
        ;; unwrap the policy
        (dbproto/-rootdb))))

(defn stage
  "Stages changes, but does not commit.
  Returns async channel that will contain updated db or exception."
  [db json-ld opts]
  (go-try
    (let [{tx :subject did :did} (or (<? (cred/verify json-ld))
                                        {:subject json-ld})
          opts* (cond-> opts did (assoc :did did))
          db* (if-let [policy-opts (perm/policy-opts opts*)]
                (<? (perm/wrap-policy db policy-opts))
                db)
          tx-state (->tx-state db* opts*)
          flakes   (if (q-parse/update? tx)
                     (<? (modify db tx tx-state))
                     (<? (insert db tx tx-state)))]
      (<? (flakes->final-db tx-state flakes)))))
