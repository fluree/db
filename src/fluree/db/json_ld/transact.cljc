(ns fluree.db.json-ld.transact
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.reify :as jld-reify]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util :refer [vswap!]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.datatype :as datatype]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.exec.where :as where]
            [clojure.core.async :as async]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.policy.enforce-tx :as policy]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.util.log :as log])
  (:refer-clojure :exclude [vswap!]))

#?(:clj (set! *warn-on-reflection* true))

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
                         (flake/create type-sid const/$iri class-iri const/$xsd:string t true nil)
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
                        (and value (not= shacl-dt const/$xsd:anyURI))
                        (let [[value* dt] (datatype/from-expanded v-map shacl-dt)]
                          (if validate-fn
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
                               new-subj? (conj (flake/create sid const/$iri id* const/$xsd:string t true nil))
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
                                   (cond-> (conj property-flakes (flake/create pid const/$iri k const/$xsd:string t true nil))
                                           ref? (conj (flake/create pid const/$rdf:type const/$iri const/$xsd:anyURI t true nil))))
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
  [db {:keys [bootstrap? issuer context-type] :as _opts}]
  (let [{:keys [block ecount schema branch ledger policy], db-t :t} db
        last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        commit-t (-> (ledger-proto/-status ledger branch) branch/latest-commit-t)
        t        (-> commit-t inc -)] ;; commit-t is always positive, need to make negative for internal indexing
    {:issuer        issuer
     :db-before     (dbproto/-rootdb db)
     :policy        policy
     :bootstrap?    bootstrap?
     :default-ctx   (if (= :string context-type)
                      (:context-str schema)
                      (:context schema))
     :stage-update? (= t db-t) ;; if a previously staged db is getting updated again before committed
     :refs          (volatile! (or (:refs schema) #{const/$rdf:type}))
     :t             t
     :block         block
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
  "Returns ref flakes from set of all flakes. Uses Flake datatype to know if a ref."
  [flakes]
  (filter #(= (flake/dt %) const/$xsd:anyURI) flakes))

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
  [{:keys [add remove ref-add ref-remove size count] :as staged} {:keys [db-before policy bootstrap? t block] :as tx-state}]
  (let [{:keys [novelty]} db-before
        {:keys [spot psot post opst tspo]} novelty
        new-db (assoc db-before :ecount (final-ecount tx-state)
                                :policy policy ;; re-apply policy to db-after
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
                                           (update :flakes + count)))]
    (if bootstrap?
      new-db
      ;; TODO - we used to add tt-id to break the cache, so multiple 'staged' dbs with same t value don't get cached as the same
      ;; TODO - now that each db should have its own unique hash, we can use the db's hash id instead of 't' or 'tt-id' for caching
      (add-tt-id new-db))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an updated db."
  [new-flakes {:keys [t stage-update? db-before refs class-mods] :as _tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db-before [:novelty :spot]) new-flakes)
                         [new-flakes nil])
          vocab-flakes (jld-reify/get-vocab-flakes new-flakes)
          staged-map   {:add        add
                        :remove     remove
                        :ref-add    (ref-flakes add)
                        :ref-remove (ref-flakes remove)
                        :count      (cond-> (if add (count add) 0)
                                            remove (- (count remove)))
                        :size       (cond-> (flake/size-bytes add)
                                            remove (- (flake/size-bytes remove)))}
          db-after     (cond-> (db-after staged-map _tx-state)
                               vocab-flakes vocab/refresh-schema
                               vocab-flakes <?)]
      (assoc staged-map :db-after db-after))))

(defn stage-flakes
  [flakeset tx-state nodes]
  (go-try
    (loop [[node & r] nodes
           flakes* flakeset]
      (if node
        (let [[_node-sid node-flakes] (<? (json-ld-node->flakes node tx-state nil))]
          (recur r (into flakes* node-flakes)))
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
                (<? (shacl/validate-target root-db shacl all-flakes))))
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
  [{:keys [schema t] :as db} json-ld {:keys [default-ctx] :as tx-state}]
  (log/debug "insert default-ctx:" default-ctx)
  (let [nodes    (-> json-ld
                     (json-ld/expand default-ctx)
                     util/sequential)
        flakeset (cond-> (flake/sorted-set-by flake/cmp-flakes-spot)
                         (init-db? db) (into (base-flakes t)))]
    (stage-flakes flakeset tx-state nodes)))

;; TODO - delete passes the error-ch but doesn't monitor for it at the top level here to properly throw exceptions
(defn delete
  "Executes a delete statement"
  [db max-fuel json-ld {:keys [t] :as _tx-state}]
  (go-try
    (let [{:keys [delete] :as parsed-query}
          (-> json-ld
              syntax/validate
              (q-parse/parse-delete db))

          [s p o] delete
          parsed-query (assoc parsed-query :delete [s p o])
          error-ch     (async/chan)
          flake-ch     (async/chan)
          where-ch     (where/search db parsed-query error-ch)]
      (async/pipeline-async 1
                            flake-ch
                            (fn [solution ch]
                              (let [s* (if (::where/val s)
                                         s
                                         (get solution (::where/var s)))
                                    p* (if (::where/val p)
                                         p
                                         (get solution (::where/var p)))
                                    o* (if (::where/val o)
                                         o
                                         (get solution (::where/var o)))]
                                (async/pipe
                                  (where/resolve-flake-range db error-ch [s* p* o*])
                                  ch)))
                            where-ch)
      (let [delete-ch (async/transduce (comp cat
                                             (map (fn [f]
                                                    (flake/flip-flake f t))))
                                       (completing conj)
                                       (flake/sorted-set-by flake/cmp-flakes-spot)
                                       flake-ch)
            flakes    (async/alt!
                        error-ch ([e]
                                  (throw e))
                        delete-ch ([flakes]
                                   flakes))]
        flakes))))

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
        <?)))

(defn stage
  "Stages changes, but does not commit.
  Returns async channel that will contain updated db or exception."
  [db json-ld opts]
  (go-try
    (let [{tx :subject issuer :issuer} (or (<? (cred/verify json-ld))
                                           {:subject json-ld})
          tx-state (->tx-state db (assoc opts :issuer issuer))
          flakes   (if (and (contains? tx :delete)
                            (contains? tx :where))
                     (<? (delete db util/max-integer tx tx-state))
                     (<? (insert db tx tx-state)))]
      (<? (flakes->final-db tx-state flakes)))))
