(ns fluree.db.db.json-ld
  (:refer-clojure :exclude [load vswap!])
  (:require [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]]
            [clojure.core.async :as async :refer [go]]
            [clojure.set :refer [map-invert]]
            [fluree.db.connection :as connection]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.db.json-ld.format :as jld-format]
            [fluree.db.db.json-ld.history :as history]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.index :as index]
            [fluree.db.indexer :as indexer]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.query.fql :as fql]
            [fluree.db.indexer.storage :as index-storage]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.json-ld.policy.query :as qpolicy]
            [fluree.db.json-ld.policy.rules :as policy-rules]
            [fluree.db.json-ld.reify :as reify]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.policy.modify :as tx-policy]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.history :refer [AuditLog]]
            [fluree.db.query.json-ld.response :as jld-response]
            [fluree.db.query.range :as query-range]
            [fluree.db.serde.json :as serde-json]
            [fluree.db.time-travel :refer [TimeTravel]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [get-first get-first-value
                                                  get-first-id vswap!]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(def data-version 0)

;; ================ Jsonld record support fns ================================

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db subject-id]
  (go-try
    (let [root (policy/root db)]
      (<? (query-range/index-range root :spot = [subject-id const/$rdf:type]
                                   {:flake-xf (map flake/o)})))))

(defn p-prop
  [schema property predicate]
  (assert (#{:id :iri :subclassOf :parentProps :childProps :datatype}
           property)
          (str "Invalid predicate property: " (pr-str property)))
  (get-in schema [:pred predicate property]))

(defn class-prop
  [{:keys [schema] :as _db} meta-key class]
  (if (= :subclasses meta-key)
    (get @(:subclasses schema) class)
    (p-prop schema meta-key class)))

(defn empty-all-novelty
  [db]
  (let [cleared (reduce (fn [db* idx]
                          (update-in db* [:novelty idx] empty))
                        db index/types)]
    (assoc-in cleared [:novelty :size] 0)))

(defn empty-novelty
  "Empties novelty @ t value and earlier. If t is null, empties all novelty."
  [db t]
  (cond
    (or (nil? t)
        (= t (:t db)))
    (empty-all-novelty db)

    (flake/t-before? t (:t db))
    (let [cleared (reduce (fn [db* idx]
                            (update-in db* [:novelty idx]
                                       (fn [flakes]
                                         (index/flakes-after t flakes))))
                          db index/types)
          size    (flake/size-bytes (get-in cleared [:novelty :spot]))]
      (assoc-in cleared [:novelty :size] size))

    :else
    (throw (ex-info (str "Request to empty novelty at t value: " t
                         ", however provided db is only at t value: " (:t db))
                    {:status 500 :error :db/indexing}))))

(defn force-index-update
  [{:keys [commit] :as db} {data-map :data, :keys [spot post opst tspo] :as commit-index}]
  (let [index-t (:t data-map)
        commit* (assoc commit :index commit-index)]
    (-> db
        (empty-novelty index-t)
        (assoc :commit commit*
               :novelty* (empty-novelty db index-t)
               :spot spot
               :post post
               :opst opst
               :tspo tspo)
        (assoc-in [:stats :indexed] index-t))))

(defn newer-index?
  [commit {data-map :data, :as _commit-index}]
  (if data-map
    (let [commit-index-t (commit-data/index-t commit)
          index-t        (:t data-map)]
      (or (nil? commit-index-t)
          (flake/t-after? index-t commit-index-t)))
    false))

(defn index-update
  "If provided commit-index is newer than db's commit index, updates db by cleaning novelty.
  If it is not newer, returns original db."
  [{:keys [commit] :as db} commit-index]
  (if (newer-index? commit commit-index)
    (force-index-update db commit-index)
    db))

(defn match-id
  [db fuel-tracker solution s-mch error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (partition-by flake/s)
                                       (map first)
                                       (map (fn [f]
                                              (if (where/unmatched-var? s-mch)
                                                (let [var     (where/get-variable s-mch)
                                                      matched (where/match-subject s-mch db f)]
                                                  (assoc solution var matched))
                                                solution)))))
        s-mch*     (where/assign-matched-component s-mch solution)]
    (if-let [s (where/compute-sid db s-mch*)]
      (-> db
          (where/resolve-flake-range fuel-tracker error-ch [s])
          (async/pipe matched-ch))
      (async/close! matched-ch))
    matched-ch))

(defn match-triple
  [db fuel-tracker solution tuple error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (map (fn [flake]
                                              (where/match-flake solution tuple db flake)))))
        db-alias   (:alias db)
        triple     (where/assign-matched-values tuple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [pid (where/get-sid p db)]
        (if-let [props (and pid (where/get-child-properties db pid))]
          (let [prop-ch (-> props (conj pid) async/to-chan!)]
            (async/pipeline-async 2
                                  matched-ch
                                  (fn [prop ch]
                                    (let [p* (where/match-sid p db-alias prop)]
                                      (-> db
                                          (where/resolve-flake-range fuel-tracker error-ch [s p* o])
                                          (async/pipe ch))))
                                  prop-ch))

          (-> db
              (where/resolve-flake-range fuel-tracker error-ch [s p o])
              (async/pipe matched-ch))))
      (async/close! matched-ch))
    matched-ch))

(defn with-distinct-subjects
  "Return a transducer that filters a stream of flakes by removing any flakes with
  subject ids repeated from previously processed flakes."
  []
  (fn [rf]
    (let [seen-sids (volatile! #{})]
      (fn
        ;; Initialization: do nothing but initialize the supplied reducing fn
        ([]
         (rf))

        ;; Iteration: keep track of subject ids seen; only pass flakes with new
        ;; subject ids through to the supplied reducing fn.
        ([result f]
         (let [sid (flake/s f)]
           (if (contains? @seen-sids sid)
             result
             (do (vswap! seen-sids conj sid)
                 (rf result f)))))

        ;; Termination: do nothing but terminate the supplied reducing fn
        ([result]
         (rf result))))))

(defn match-class
  [db fuel-tracker solution triple error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (with-distinct-subjects)
                                       (map (fn [flake]
                                              (where/match-flake solution triple db flake)))))
        db-alias   (:alias db)
        triple     (where/assign-matched-values triple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [cls        (where/get-sid o db)
            sub-obj    (dissoc o ::sids ::iri)
            class-objs (into [o]
                             (comp (map (fn [cls]
                                          (where/match-sid sub-obj db-alias cls)))
                                   (remove nil?))
                             (class-prop db :subclasses cls))
            class-ch   (async/to-chan! class-objs)]
        (async/pipeline-async 2
                              matched-ch
                              (fn [class-obj ch]
                                (-> (where/resolve-flake-range db fuel-tracker error-ch [s p class-obj])
                                    (async/pipe ch)))
                              class-ch))
      (async/close! matched-ch))
    matched-ch))

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

(defn ->tx-state
  "Generates a state map for transaction processing. When optional
  reasoned-from-IRI is provided, will mark any new flakes as reasoned from the
  provided value in the flake's metadata (.-m) as :reasoned key."
  [& {:keys [db context txn author-did annotation reasoned-from-iri]}]
  (let [{:keys [policy], db-t :t} db

        commit-t  (-> db :commit commit-data/t)
        t         (flake/next-t commit-t)
        db-before (policy/root db)]
    {:db-before     db-before
     :context       context
     :txn           txn
     :annotation    annotation
     :author-did    author-did
     :policy        policy
     :stage-update? (= t db-t) ; if a previously staged db is getting updated again before committed
     :t             t
     :reasoner-max  10 ; maximum number of reasoner iterations before exception
     :reasoned      reasoned-from-iri}))

(defn into-flakeset
  [fuel-tracker error-ch flake-ch]
  (let [flakeset (flake/sorted-set-by flake/cmp-flakes-spot)
        error-xf (halt-when util/exception?)
        flake-xf (if fuel-tracker
                   (let [track-fuel (fuel/track fuel-tracker error-ch)]
                     (comp error-xf track-fuel))
                   error-xf)]
    (async/transduce flake-xf (completing conj) flakeset flake-ch)))

(defn reasoned-rule?
  "Returns truthy if the flake has been generated by reasoner"
  [flake]
  (-> flake meta :reasoned))

(defn non-reasoned-flakes
  "Takes a sequence of flakes and removes any flakes which are reasoned.

  This is primarily used to remove reasoned flakes from commits."
  [flakes]
  (remove reasoned-rule? flakes))

(defn reasoned-flakes
  "Takes a sequence of flakes and keeps only reasoned flakes"
  [flakes]
  (filter reasoned-rule? flakes))

(defn generate-flakes
  [db fuel-tracker parsed-txn tx-state]
  (go
    (let [error-ch  (async/chan)
          db-vol    (volatile! db)
          update-ch (->> (where/search db parsed-txn fuel-tracker error-ch)
                         (update/modify db-vol parsed-txn tx-state fuel-tracker error-ch)
                         (into-flakeset fuel-tracker error-ch))]
      (async/alt!
        error-ch ([e] e)
        update-ch ([result]
                   (if (util/exception? result)
                     result
                     [@db-vol result]))))))

(defn modified-subjects
  "Returns a map of sid to s-flakes for each modified subject."
  [db flakes]
  (go-try
    (loop [[s-flakes & r] (partition-by flake/s flakes)
           sid->s-flakes {}]
      (if s-flakes
        (let [sid             (some-> s-flakes first flake/s)
              existing-flakes (<? (query-range/index-range db :spot = [sid]))]
          (recur r (assoc sid->s-flakes sid (into (set s-flakes) existing-flakes))))
        sid->s-flakes))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [db new-flakes {:keys [stage-update? policy t txn author-did annotation db-before context] :as _tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db [:novelty :spot]) new-flakes)
                         [new-flakes nil])

          mods     (<? (modified-subjects (policy/root db) add))

          db-after (-> db
                       (update :staged conj [txn author-did annotation])
                       (assoc :policy policy) ;; re-apply policy to db-after
                       (assoc :t t)
                       (commit-data/update-novelty add remove)
                       (commit-data/add-tt-id)
                       (vocab/hydrate-schema add mods))]
      {:add add :remove remove :db-after db-after :db-before db-before :mods mods :context context})))

(defn validate-db-update
  [{:keys [db-after db-before mods context] :as staged-map}]
  (go-try
    (<? (shacl/validate! db-before (policy/root db-after) (vals mods) context))
    (let [allowed-db (<? (tx-policy/allowed? staged-map))]
      allowed-db)))

(defn stage
  [db fuel-tracker context identity annotation raw-txn parsed-txn]
  (go-try
    (let [tx-state   (->tx-state :db db
                                 :context context
                                 :txn raw-txn
                                 :author-did (:did identity)
                                 :annotation annotation)
          [db** new-flakes] (<? (generate-flakes db fuel-tracker parsed-txn tx-state))
          updated-db (<? (final-db db** new-flakes tx-state))]
      (<? (validate-db-update updated-db)))))

(defn read-db
  [conn db-address]
  (go-try
    (let [file-data (<? (connection/-c-read conn db-address))
          db        (assoc file-data "f:address" db-address)]
      (json-ld/expand db))))

(defn with-namespaces
  [{:keys [namespaces max-namespace-code] :as db} new-namespaces]
  (let [new-ns-map          (into namespaces
                                  (map-indexed (fn [i ns]
                                                 (let [ns-code (+ (inc i)
                                                                  max-namespace-code)]
                                                   [ns ns-code])))
                                  new-namespaces)
        new-ns-codes        (map-invert new-ns-map)
        max-namespace-code* (apply max (vals new-ns-map))]
    (assoc db
           :namespaces new-ns-map
           :namespace-codes new-ns-codes
           :max-namespace-code max-namespace-code*)))

(defn enrich-values
  [id->node values]
  (mapv (fn [{:keys [id type] :as v-map}]
          (if id
            (merge (get id->node id)
                   (cond-> v-map
                     (nil? type) (dissoc :type)))
            v-map))
        values))

(defn enrich-node
  [id->node node]
  (reduce-kv
   (fn [updated-node k v]
     (assoc updated-node k (cond (= :id k) v
                                 (:list (first v)) [{:list (enrich-values id->node (:list (first v)))}]
                                 :else (enrich-values id->node v))))
   {}
   node))

(defn enrich-assertion-values
  "`asserts` is a json-ld flattened (ish) sequence of nodes. In order to properly generate
  sids (or pids) for these nodes, we need the full node additional context for ref objects. This
  function traverses the asserts and builds a map of node-id->node, then traverses the
  asserts again and merges each ref object into the ref's node.

  example input:
  [{:id \"foo:bar\"
    \"ex:key1\" {:id \"foo:ref-id\"}}
  {:id \"foo:ref-id\"
   :type \"some:type\"}]

  example output:
  [{:id \"foo:bar\"
    \"ex:key1\" {:id \"foo:ref-id\"
                 :type \"some:type\"}}
  {:id \"foo:ref-id\"
   :type \"some:type\"}]
  "
  [asserts]
  (let [id->node (reduce (fn [id->node {:keys [id] :as node}] (assoc id->node id node))
                         {}
                         asserts)]
    (mapv (partial enrich-node id->node)
          asserts)))

(defn db-assert
  [db-data]
  (let [commit-assert (get db-data const/iri-assert)]
    ;; TODO - any basic validation required
    (enrich-assertion-values commit-assert)))

(defn db-retract
  [db-data]
  (let [commit-retract (get db-data const/iri-retract)]
    ;; TODO - any basic validation required
    commit-retract))

(defn commit-error
  [message commit-data]
  (throw
   (ex-info message
            {:status 400, :error :db/invalid-commit, :commit commit-data})))

(defn db-t
  "Returns 't' value from commit data."
  [db-data]
  (let [t (get-first-value db-data const/iri-t)]
    (when-not (pos-int? t)
      (commit-error
       (str "Invalid, or non existent 't' value inside commit: " t) db-data))
    t))

(defn add-list-meta
  [list-val]
  (let [m {:i (-> list-val :idx last)}]
    (assoc list-val ::meta m)))

(defn list-value?
  "returns true if json-ld value is a list object."
  [v]
  (and (map? v)
       (= :list (-> v first key))))

(defn node?
  "Returns true if a nested value is itself another node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (cond
    (contains? mapx :value)
    false

    (list-value? mapx)
    false

    (and
     (contains? mapx :set)
     (= #{:set :idx} (set (keys mapx))))
    false

    :else
    true))

(defn assert-value-map
  [db sid pid t v-map]
  (let [ref-id (:id v-map)
        meta   (::meta v-map)]
    (if (and ref-id (node? v-map))
      (let [ref-sid (iri/encode-iri db ref-id)]
        (flake/create sid pid ref-sid const/$xsd:anyURI t true meta))
      (let [[value dt] (datatype/from-expanded v-map nil)]
        (flake/create sid pid value dt t true meta)))))

(defn assert-property
  [db sid pid t value]
  (let [v-maps (util/sequential value)]
    (mapcat (fn [v-map]
              (if (list-value? v-map)
                (let [list-vals (:list v-map)]
                  (into []
                        (comp (map add-list-meta)
                              (map (partial assert-value-map db sid pid t)))
                        list-vals))
                [(assert-value-map db sid pid t v-map)]))
            v-maps)))

(defn- get-type-assertions
  [db t sid type]
  (if type
    (loop [[type-item & r] type
           acc []]
      (if type-item
        (let [type-id (iri/encode-iri db type-item)]
          (recur r (conj acc (flake/create sid const/$rdf:type type-id const/$xsd:anyURI t true nil))))
        acc))
    []))

(defn assert-node
  [db t node]
  (log/trace "assert-node:" node)
  (let [{:keys [id type]} node
        sid             (iri/encode-iri db id)
        type-assertions (if (seq type)
                          (get-type-assertions db t sid type)
                          [])]
    (into type-assertions
          (comp (filter (fn [node-entry]
                          (not (-> node-entry key keyword?))))
                (mapcat (fn [[prop value]]
                          (let [pid (iri/encode-iri db prop)]
                            (assert-property db sid pid t value)))))
          node)))

(defn assert-flakes
  [db t assertions]
  (into []
        (mapcat (partial assert-node db t))
        assertions))

(defn merge-flakes
  "Returns updated db with merged flakes."
  [db t flakes]
  (-> db
      (assoc :t t)
      (commit-data/update-novelty flakes)
      (vocab/hydrate-schema flakes)))

(defn retract-value-map
  [db sid pid t v-map]
  (let [ref-id (:id v-map)]
    (if (and ref-id (node? v-map))
      (let [ref-sid (iri/encode-iri db ref-id)]
        (flake/create sid pid ref-sid const/$xsd:anyURI t false nil))
      (let [[value dt] (datatype/from-expanded v-map nil)]
        (flake/create sid pid value dt t false nil)))))

(defn- get-type-retractions
  [db t sid type]
  (into []
        (map (fn [type-item]
               (let [type-sid (iri/encode-iri db type-item)]
                 (flake/create sid const/$rdf:type type-sid
                               const/$xsd:anyURI t false nil))))
        type))

(defn- retract-node*
  [db t {:keys [sid type-retractions] :as _retract-state} node]
  (loop [[[k v-maps] & r] node
         acc type-retractions]
    (if k
      (if (keyword? k)
        (recur r acc)
        (let [pid  (or (iri/encode-iri db k)
                       (throw (ex-info (str "Retraction on a property that does not exist: " k)
                                       {:status 400
                                        :error  :db/invalid-commit})))
              acc* (into acc
                         (map (partial retract-value-map db sid pid t))
                         (util/sequential v-maps))]
          (recur r acc*)))
      acc)))

(defn retract-node
  [db t node]
  (let [{:keys [id type]} node
        sid              (or (iri/encode-iri db id)
                             (throw (ex-info (str "Retractions specifies an IRI that does not exist: " id
                                                  " at db t value: " t ".")
                                             {:status 400 :error
                                              :db/invalid-commit})))
        retract-state    {:sid sid}
        type-retractions (if (seq type)
                           (get-type-retractions db t sid type)
                           [])
        retract-state*   (assoc retract-state :type-retractions type-retractions)]
    (retract-node* db t retract-state* node)))

(defn retract-flakes
  [db t retractions]
  (into []
        (mapcat (partial retract-node db t))
        retractions))

(defn merge-commit
  "Process a new commit map, converts commit into flakes, updates respective
  indexes and returns updated db"
  [conn db [commit _proof]]
  (go-try
    (let [db-address         (-> commit
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-address))
          db-data            (<? (read-db conn db-address))
          t-new              (db-t db-data)
          assert             (db-assert db-data)
          nses               (map :value
                                  (get db-data const/iri-namespaces))
          _                  (log/debug "merge-commit new namespaces:" nses)
          _                  (log/debug "db max-namespace-code:"
                                        (:max-namespace-code db))
          db*                (with-namespaces db nses)
          asserted-flakes    (assert-flakes db* t-new assert)
          retract            (db-retract db-data)
          retracted-flakes   (retract-flakes db* t-new retract)

          {:keys [previous issuer message data] :as commit-metadata}
          (commit-data/json-ld->map commit db*)

          commit-id          (:id commit-metadata)
          commit-sid         (iri/encode-iri db* commit-id)
          [prev-commit _] (some->> previous :address (reify/read-commit conn) <?)
          db-sid             (iri/encode-iri db* (:id data))
          metadata-flakes    (commit-data/commit-metadata-flakes commit-metadata
                                                                 t-new commit-sid db-sid)
          previous-id        (when prev-commit (:id prev-commit))
          prev-commit-flakes (when previous-id
                               (commit-data/prev-commit-flakes db* t-new commit-sid
                                                               previous-id))
          prev-data-id       (get-first-id prev-commit const/iri-data)
          prev-db-flakes     (when prev-data-id
                               (commit-data/prev-data-flakes db* db-sid t-new
                                                             prev-data-id))
          issuer-flakes      (when-let [issuer-iri (:id issuer)]
                               (commit-data/issuer-flakes db* t-new commit-sid issuer-iri))
          message-flakes     (when message
                               (commit-data/message-flakes t-new commit-sid message))
          all-flakes         (-> db*
                                 (get-in [:novelty :spot])
                                 empty
                                 (into metadata-flakes)
                                 (into retracted-flakes)
                                 (into asserted-flakes)
                                 (cond-> prev-commit-flakes (into prev-commit-flakes)
                                         prev-db-flakes (into prev-db-flakes)
                                         issuer-flakes (into issuer-flakes)
                                         message-flakes (into message-flakes)))]
      (when (empty? all-flakes)
        (commit-error "Commit has neither assertions or retractions!"
                      commit-metadata))
      (-> db*
          (merge-flakes t-new all-flakes)
          (assoc :commit commit-metadata)))))

;; ================ end Jsonld record support fns ============================

(defrecord JsonLdDb [conn alias branch commit t tt-id stats spot post opst tspo
                     schema comparators staged novelty policy namespaces
                     namespace-codes max-namespace-code reindex-min-bytes
                     reindex-max-bytes]
  dbproto/IFlureeDb
  (-query [this query-map] (fql/query this query-map))
  (-p-prop [_ meta-key property] (p-prop schema meta-key property))
  (-class-ids [this subject] (class-ids this subject))
  (-index-update [db commit-index] (index-update db commit-index))

  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes))

  where/Matcher
  (-match-id [db fuel-tracker solution s-mch error-ch]
    (match-id db fuel-tracker solution s-mch error-ch))

  (-match-triple [db fuel-tracker solution s-mch error-ch]
    (match-triple db fuel-tracker solution s-mch error-ch))

  (-match-class [db fuel-tracker solution s-mch error-ch]
    (match-class db fuel-tracker solution s-mch error-ch))

  jld-transact/Transactable
  (-stage-txn [db fuel-tracker context identity annotation raw-txn parsed-txn]
    (stage db fuel-tracker context identity annotation raw-txn parsed-txn))
  (-merge-commit [db new-commit proof] (merge-commit conn db [new-commit proof]))
  (-merge-commit [db new-commit] (merge-commit conn db [new-commit]))

  jld-response/NodeFormatter
  (-forward-properties [db iri spec context compact-fn cache fuel-tracker error-ch]
    (jld-format/forward-properties db iri spec context compact-fn cache fuel-tracker error-ch))

  (-reverse-property [db iri reverse-spec compact-fn cache fuel-tracker error-ch]
    (jld-format/reverse-property db iri reverse-spec compact-fn cache fuel-tracker error-ch))

  (-iri-visible? [db iri]
    (let [sid (iri/encode-iri db iri)]
      (qpolicy/allow-iri? db sid)))

  indexer/Indexable
  (index [db changes-ch]
    (if (idx-default/novelty-min? db reindex-min-bytes)
      (idx-default/refresh db changes-ch)
      (go)))

  TimeTravel
  (datetime->t [db datetime]
    (go-try
      (log/debug "datetime->t db:" (pr-str db))
      (let [epoch-datetime (util/str->epoch-ms datetime)
            current-time   (util/current-time-millis)
            [start end] (if (< epoch-datetime current-time)
                          [epoch-datetime current-time]
                          [current-time epoch-datetime])
            flakes         (-> db
                               policy/root
                               (query-range/index-range
                                :post
                                > [const/$_commit:time start]
                                < [const/$_commit:time end])
                               <?)]
        (log/debug "datetime->t index-range:" (pr-str flakes))
        (if (empty? flakes)
          (:t db)
          (let [t (-> flakes first flake/t flake/prev-t)]
            (if (zero? t)
              (throw (ex-info (str "There is no data as of " datetime)
                              {:status 400, :error :db/invalid-query}))
              t))))))

  (latest-t [_]
    t)

  (-as-of [db t]
    (assoc db :t t))

  AuditLog
  (-history [db context from-t to-t commit-details? error-ch history-q]
    (history/query-history db context from-t to-t commit-details? error-ch history-q))
  (-commits [db context from-t to-t error-ch]
    (history/query-commits db context from-t to-t error-ch))

  policy/Restrictable
  (wrap-policy [db policy default-allow? values-map]
    (policy-rules/wrap-policy db policy default-allow? values-map))
  (wrap-identity-policy [db identity default-allow? values-map]
    (policy-rules/wrap-identity-policy db identity default-allow? values-map))
  (root [db]
    (policy/root-db db)))

(defn db?
  [x]
  (instance? JsonLdDb x))

(def ^String label "#fluree/JsonLdDb ")

(defn display
  [db]
  (select-keys db [:alias :branch :t :stats :policy]))

#?(:cljs
   (extend-type JsonLdDb
     IPrintWithWriter
     (-pr-writer [db w _opts]
       (-write w label)
       (-write w (-> db display pr)))))

#?(:clj
   (defmethod print-method JsonLdDb [^JsonLdDb db, ^Writer w]
     (.write w label)
     (binding [*out* w]
       (-> db display pr))))

(defmethod pprint/simple-dispatch JsonLdDb
  [db]
  (print label)
  (-> db display pprint))

(defn new-novelty-map
  [comparators]
  (reduce
   (fn [m idx]
     (assoc m idx (-> comparators
                      (get idx)
                      flake/sorted-set-by)))
   {:size 0} index/types))

(defn genesis-root-map
  [ledger-alias]
  (let [{spot-cmp :spot, post-cmp :post, opst-cmp :opst, tspo-cmp :tspo}
        index/comparators]
    {:t               0
     :spot            (index/empty-branch ledger-alias spot-cmp)
     :post            (index/empty-branch ledger-alias post-cmp)
     :opst            (index/empty-branch ledger-alias opst-cmp)
     :tspo            (index/empty-branch ledger-alias tspo-cmp)
     :stats           {:flakes 0, :size 0, :indexed 0}
     :namespaces      iri/default-namespaces
     :namespace-codes iri/default-namespace-codes
     :schema          (vocab/base-schema)}))

(defn get-max-ns-code
  [ns-codes]
  (->> ns-codes keys (apply max)))

(defn load-novelty
  [conn indexed-db index-t commit-jsonld]
  (go-try
    (loop [[commit-tuple & r] (<? (reify/trace-commits conn [commit-jsonld nil] (inc index-t)))
           db indexed-db]
      (if commit-tuple
        (let [new-db (<? (merge-commit conn db commit-tuple))]
          (recur r new-db))
        db))))

(defn load
  ([conn ledger-alias branch commit-pair]
   (load conn ledger-alias branch commit-pair {}))
  ([conn ledger-alias branch [commit-jsonld commit-map]
    {:keys [reindex-min-bytes reindex-max-bytes]
     :or   {reindex-min-bytes 100000 ; 100 kb
            reindex-max-bytes 1000000}}] ; 1 mb
   (go-try
     (let [root-map    (if-let [{:keys [address]} (:index commit-map)]
                         (<? (index-storage/read-db-root conn address))
                         (genesis-root-map ledger-alias))
           max-ns-code (max iri/last-default-code
                            (-> root-map :namespace-codes get-max-ns-code))
           indexed-db  (-> root-map
                           (assoc :conn conn
                                  :alias ledger-alias
                                  :branch branch
                                  :commit commit-map
                                  :tt-id nil
                                  :comparators index/comparators
                                  :staged []
                                  :novelty (new-novelty-map index/comparators)
                                  :max-namespace-code max-ns-code
                                  :reindex-min-bytes reindex-min-bytes
                                  :reindex-max-bytes reindex-max-bytes)
                           map->JsonLdDb
                           policy/root)
           indexed-db* (if (nil? (:schema root-map)) ;; needed for legacy (v0) root index map
                         (<? (vocab/load-schema indexed-db (:preds root-map)))
                         indexed-db)
           commit-t    (-> commit-jsonld
                           (get-first const/iri-data)
                           (get-first-value const/iri-t))
           index-t     (:t indexed-db*)]
       (if (= commit-t index-t)
         indexed-db*
         (<? (load-novelty conn indexed-db* index-t commit-jsonld)))))))

(defn get-s-iri
  "Returns a compact IRI from a subject id (sid)."
  [db sid compact-fn]
  (compact-fn (iri/decode-sid db sid)))

(defn- subject-block-pred
  [db compact-fn list? p-flakes]
  (loop [[p-flake & r] p-flakes
         all-refs? nil
         acc       nil]
    (let [pdt  (flake/dt p-flake)
          ref? (= const/$xsd:anyURI pdt)
          [obj all-refs?] (if ref?
                            [{"@id" (get-s-iri db (flake/o p-flake) compact-fn)}
                             (if (nil? all-refs?) true all-refs?)]
                            [{"@value" (-> p-flake
                                           flake/o
                                           (serde-json/serialize-object pdt))}
                             false])
          obj* (cond-> obj
                 list? (assoc :i (-> p-flake flake/m :i))

                  ;; need to retain the `@type` for times so they will be
                  ;; coerced correctly when loading
                 (datatype/time-type? pdt)
                 (assoc "@type" (get-s-iri db pdt compact-fn)))
          acc' (conj acc obj*)]
      (if (seq r)
        (recur r all-refs? acc')
        [acc' all-refs?]))))

(defn- set-refs-type-in-ctx
  [^clojure.lang.Volatile ctx p-iri refs]
  (vswap! ctx assoc-in [p-iri "@type"] "@id")
  (map #(get % "@id") refs))

(defn- handle-list-values
  [objs]
  {"@list" (->> objs (sort-by :i) (map #(dissoc % :i)))})

(defn- subject-block
  [s-flakes db ^clojure.lang.Volatile ctx compact-fn]
  (loop [[p-flakes & r] (partition-by flake/p s-flakes)
         acc nil]
    (let [fflake          (first p-flakes)
          list?           (-> fflake flake/m :i)
          pid             (flake/p fflake)
          p-iri           (get-s-iri db pid compact-fn)
          [objs all-refs?] (subject-block-pred db compact-fn list?
                                               p-flakes)
          handle-all-refs (partial set-refs-type-in-ctx ctx p-iri)
          objs*           (cond-> objs
                                 ;; next line is for compatibility with json-ld/parse-type's expectations; should maybe revisit
                            (and all-refs? (not list?)) handle-all-refs
                            list? handle-list-values
                            (= 1 (count objs)) first)
          acc'            (assoc acc p-iri objs*)]
      (if (seq r)
        (recur r acc')
        acc'))))

(defn commit-flakes
  "Returns commit flakes from novelty based on 't' value."
  [{:keys [novelty t] :as _db}]
  (-> novelty
      :tspo
      (flake/match-tspo t)
      not-empty))

(defn generate-commit
  "Generates assertion and retraction flakes for a given set of flakes
  which is assumed to be for a single (t) transaction.

  Returns a map of
  :assert - assertion flakes
  :retract - retraction flakes
  :refs-ctx - context that must be included with final context, for refs (@id) values
  :flakes - all considered flakes, for any downstream processes that need it"
  [{:keys [reasoner] :as db} {:keys [compact-fn id-key type-key] :as _opts}]
  (when-let [flakes (cond-> (commit-flakes db)
                      reasoner non-reasoned-flakes)]
    (log/trace "generate-commit flakes:" flakes)
    (let [ctx (volatile! {})]
      (loop [[s-flakes & r] (partition-by flake/s flakes)
             assert  []
             retract []]
        (if s-flakes
          (let [sid   (flake/s (first s-flakes))
                s-iri (get-s-iri db sid compact-fn)
                [assert* retract*]
                (if (and (= 1 (count s-flakes))
                         (= const/$rdfs:Class (->> s-flakes first flake/o))
                         (= const/$rdf:type (->> s-flakes first flake/p)))
                  ;; we don't output auto-generated rdfs:Class definitions for classes
                  ;; (they are implied when used in rdf:type statements)
                  [assert retract]
                  (let [{assert-flakes  true
                         retract-flakes false}
                        (group-by flake/op s-flakes)

                        s-assert  (when assert-flakes
                                    (-> (subject-block assert-flakes db ctx compact-fn)
                                        (assoc id-key s-iri)))
                        s-retract (when retract-flakes
                                    (-> (subject-block retract-flakes db ctx compact-fn)
                                        (assoc id-key s-iri)))]
                    [(cond-> assert
                       s-assert (conj s-assert))
                     (cond-> retract
                       s-retract (conj s-retract))]))]
            (recur r assert* retract*))
          {:refs-ctx (dissoc @ctx type-key) ; @type will be marked as @type: @id, which is implied
           :assert   assert
           :retract  retract
           :flakes   flakes})))))

(defn new-namespaces
  [{:keys [max-namespace-code namespace-codes] :as _db}]
  (->> namespace-codes
       (filter (fn [[k _v]]
                 (> k max-namespace-code)))
       (sort-by key)
       (mapv val)))

(defn db->jsonld
  "Creates the JSON-LD map containing a new ledger update"
  [{:keys [t commit stats staged max-namespace-code] :as db}
   {:keys [type-key compact ctx-used-atom id-key] :as commit-opts}]
  (let [prev-dbid   (commit-data/data-id commit)

        {:keys [assert retract refs-ctx]}
        (generate-commit db commit-opts)

        prev-db-key (compact const/iri-previous)
        assert-key  (compact const/iri-assert)
        retract-key (compact const/iri-retract)
        refs-ctx*   (cond-> refs-ctx
                      prev-dbid (assoc-in [prev-db-key "@type"] "@id")
                      (seq assert) (assoc-in [assert-key "@container"] "@graph")
                      (seq retract) (assoc-in [retract-key "@container"] "@graph"))
        nses        (new-namespaces db)
        db-json     (cond-> {id-key                nil ;; comes from hash later
                             type-key              [(compact const/iri-DB)]
                             (compact const/iri-t) t
                             (compact const/iri-v) data-version}
                      prev-dbid (assoc prev-db-key prev-dbid)
                      (seq assert) (assoc assert-key assert)
                      (seq retract) (assoc retract-key retract)
                      (seq nses) (assoc (compact const/iri-namespaces) nses)
                      (:flakes stats) (assoc (compact const/iri-flakes) (:flakes stats))
                      (:size stats) (assoc (compact const/iri-size) (:size stats)))
        ;; TODO - this is re-normalized below, can try to do it just once
        dbid        (commit-data/db-json->db-id db-json)
        db-json*    (-> db-json
                        (assoc id-key dbid)
                        (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
    {:dbid        dbid
     :db-jsonld   db-json*
     :staged-txns staged}))
