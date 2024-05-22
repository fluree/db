(ns fluree.db.db.json-ld
  (:refer-clojure :exclude [load vswap!])
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.where :as where]
            [fluree.db.db.json-ld.format :as jld-format]
            [fluree.db.util.core :as util :refer [get-first get-first-value vswap!]]
            [fluree.db.index :as index]
            [fluree.db.indexer.storage :as index-storage]
            [fluree.db.indexer :as indexer]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.exec.update :as update]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.policy.enforce-tx :as tx-policy]
            [fluree.db.datatype :as datatype]
            [fluree.db.serde.json :as serde-json]
            [fluree.db.query.json-ld.response :as jld-response]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.reify :as reify]
            [fluree.db.json-ld.commit-data :as commit-data]
            [clojure.core.async :as async :refer [go]]
            [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(def data-version 0)

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

;; ================ Jsonld record support fns ================================

(defn root-db
  [this]
  (assoc this :policy root-policy-map))

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db subject-id]
  (go-try
    (let [root (root-db db)]
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
                                                (let [var (where/get-variable s-mch)
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
        db-before (root-db db)]
    {:db-before     db-before
     :context       context
     :txn           txn
     :annotation    annotation
     :author-did    author-did
     :policy        policy
     :stage-update? (= t db-t) ; if a previously staged db is getting updated again before committed
     :t             t
     :reasoner-max  10         ; maximum number of reasoner iterations before exception
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
           sid->s-flakes  {}]
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

          mods (<? (modified-subjects db add))

          db-after  (-> db
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
    (<? (shacl/validate! db-before (root-db db-after) (vals mods) context))
    (let [allowed-db (<? (tx-policy/allowed? staged-map))]
      (root-db allowed-db))))

(defn stage
  [db fuel-tracker context identity annotation raw-txn parsed-txn]
  (go-try
    (let [db*               (if identity
                              (<? (policy/wrap-policy db identity))
                              db)
          tx-state          (->tx-state :db db*
                                        :context context
                                        :txn raw-txn
                                        :author-did (:did identity)
                                        :annotation annotation)
          [db** new-flakes] (<? (generate-flakes db fuel-tracker parsed-txn tx-state))
          updated-db        (<? (final-db db** new-flakes tx-state))]
      (<? (validate-db-update updated-db)))))

;; ================ end Jsonld record support fns ============================

(defrecord JsonLdDb [conn alias branch commit t tt-id stats spot post opst tspo
                     schema comparators staged novelty policy namespaces
                     namespace-codes reindex-min-bytes reindex-max-bytes]
  dbproto/IFlureeDb
  (-rootdb [this] (root-db this))
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


  jld-response/NodeFormatter
  (-forward-properties [db iri spec context compact-fn cache fuel-tracker error-ch]
    (jld-format/forward-properties db iri spec context compact-fn cache fuel-tracker error-ch))

  (-reverse-property [db iri reverse-spec compact-fn cache fuel-tracker error-ch]
    (jld-format/reverse-property db iri reverse-spec compact-fn cache fuel-tracker error-ch))

  indexer/Indexed
  (collect [db changes-ch]
    (if (idx-default/novelty-min? db reindex-min-bytes)
      (idx-default/refresh db changes-ch)
      (go))))

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
     :novelty         (new-novelty-map index/comparators)
     :schema          (vocab/base-schema)}))

(defn load
  ([conn ledger-alias branch commit-jsonld]
   (load conn ledger-alias branch commit-jsonld {}))
  ([conn ledger-alias branch commit-jsonld
    {:keys [reindex-min-bytes reindex-max-bytes]
     :or   {reindex-min-bytes 100000                         ; 100 kb
            reindex-max-bytes 1000000}}]                     ; 1 mb
   (go-try
     (let [commit-map (commit-data/jsonld->clj commit-jsonld)
           root-map   (if-let [{:keys [address]} (:index commit-map)]
                        (<? (index-storage/read-db-root conn address))
                        (genesis-root-map ledger-alias))
           indexed-db (-> root-map
                          (assoc :conn conn
                                 :alias ledger-alias
                                 :branch branch
                                 :commit commit-map
                                 :tt-id nil
                                 :comparators index/comparators
                                 :staged []
                                 :policy root-policy-map
                                 :reindex-min-bytes reindex-min-bytes
                                 :reindex-max-bytes reindex-max-bytes)
                          map->JsonLdDb)
           indexed-db* (if (nil? (:schema root-map)) ;; needed for legacy (v0) root index map
                         (<? (vocab/load-schema indexed-db (:preds root-map)))
                         indexed-db)
           commit-t   (-> commit-jsonld
                          (get-first const/iri-data)
                          (get-first-value const/iri-t))
           index-t    (:t indexed-db*)]
       (if (= commit-t index-t)
         indexed-db*
         (loop [[commit-tuple & r] (<? (reify/trace-commits conn [commit-jsonld nil] (inc index-t)))
                db                 indexed-db*]
           (if commit-tuple
             (let [new-db (<? (reify/merge-commit conn db commit-tuple))]
               (recur r new-db))
             db)))))))

(defn get-s-iri
  "Returns a compact IRI from a subject id (sid)."
  [db sid compact-fn]
  (compact-fn (iri/decode-sid db sid)))

(defn- subject-block-pred
  [db compact-fn list? p-flakes]
  (loop [[p-flake & r] p-flakes
         all-refs? nil
         acc      nil]
    (let [pdt       (flake/dt p-flake)
          ref?      (= const/$xsd:anyURI pdt)
          [obj all-refs?] (if ref?
                            [{"@id" (get-s-iri db (flake/o p-flake) compact-fn)}
                             (if (nil? all-refs?) true all-refs?)]
                            [{"@value" (-> p-flake
                                           flake/o
                                           (serde-json/serialize-object pdt))}
                             false])
          obj*      (cond-> obj
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
         acc            nil]
    (let [fflake           (first p-flakes)
          list?            (-> fflake flake/m :i)
          pid              (flake/p fflake)
          p-iri            (get-s-iri db pid compact-fn)
          [objs all-refs?] (subject-block-pred db compact-fn list?
                                               p-flakes)
          handle-all-refs  (partial set-refs-type-in-ctx ctx p-iri)
          objs*            (cond-> objs
                             ;; next line is for compatibility with json-ld/parse-type's expectations; should maybe revisit
                             (and all-refs? (not list?)) handle-all-refs
                             list?                       handle-list-values
                             (= 1 (count objs))          first)
          acc'         (assoc acc p-iri objs*)]
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
             assert         []
             retract        []]
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

(defn db->jsonld
  "Creates the JSON-LD map containing a new ledger update"
  [{:keys [t commit stats] :as db} {:keys [type-key compact ctx-used-atom id-key] :as commit-opts}]
  (let [prev-dbid   (commit-data/data-id commit)
        {:keys [assert retract refs-ctx]} (generate-commit db commit-opts)
        prev-db-key (compact const/iri-previous)
        assert-key  (compact const/iri-assert)
        retract-key (compact const/iri-retract)
        refs-ctx*   (cond-> refs-ctx
                      prev-dbid (assoc-in [prev-db-key "@type"] "@id")
                      (seq assert) (assoc-in [assert-key "@container"] "@graph")
                      (seq retract) (assoc-in [retract-key "@container"] "@graph"))
        db-json     (cond-> {id-key                nil ;; comes from hash later
                             type-key              [(compact const/iri-DB)]
                             (compact const/iri-t) t
                             (compact const/iri-v) data-version}
                      prev-dbid (assoc prev-db-key prev-dbid)
                      (seq assert) (assoc assert-key assert)
                      (seq retract) (assoc retract-key retract)
                      (:flakes stats) (assoc (compact const/iri-flakes) (:flakes stats))
                      (:size stats) (assoc (compact const/iri-size) (:size stats)))
        ;; TODO - this is re-normalized below, can try to do it just once
        dbid        (commit-data/db-json->db-id db-json)
        db-json*    (-> db-json
                        (assoc id-key dbid)
                        (assoc "@context" (merge-with merge @ctx-used-atom refs-ctx*)))]
    [dbid db-json*]))
