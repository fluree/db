(ns fluree.db.flake.flake-db
  (:refer-clojure :exclude [load vswap!])
  (:require [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [clojure.set :refer [map-invert]]
            [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.format :as jld-format]
            [fluree.db.flake.history :as history]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.novelty :as novelty]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.flake.match :as match]
            [fluree.db.flake.reasoner :as flake.reasoner]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.json-ld.policy.query :as qpolicy]
            [fluree.db.json-ld.policy.rules :as policy-rules]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.history :refer [AuditLog]]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.query.range :as query-range]
            [fluree.db.reasoner :as reasoner]
            [fluree.db.time-travel :refer [TimeTravel]]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util :refer [try* catch* get-id get-types get-list
                                             get-first get-first-value]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.db.util.reasoner :as reasoner-util]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defn empty-all-novelty
  [db]
  (let [cleared (reduce (fn [db* idx]
                          (update-in db* [:novelty idx] empty))
                        db index/types)]
    (assoc-in cleared [:novelty :size] 0)))

(defn novelty-after-t
  "Returns novelty after t value for provided index."
  [db t idx]
  (index/filter-after t (get-in db [:novelty idx])))

(defn empty-novelty
  "Empties novelty @ t value and earlier. If t is null, empties all novelty."
  [db t]
  (cond
    (or (nil? t)
        (= t (:t db)))
    (empty-all-novelty db)

    (flake/t-before? t (:t db))
    (let [novelty (reduce (fn [acc idx]
                            (assoc acc idx
                                   #?(:clj  (future (novelty-after-t db t idx))
                                      :cljs (novelty-after-t db t idx))))
                          {} index/types)
          size    (flake/size-bytes #?(:clj  @(:spot novelty)
                                       :cljs (:spot novelty)))
          db*     (reduce
                   (fn [db* idx]
                     (assoc-in db* [:novelty idx] #?(:clj  @(get novelty idx)
                                                     :cljs (get novelty idx))))
                   (assoc-in db [:novelty :size] size)
                   index/types)]
      db*)

    :else
    (throw (ex-info (str "Request to empty novelty at t value: " t
                         ", however provided db is only at t value: " (:t db))
                    {:status 500 :error :db/indexing}))))

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
  [{:keys [commit] :as db} {data-map :data, :keys [spot psot post opst tspo] :as index-map}]
  (if (newer-index? commit index-map)
    (let [index-t (:t data-map)
          commit* (assoc commit :index index-map)]
      (-> db
          (empty-novelty index-t)
          (assoc :commit commit*
                 :spot spot
                 :psot psot
                 :post post
                 :opst opst
                 :tspo tspo)
          (assoc-in [:stats :indexed] index-t)))
    db))

(defn with-namespaces
  [{:keys [namespaces max-namespace-code] :as db} new-namespaces]
  (let [new-ns-map          (into namespaces
                                  (map-indexed (fn [i ns]
                                                 (let [ns-code (+ (inc i)
                                                                  max-namespace-code)]
                                                   [ns ns-code])))
                                  new-namespaces)
        new-ns-codes        (map-invert new-ns-map)
        max-namespace-code* (iri/get-max-namespace-code new-ns-codes)]
    (assoc db
           :namespaces new-ns-map
           :namespace-codes new-ns-codes
           :max-namespace-code max-namespace-code*)))

(defn db-assert
  [db-data]
  (get db-data const/iri-assert))

(defn db-retract
  [db-data]
  (get db-data const/iri-retract))

(defn commit-error
  [message commit-data]
  (throw
   (ex-info message
            {:status 400, :error :db/invalid-commit, :commit commit-data})))

(defn db-t
  "Returns 't' value from commit data."
  [db-data]
  (let [t (get-first-value db-data const/iri-fluree-t)]
    (when-not (pos-int? t)
      (commit-error
       (str "Invalid, or non existent 't' value inside commit: " t) db-data))
    t))

(defn add-list-meta
  [idx list-val]
  (let [m {:i idx}]
    (assoc list-val ::meta m)))

(defn list-value?
  "returns true if json-ld value is a list object."
  [v]
  (and (map? v)
       (= const/iri-list (-> v first key))))

(defn subject-node?
  "Returns true if a nested value is itself another subject node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (cond
    (contains? mapx const/iri-value)
    false

    (list-value? mapx)
    false

    (and
     (contains? mapx const/iri-set)
     (= 1 (count mapx)))
    false

    :else
    true))

(defn value-map->flake
  [assert? db sid pid t v-map]
  (let [ref-id (get-id v-map)
        meta   (::meta v-map)]
    (if (and ref-id (subject-node? v-map))
      (let [ref-sid (iri/encode-iri db ref-id)]
        (flake/create sid pid ref-sid const/$id t assert? meta))
      (let [[value dt] (datatype/from-expanded db v-map)]
        (flake/create sid pid value dt t assert? meta)))))

(defn property->flake
  [assert? db sid pid t value]
  (let [v-maps (util/sequential value)]
    (mapcat (fn [v-map]
              (if (list-value? v-map)
                (let [list-vals (get-list v-map)]
                  (into []
                        (comp (map-indexed add-list-meta)
                              (map (partial value-map->flake assert? db sid pid t)))
                        list-vals))
                [(value-map->flake assert? db sid pid t v-map)]))
            v-maps)))

(defn- get-type-flakes
  [assert? db t sid types]
  (into []
        (map (fn [type-item]
               (let [type-sid (iri/encode-iri db type-item)]
                 (flake/create sid const/$rdf:type type-sid
                               const/$id t assert? nil))))
        types))

(defn node->flakes
  [assert? db t node]
  (log/trace "node->flakes:" node "assert?" assert?)
  (let [id              (get-id node)
        types           (get-types node)
        sid             (if assert?
                          (iri/encode-iri db id)
                          (or (iri/encode-iri db id)
                              (throw
                               (ex-info
                                "Cannot retract subject IRI with unknown namespace."
                                {:status 400
                                 :error  :db/invalid-retraction
                                 :iri    id}))))
        type-assertions (if (seq types)
                          (get-type-flakes assert? db t sid types)
                          [])]
    (into type-assertions
          (comp (remove #(-> % key #{const/iri-id const/iri-type}))
                (mapcat
                 (fn [[prop value]]
                   (let [pid (if assert?
                               (iri/encode-iri db prop)
                               (or (iri/encode-iri db prop)
                                   (throw
                                    (ex-info
                                     "Cannot retract property IRI with unknown namespace."
                                     {:status 400
                                      :error  :db/invalid-retraction
                                      :iri    prop}))))]
                     (property->flake assert? db sid pid t value)))))
          node)))

(defn create-flakes
  [assert? db t assertions]
  (into []
        (mapcat (partial node->flakes assert? db t))
        assertions))

(defn merge-flakes
  "Returns updated db with merged flakes."
  [db t flakes]
  (-> db
      (assoc :t t)
      (commit-data/update-novelty flakes)
      (vocab/hydrate-schema flakes)))

(defn merge-commit
  "Process a new commit map, converts commit into flakes, updates respective
  indexes and returns updated db"
  [db commit-jsonld commit-data-jsonld]
  (go-try
    (let [t-new            (db-t commit-data-jsonld)
          nses             (map util/get-value
                                (get commit-data-jsonld const/iri-namespaces))
          db*              (with-namespaces db nses)
          asserted-flakes  (->> (db-assert commit-data-jsonld)
                                (create-flakes true db* t-new))
          retracted-flakes (->> (db-retract commit-data-jsonld)
                                (create-flakes false db* t-new))
          commit-metadata  (commit-data/json-ld->map commit-jsonld db*)
          metadata-flakes  (commit-data/commit-metadata-flakes db* t-new commit-metadata)
          all-flakes       (-> db*
                               (get-in [:novelty :spot])
                               empty
                               (into metadata-flakes)
                               (into retracted-flakes)
                               (into asserted-flakes))]

      (when (empty? all-flakes)
        (commit-error "Commit has neither assertions or retractions!"
                      commit-metadata))
      (log/debug "Updating db" (str/join "@" [(:alias db) (:t db)])
                 "to t:" t-new "with new commit:" commit-metadata)
      (-> db*
          (merge-flakes t-new all-flakes)
          (assoc :commit commit-metadata)))))

(defn- component->user-value
  "Convert an internal pattern component to user-readable format"
  [component compact-fn]
  (cond
    (nil? component)
    nil

    (where/unmatched-var? component)
    (str (where/get-variable component))

    (where/matched-iri? component)
    (let [iri (where/get-iri component)]
      (json-ld/compact iri compact-fn))

    (where/matched-value? component)
    (where/get-value component)

    :else
    (throw (ex-info (str "Unexpected component type: " (pr-str component))
                    {:component component}))))

(defn- pattern->user-format
  "Convert internal pattern to user-readable triple format"
  [pattern compact-fn]
  (let [ptype (where/pattern-type pattern)
        pdata (where/pattern-data pattern)]
    (case ptype
      :class
      (let [[s _ o] pdata]
        {:subject (component->user-value s compact-fn)
         :property const/iri-type
         :object (component->user-value o compact-fn)})

      :tuple
      (let [[s p o] pdata]
        {:subject (component->user-value s compact-fn)
         :property (component->user-value p compact-fn)
         :object (component->user-value o compact-fn)})

      :id
      {:subject (component->user-value pdata compact-fn)}

      ;; Other pattern types (filter, bind, etc.)
      {:type ptype
       :data (pr-str pdata)})))

(defn- pattern-type->user-type
  "Convert internal pattern type to user-friendly type name"
  [ptype]
  (case ptype
    :tuple :triple
    ptype))

(defn- pattern-explain
  "Generate explain information for a single pattern"
  [db stats pattern compact-fn]
  (let [ptype       (where/pattern-type pattern)
        optimizable? (optimize/optimizable-pattern? pattern)
        selectivity (optimize/calculate-selectivity db stats pattern)]
    {:type        (pattern-type->user-type ptype)
     :pattern     (pattern->user-format pattern compact-fn)
     :selectivity selectivity
     :optimizable (when optimizable? (pattern-type->user-type optimizable?))}))

(defn- segment-explain
  "Generate explain information for pattern segments"
  [db stats where-clause compact-fn]
  (let [segments (optimize/split-by-optimization-boundaries where-clause)]
    (mapv (fn [segment]
            (if (= :optimizable (:type segment))
              {:type     :optimizable
               :patterns (mapv #(pattern-explain db stats % compact-fn) (:data segment))}
              {:type    :boundary
               :pattern (pattern-explain db stats (:data segment) compact-fn)}))
          segments)))

(defn optimize-query
  "Optimize a parsed query using statistics if available.
   Returns the optimized query with patterns reordered for optimal execution."
  [db parsed-query]
  (let [stats (:stats db)]
    (if (and stats (not-empty stats) (:where parsed-query))
      (let [optimized-where (optimize/optimize-patterns db (:where parsed-query))]
        (assoc parsed-query :where optimized-where))
      parsed-query)))

(defn explain-query
  "Generate an execution plan for the query showing optimization details.
   Returns a query plan map with optimization information."
  [db parsed-query]
  (let [stats            (:stats db)
        has-stat-counts? (and stats
                              (or (seq (:properties stats))
                                  (seq (:classes stats))))
        context          (:context parsed-query)
        compact-fn       (json-ld/compact-fn context)]
    (if-not has-stat-counts?
      {:query parsed-query
       :plan  {:optimization :none
               :reason       "No statistics available"
               :where-clause (:where parsed-query)}}
      (let [where-clause      (:where parsed-query)
            optimized-where   (when where-clause
                                (optimize/optimize-patterns db where-clause))
            original-explain  (when where-clause
                                (mapv #(pattern-explain db stats % compact-fn) where-clause))
            optimized-explain (when optimized-where
                                (mapv #(pattern-explain db stats % compact-fn) optimized-where))
            segments          (when where-clause
                                (segment-explain db stats where-clause compact-fn))
            changed?          (not= where-clause optimized-where)]
        {:query (assoc parsed-query :where optimized-where)
         :plan  {:optimization (if changed? :reordered :unchanged)
                 :statistics   {:property-counts (count (:properties stats))
                                :class-counts    (count (:classes stats))
                                :total-flakes    (:flakes stats)
                                :indexed-at-t    (:indexed stats)}
                 :original     original-explain
                 :optimized    optimized-explain
                 :segments     segments
                 :changed?     changed?}}))))

(defrecord FlakeDB [index-catalog commit-catalog alias commit t tt-id stats
                    spot post opst tspo vg schema comparators staged novelty policy
                    namespaces namespace-codes max-namespace-code
                    reindex-min-bytes reindex-max-bytes max-old-indexes]
  dbproto/IFlureeDb
  (-query [this tracker query-map] (fql/query this tracker query-map))
  (-class-ids [this tracker subject] (match/class-ids this tracker subject))
  (-index-update [db commit-index] (index-update db commit-index))

  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes))

  where/Matcher
  (-match-id [db tracker solution s-mch error-ch]
    (match/match-id db tracker solution s-mch error-ch))

  (-match-triple [db tracker solution triple-mch error-ch]
    (match/match-triple db tracker solution triple-mch error-ch))

  (-match-class [db tracker solution class-mch error-ch]
    (match/match-class db tracker solution class-mch error-ch))

  (-activate-alias [db alias']
    (go-try
      (when (= alias alias')
        db)))

  (-aliases [_]
    [alias])

  (-finalize [_ _ _ solution-ch]
    solution-ch)

  transact/Transactable
  (-stage-txn [db tracker context identity author annotation raw-txn parsed-txn]
    (flake.transact/stage db tracker context identity author annotation raw-txn parsed-txn))
  (-merge-commit [db commit-jsonld commit-data-jsonld]
    (merge-commit db commit-jsonld commit-data-jsonld))

  subject/SubjectFormatter
  (-forward-properties [db iri spec context compact-fn cache tracker error-ch]
    (jld-format/forward-properties db iri spec context compact-fn cache tracker error-ch))

  (-reverse-property [db iri reverse-spec context tracker error-ch]
    (jld-format/reverse-property db iri reverse-spec context tracker error-ch))

  (-iri-visible? [db tracker iri]
    (qpolicy/allow-iri? db tracker iri))

  indexer/Indexable
  (index [db changes-ch]
    (if (novelty/min-novelty? db)
      (do (log/debug "minimum reindex novelty exceeded for:" (:alias db) ". starting reindex")
          (novelty/refresh db changes-ch max-old-indexes))
      (do (log/debug "minimum reindex novelty size not met for:" (:alias db) ". skipping reindex")
          (go db))))

  TimeTravel
  (datetime->t [db datetime]
    (go-try
      (log/debug "datetime->t db:" (pr-str db))
      (let [epoch-datetime (util/str->epoch-ms datetime)
            current-time   (util/current-time-millis)
            [start end]    (if (< epoch-datetime current-time)
                             [epoch-datetime current-time]
                             [current-time epoch-datetime])
            flakes         (-> db
                               policy/root
                               (query-range/index-range
                                nil    ;; TODO: track fuel
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

  (sha->t [db sha]
    (go-try
      (log/debug "sha->t looking up commit SHA:" sha)
      ;; Normalize the input - use only 'fluree:commit:sha256:b' prefix when present,
      ;; otherwise ensure the value starts with 'b'
      (let [sha-normalized (cond
                             ;; Input is a full commit IRI with ':b' segment - keep leading 'b'
                             (str/starts-with? sha iri/f-commit-256-b-ns)
                             (subs sha (dec (count iri/f-commit-256-b-ns)))

                             ;; Already has correct format (starts with 'b')
                             (str/starts-with? sha "b")
                             sha

                             ;; User provided just the hash without 'b' prefix
                             :else
                             (str "b" sha))
            sha-length (count sha-normalized)]

        (log/debug "sha->t normalized SHA:" sha-normalized "length:" sha-length)

        (cond
          ;; Too long to be a valid SHA (52 = 'b' + 51 char hash)
          (> sha-length 52)
          (throw (ex-info (str "Invalid SHA: too long (" sha-length " chars). "
                               "SHA-256 in base32 with 'b' prefix should be 52 characters.")
                          {:status 400 :error :db/invalid-commit-sha
                           :sha sha :normalized sha-normalized :length sha-length}))

          ;; Too short to be a useful/efficient prefix (minimum 6)
          (< sha-length 6)
          (throw (ex-info "SHA prefix must be at least 6 characters"
                          {:status 400 :error :db/invalid-commit-sha :min 6}))

          :else
          (let [;; sha-normalized already has 'b' prefix from normalization
                commit-id-prefix (str iri/f-commit-256-ns sha-normalized)
                ;; Use the index to find commits with this SHA or prefix
                start-sid (iri/encode-iri db commit-id-prefix)
                end-sid   (iri/encode-iri db (str commit-id-prefix "~"))
                ;; Get flakes for subjects in this range
                flakes    (-> db
                              policy/root
                              (query-range/index-range
                               nil ;; TODO: track fuel
                               :spot
                               >= [start-sid]
                               < [end-sid])
                              <?)
                distinct-sids (count (distinct (map flake/s flakes)))]
            (log/debug "sha->t prefix search found" distinct-sids "matching commits")
            (cond
              (empty? flakes)
              (throw (ex-info (str "No commit found with SHA prefix: " sha-normalized)
                              {:status 400 :error :db/invalid-commit-sha :sha sha}))

              (> distinct-sids 1)
              (let [commit-sids (distinct (map flake/s flakes))
                    commit-ids (mapv #(iri/decode-sid db %) commit-sids)]
                (throw (ex-info (str "Ambiguous SHA prefix: " sha-normalized ". Multiple commits match.")
                                {:status 400 :error :db/ambiguous-commit-sha
                                 :sha sha
                                 :matches commit-ids})))

              :else
              ;; Single matching commit - use the t from the first flake
              (flake/t (first flakes))))))))

  (-as-of [db t]
    (assoc db :t t))

  AuditLog
  (-history [db tracker context from-t to-t commit-details? include error-ch history-q]
    (history/query-history db tracker context from-t to-t commit-details? include error-ch history-q))
  (-commits [db tracker context from-t to-t include error-ch]
    (history/query-commits db tracker context from-t to-t include error-ch))

  policy/Restrictable
  (wrap-policy [db policy policy-values]
    (policy-rules/wrap-policy db policy policy-values))
  (wrap-policy [db tracker policy policy-values]
    (policy-rules/wrap-policy db tracker policy policy-values))
  (root [db]
    (policy/root-db db))

  reasoner/Reasoner
  (-reason [db methods rule-sources tracker reasoner-max]
    (flake.reasoner/reason db methods rule-sources tracker reasoner-max))
  (-reasoned-facts [db]
    (reasoner-util/reasoned-facts db))

  optimize/Optimizable
  (-reorder [db parsed-query]
    (async/go (optimize-query db parsed-query)))

  (-explain [db parsed-query]
    (async/go (explain-query db parsed-query))))

(defn db?
  [x]
  (instance? FlakeDB x))

(def ^String label "#fluree/FlakeDB ")

(defn display
  [db]
  (select-keys db [:alias :t :stats :policy]))

#?(:cljs (extend-type FlakeDB
           IPrintWithWriter
           (-pr-writer [db w _opts]
             (-write w label)
             (-write w (-> db display pr))))

   :clj (defmethod print-method FlakeDB [^FlakeDB db, ^Writer w]
          (.write w label)
          (binding [*out* w]
            (-> db display pr))))

(defmethod pprint/simple-dispatch FlakeDB
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
   {:size 0
    :t    0} index/types))

(defn genesis-root-map
  [ledger-alias]
  (let [{spot-cmp :spot, psot-cmp :psot, post-cmp :post, opst-cmp :opst, tspo-cmp :tspo}
        index/comparators]
    {:t               0
     :spot            (index/empty-branch ledger-alias spot-cmp)
     :psot            (index/empty-branch ledger-alias psot-cmp)
     :post            (index/empty-branch ledger-alias post-cmp)
     :opst            (index/empty-branch ledger-alias opst-cmp)
     :tspo            (index/empty-branch ledger-alias tspo-cmp)
     :stats           {:flakes 0, :size 0, :indexed 0}
     :namespaces      iri/default-namespaces
     :namespace-codes iri/default-namespace-codes
     :schema          (vocab/base-schema)}))

(defn read-commit-data
  [commit-storage commit-jsonld db-address error-ch]
  (go
    (try*
      (let [commit-data (<? (commit-storage/read-data-jsonld commit-storage db-address))]
        [commit-jsonld commit-data])
      (catch* e
        (log/error e "Error reading commit data")
        (>! error-ch e)))))

(defn with-commit-data
  [commit-storage error-ch commits-ch]
  (let [to (async/chan)
        af (fn [input ch]
             (go
               (let [[commit-jsonld _commit-proof] input
                     db-address (-> commit-jsonld
                                    (get-first const/iri-data)
                                    (get-first-value const/iri-address))]
                 (-> commit-storage
                     (read-commit-data commit-jsonld db-address error-ch)
                     (async/pipe ch)))))]
    (async/pipeline-async 2 to af commits-ch)
    to))

(defn merge-novelty-commit
  [db error-ch [commit-jsonld db-data-jsonld]]
  (go
    (try*
      (<? (transact/-merge-commit db commit-jsonld db-data-jsonld))
      (catch* e
        (log/error e "Error merging novelty commit")
        (>! error-ch e)))))

(defn merge-novelty-commits
  [indexed-db error-ch commit-pair-ch]
  (go-loop [db indexed-db]
    (if-let [commit-pair (<! commit-pair-ch)]
      (recur (<! (merge-novelty-commit db error-ch commit-pair)))
      db)))

(defn load-novelty
  [commit-storage indexed-db index-t commit-jsonld]
  (go
    (let [error-ch (async/chan)
          db-ch    (->> (commit-storage/trace-commits commit-storage commit-jsonld (inc index-t) error-ch)
                        (with-commit-data commit-storage error-ch)
                        (merge-novelty-commits indexed-db error-ch))]
      (async/alt!
        error-ch ([e] e)
        db-ch    ([db] db)))))

(defn add-reindex-thresholds
  "Adds reindexing thresholds to the root map.

  Gives preference to indexing-opts param, which is passed in
  when creating a new ledger.

  If no indexing opts are present, looks for latest setting
  written at latest index root and uses that.

  Else, uses default values."
  [{:keys [config] :as root-map} indexing-opts]
  (let [reindex-min-bytes (or (:reindex-min-bytes indexing-opts)
                              (:reindex-min-bytes config)
                              100000) ; 100 kb
        reindex-max-bytes (or (:reindex-max-bytes indexing-opts)
                              (:reindex-max-bytes config)
                              1000000) ; 1mb
        max-old-indexes (or (:max-old-indexes indexing-opts)
                            (:max-old-indexes config)
                            3)] ;; default of 3 maximum old indexes not garbage collected
    (when-not (and (int? max-old-indexes)
                   (>= max-old-indexes 0))
      (throw (ex-info "Invalid max-old-indexes value. Must be a non-negative integer."
                      {:status 400, :error :db/invalid-config})))
    (assoc root-map :reindex-min-bytes reindex-min-bytes
           :reindex-max-bytes reindex-max-bytes
           :max-old-indexes max-old-indexes)))

;; TODO - VG - need to reify vg from db-root!!
(defn load
  ([ledger-alias commit-catalog index-catalog commit-pair]
   (load ledger-alias commit-catalog index-catalog commit-pair {}))
  ([ledger-alias commit-catalog index-catalog [commit-jsonld commit-map] indexing-opts]
   (go-try
     (let [commit-t    (-> commit-jsonld
                           (get-first const/iri-data)
                           (get-first-value const/iri-fluree-t))
           root-map    (if-let [{:keys [address]} (:index commit-map)]
                         (<? (index-storage/read-db-root index-catalog address))
                         (genesis-root-map ledger-alias))
           max-ns-code (-> root-map :namespace-codes iri/get-max-namespace-code)
           indexed-db  (-> root-map
                           (add-reindex-thresholds indexing-opts)
                           (assoc :index-catalog index-catalog
                                  :commit-catalog commit-catalog
                                  :alias ledger-alias
                                  :commit commit-map
                                  :tt-id nil
                                  :comparators index/comparators
                                  :staged nil
                                  :novelty (new-novelty-map index/comparators)
                                  :max-namespace-code max-ns-code)
                           map->FlakeDB
                           policy/root)
           indexed-db* (if (nil? (:schema root-map)) ;; needed for legacy (v0) root index map
                         (<? (vocab/load-schema indexed-db (:preds root-map)))
                         indexed-db)
           index-t     (:t indexed-db*)
           loaded-db   (if (= commit-t index-t)
                         indexed-db*
                         (<? (load-novelty commit-catalog indexed-db* index-t commit-jsonld)))]
       (<? (shacl/hydrate-shape-cache! loaded-db))))))
