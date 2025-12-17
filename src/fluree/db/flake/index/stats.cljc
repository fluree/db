(ns fluree.db.flake.index.stats
  "Statistics tracking for ledger indexing.
   Tracks property usage per class including datatypes, reference classes, and language tags."
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.hyperloglog :as hll-persist]
            [fluree.db.indexer.hll :as hll]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

;; Batched PSOT lookup for class retrieval

(defn- subject->psot-flake
  "Create a PSOT seek flake for a subject SID at rdf:type."
  [subject-sid]
  ;; flake/create args: s p o dt t op m
  (flake/create subject-sid const/$rdf:type flake/min-s flake/min-dt flake/min-t flake/min-op flake/min-meta))

(defn- extract-classes-from-leaf
  "Returns seq of [sid #{class-sids}] pairs from a resolved leaf."
  [leaf subject-sids]
  (let [subject-set (set subject-sids)
        flakes (:flakes leaf)]
    ;; Group flakes by subject, filter to only rdf:type flakes for our subjects
    (->> flakes
         (filter (fn [f]
                   (and (= const/$rdf:type (flake/p f))
                        (contains? subject-set (flake/s f))
                        (flake/op f)))) ; only assertions
         (group-by flake/s)
         (map (fn [[sid fs]]
                [sid (set (map flake/o fs))])))))

(defn- extract-classes-from-novelty
  "Returns {sid #{class-sids}} computed from novelty (handles retractions)."
  [novelty subject-sids]
  ;; Handle both assertions and retractions (op true/false).
  (let [wanted? (set subject-sids)]
    (reduce
     (fn [acc f]
       (let [sid (flake/s f)]
         (if (and (wanted? sid)
                  (= const/$rdf:type (flake/p f)))
           (let [class-sid (flake/o f)]
             (if (flake/op f)
               (update acc sid (fnil conj #{}) class-sid)
               (update acc sid (fnil disj #{}) class-sid)))
           acc)))
     {}
     (or novelty []))))

(defn batched-get-subject-classes
  "Returns a channel that yields {sid #{class-sids}}.

  Uses PSOT (when present) + PSOT novelty."
  ([db sids]
   (batched-get-subject-classes db sids {:sorted? false}))
  ([db sids {:keys [sorted?] :or {sorted? false}}]
   (if (empty? sids)
     (go {})
     (let [psot-root (:psot db)
           resolver  (:index-catalog db)
           ;; Get classes from novelty (not yet persisted)
           psot-novelty     (get-in db [:novelty :psot])
           novelty-classes  (extract-classes-from-novelty psot-novelty sids)]
       (if (nil? psot-root)
         ;; No PSOT on this db (e.g. legacy ledgers) - return novelty-only classes.
         (go novelty-classes)
        (let [sorted-sids (if sorted? sids (sort sids))
               input-ch    (async/to-chan! sorted-sids)
              error-ch    (async/chan 1)
              result-ch   (index/streaming-index-lookup
                           resolver psot-root input-ch subject->psot-flake extract-classes-from-leaf
                           error-ch {})]
          ;; Collect results from index and merge with novelty (or throw on error)
           (go-try
            (loop [result novelty-classes]
              (async/alt!
                error-ch ([e] (throw e))
                result-ch ([item]
                           (if item
                             (let [[sid classes] item]
                               (recur (update result sid (fnil into #{}) classes)))
                             result)))))))))))

(defn- merge-class-maps
  "Merge {sid -> #{class-sids}} maps."
  [a b]
  (merge-with into (or a {}) (or b {})))

(defn- collect-subject-and-ref-sids
  "Single pass over subject-ordered `spot-novelty` to collect:
   - distinct subject SIDs in encounter order (already sorted)
   - distinct referenced object SIDs (dt=@id), as a set"
  [spot-novelty]
  (reduce
   (fn [{:keys [last-s] :as acc} f]
     (let [s (flake/s f)
           acc* (if (= s last-s)
                  acc
                  (-> acc
                      (assoc :last-s s)
                      (update :subjects conj! s)))]
       (if (= const/$id (flake/dt f))
         (update acc* :refs conj! (flake/o f))
         acc*)))
   {:last-s nil
    :subjects (transient [])
    :refs     (transient #{})}
   spot-novelty))

(defn- get-lang-tag
  "Extracts language tag from flake's m field."
  [flake]
  (:lang (flake/m flake)))

(defn update-class-counts
  "Update class counts for rdf:type flakes.
   Each flake's object is a class SID - increment for assertions, decrement for retractions."
  [property-flakes prev-classes]
  (reduce (fn [cls f]
            (let [class-sid (flake/o f)
                  delta (if (flake/op f) 1 -1)
                  class-map (get cls class-sid {:count 0})
                  new-count (max 0 (+ (:count class-map 0) delta))]
              (assoc cls class-sid (assoc class-map :count new-count))))
          prev-classes
          property-flakes))

(defn- build-subject-classes-map
  "Build a map of {subject-sid -> #{class-sids}} from novelty flakes.
   Only looks at rdf:type flakes in novelty to determine classes for subjects.
   This is synchronous - no async queries needed."
  [novelty-flakes]
  (let [;; Find all rdf:type flakes in novelty
        type-flakes (filter flake/class-flake? novelty-flakes)]
    ;; Build map from novelty type flakes
    (reduce
     (fn [acc f]
       (let [subject-sid (flake/s f)
             class-sid (flake/o f)
             assert? (flake/op f)]
         (if assert?
           (update acc subject-sid (fnil conj #{}) class-sid)
           (update acc subject-sid (fnil disj #{}) class-sid))))
     {}
     type-flakes)))

(defn- track-property-usage-sync
  "Synchronous version of track-property-usage for novelty replay.
   ref-classes-map: Pre-built map of {ref-sid -> #{class-sids}} from novelty (for @id refs)"
  [class-props classes flake ref-classes-map]
  (let [prop-sid (flake/p flake)]
    (if (= prop-sid const/$rdf:type) ;; Skip tracking for @type/rdf:type property
      class-props
      (let [dt-sid (flake/dt flake)
            assert? (flake/op flake)
            delta (if assert? 1 -1)]
        (reduce
         (fn [class-props* cls]
           (let [cls-data (get class-props* cls {})
                 props    (get cls-data :properties {})
                 prop-data    (get props prop-sid {:types {} :ref-classes {} :langs {}})

                 ;; Update type count
                 prop-data*   (update-in prop-data [:types dt-sid]
                                         (fn [cnt] (max 0 (+ (or cnt 0) delta))))

                 ;; Update ref-class counts if @id type
                 prop-data**  (if (= dt-sid const/$id)
                                (let [ref-sid (flake/o flake)
                                      ;; Get ref classes from pre-built map
                                      ref-classes (get ref-classes-map ref-sid #{})]
                                  (reduce (fn [pd ref-cls]
                                            (update-in pd [:ref-classes ref-cls]
                                                       (fn [cnt] (max 0 (+ (or cnt 0) delta)))))
                                          prop-data*
                                          ref-classes))
                                prop-data*)

                 ;; Update lang counts if langString type
                 prop-data*** (if (= dt-sid const/$rdf:langString)
                                (if-let [lang (get-lang-tag flake)]
                                  (update-in prop-data** [:langs lang]
                                             (fn [cnt] (max 0 (+ (or cnt 0) delta))))
                                  prop-data**)
                                prop-data**)

                 props* (assoc props prop-sid prop-data***)
                 cls-data* (assoc cls-data :properties props*)]
             (assoc class-props* cls cls-data*)))
         class-props
         classes)))))

(defn compute-class-property-stats-from-novelty
  "Compute class property statistics from novelty for ledger-info.
   Only tracks subjects that have rdf:type assertions in novelty.
   Returns updated classes map with :properties details merged from novelty."
  [novelty-flakes prev-classes]
  (if (empty? novelty-flakes)
    prev-classes
    (let [subject-classes-map (build-subject-classes-map novelty-flakes)
          prev-class-props (reduce-kv (fn [acc class-sid class-data]
                                        (if-let [props (:properties class-data)]
                                          (assoc acc class-sid {:properties props})
                                          acc))
                                      {}
                                      prev-classes)
          subject-groups (partition-by flake/s novelty-flakes)
          updated-class-props
          (reduce
           (fn [class-props subject-flakes]
             (let [subject-sid (flake/s (first subject-flakes))
                   classes (get subject-classes-map subject-sid)]
               (if (empty? classes)
                 class-props
                 (reduce
                  (fn [cp f]
                    (track-property-usage-sync cp classes f subject-classes-map))
                  class-props
                  subject-flakes))))
           prev-class-props
           subject-groups)]
      (reduce-kv
       (fn [acc class-sid class-data]
         (if-let [updated-props (get-in updated-class-props [class-sid :properties])]
           (assoc acc class-sid (assoc class-data :properties updated-props))
           (assoc acc class-sid class-data)))
       {}
       prev-classes))))

(defn- process-property-group
  "Process all flakes for a single property, updating counts and sketches.
   Returns updated property data and sketch for this property only."
  [property-flakes prev-prop-data prev-sketch]
  (loop [[f & r] property-flakes
         prop-data prev-prop-data
         sketch prev-sketch]
    (if f
      (let [assert? (flake/op f)
            new-count (if assert?
                        (inc (:count prop-data 0))
                        (dec (:count prop-data)))
            prop-data* (assoc prop-data :count new-count)
            ;; Update HLL sketches (only on assertions - monotone NDV)
            sketch* (if assert?
                      (let [values-sketch (hll/add-value (or (:values sketch) (hll/create-sketch)) (flake/o f))
                            subjects-sketch (hll/add-value (or (:subjects sketch) (hll/create-sketch)) (flake/s f))]
                        {:values values-sketch
                         :subjects subjects-sketch})
                      sketch)]
        (recur r prop-data* sketch*))

      ;; Return updated data with NDV extracted from sketches
      (let [values-sketch (or (:values sketch) (hll/create-sketch))
            subjects-sketch (or (:subjects sketch) (hll/create-sketch))]
        {:property-data (assoc prop-data
                               :ndv-values (hll/cardinality values-sketch)
                               :ndv-subjects (hll/cardinality subjects-sketch))
         :sketch {:values values-sketch
                  :subjects subjects-sketch}}))))

(defn- write-property-sketch
  "Write a single property's sketch to disk immediately.
   Writes values and subjects sketches in parallel.
   Returns :success on successful write (throws on error)."
  [{:keys [storage] :as _index-catalog} ledger-name current-t property-sid sketch]
  (go-try
    (let [{:keys [values subjects]} sketch
          default-key (keyword "fluree.db.storage" "default")
          store       (storage/get-content-store storage default-key)

          values-path   (hll-persist/sketch-filename ledger-name property-sid :values current-t)
          subjects-path (hll-persist/sketch-filename ledger-name property-sid :subjects current-t)

          ;; Kick off both writes in parallel
          values-ch   (storage/write-bytes store values-path values)
          subjects-ch (storage/write-bytes store subjects-path subjects)]

      (<? values-ch)
      (<? subjects-ch)

      :success)))

(defn- process-and-write-property
  "Load sketch, process property flakes, write sketch immediately.
   Returns property stats, class updates (if rdf:type), and old sketch paths for garbage."
  [index-catalog ledger-name current-t property-flakes prev-properties prev-classes]
  (go-try
    (let [p (flake/p (first property-flakes))
          prev-prop-data (get prev-properties p {:count 0})
          prev-last-t (:last-modified-t prev-prop-data)

          ;; Load sketch for this property only (from disk using :last-modified-t)
          loaded-sketch (<? (hll-persist/read-property-sketches index-catalog ledger-name p prev-last-t))
          prev-sketch (or loaded-sketch
                          {:values (hll/create-sketch)
                           :subjects (hll/create-sketch)})

          ;; Process flakes for this property
          {:keys [property-data sketch]} (process-property-group property-flakes prev-prop-data prev-sketch)

          ;; :last-modified-t is important as the sketch file name uses it, without it the sketch cannot be loaded
          property-data* (assoc property-data :last-modified-t current-t)

          _ (<? (write-property-sketch index-catalog ledger-name current-t p sketch))

          ;; Calculate old sketch paths for garbage collection
          old-sketch-paths (when (and prev-last-t (not= prev-last-t current-t))
                             #{(str "fluree:file://" (hll-persist/sketch-filename ledger-name p :values prev-last-t))
                               (str "fluree:file://" (hll-persist/sketch-filename ledger-name p :subjects prev-last-t))})

          class-updates (when (flake/class-flake? (first property-flakes))
                          (update-class-counts property-flakes prev-classes))]

      [p property-data* class-updates old-sketch-paths])))

(defn compute-stats-with-writes
  "Process properties one-by-property: load → update → write.
   Each property's sketch is loaded, updated, and written immediately.
   Returns {:properties {...} :classes {...} :old-sketch-paths #{...}}
   NO sketches in return value - they're written to disk and discarded."
  [index-catalog ledger-name current-t novelty-flakes prev-properties prev-classes]
  (go-try
    (let [property-groups (partition-by flake/p novelty-flakes)]
      (loop [[pg & rest] property-groups
             properties prev-properties
             classes prev-classes
             old-paths #{}]
        (if pg
          (let [[property-sid property-data class-updates old-sketch-paths]
                (<? (process-and-write-property index-catalog ledger-name current-t pg
                                                prev-properties prev-classes))]
            (recur rest
                   (assoc properties property-sid property-data)
                   (or class-updates classes)
                   (into old-paths (or old-sketch-paths #{}))))
          {:properties properties
           :classes classes
           :old-sketch-paths old-paths})))))

(defn- process-subject-group-with-classes
  "Process all flakes for a subject using pre-fetched classes (no async lookup).
   Returns updated class-props map."
  [class-props classes subject-flakes subject-classes-map]
  (if (empty? classes)
    class-props
    (reduce
     (fn [cp f]
       (track-property-usage-sync cp classes f subject-classes-map))
     class-props
     subject-flakes)))

(defn compute-class-property-stats-async
  "Compute enhanced class property statistics by processing all subjects.
   Uses batched PSOT lookup to efficiently get classes for all subjects at once.
   Returns a map of {class-sid {:properties {prop-sid {:types {dt-sid count} :ref-classes {ref-sid count} :langs {lang count}}}}}"
  [db]
  (go-try
    (let [post-novelty (get-in db [:novelty :post])
          prev-classes (get-in db [:stats :classes] {})]
      (log/debug "compute-class-property-stats-async START"
                 {:novelty-size (count post-novelty)
                  :ledger-alias (:alias db)
                  :t (:t db)})
      (if (empty? post-novelty)
        (do
          (log/debug "compute-class-property-stats-async EMPTY - returning {}")
          {})
        (try*
          ;; Prefer grouping by subject using :spot novelty (already subject-ordered).
          ;; We still use post-novelty for the overall "empty?" check above.
          (let [spot-novelty   (or (get-in db [:novelty :spot]) [])
                subject-groups (partition-by flake/s spot-novelty)
                ;; Collect distinct subject SIDs (already sorted by :spot) and referenced SIDs.
                {:keys [subjects refs] :as _sid-acc} (collect-subject-and-ref-sids spot-novelty)
                subject-sids            (persistent! subjects)
                ref-sids                (persistent! refs)
                ;; If we have many ref lookups, sort them once (PSOT needs predicate then subject ordering).
                sorted-ref-sids         (when (seq ref-sids) (sort ref-sids))
                ;; Batched PSOT lookup for all subject classes at once
                _ (log/debug "compute-class-property-stats-async: batched lookup for subjects"
                             {:subject-count (count subject-sids)
                              :ref-count     (count ref-sids)})
                subject-classes-map     (<? (batched-get-subject-classes db subject-sids {:sorted? true}))
                ref-classes-map         (if (seq sorted-ref-sids)
                                          (<? (batched-get-subject-classes db sorted-ref-sids {:sorted? true}))
                                          {})
                all-classes-map         (merge-class-maps subject-classes-map ref-classes-map)
                _ (log/debug "compute-class-property-stats-async: batched lookup complete"
                             {:sids-with-classes (count all-classes-map)})
                ;; Extract only :properties from previous classes
                prev-class-props (reduce-kv (fn [acc class-sid class-data]
                                              (if-let [props (:properties class-data)]
                                                (assoc acc class-sid {:properties props})
                                                acc))
                                            {}
                                            prev-classes)]
            (log/debug "compute-class-property-stats-async processing"
                       {:subject-groups-count (count subject-groups)})
            ;; Process all subject groups synchronously using pre-fetched classes
            (loop [[sg & rest-sgs] subject-groups
                   class-props* prev-class-props
                   idx 0]
              (if sg
                (let [subject-sid (flake/s (first sg))
                      classes (get all-classes-map subject-sid #{})
                      updated (process-subject-group-with-classes class-props* classes sg all-classes-map)]
                  (when (zero? (mod idx 100))
                    (log/debug "compute-class-property-stats-async progress"
                               {:processed idx :remaining (count rest-sgs)}))
                  (recur rest-sgs updated (inc idx)))
                (do
                  (log/debug "compute-class-property-stats-async DONE"
                             {:processed idx})
                  class-props*))))
          (catch* e
            (log/error e "Class property stats computation failed"
                       {:novelty-size (get-in db [:novelty :size])
                        :ledger-alias (:alias db)
                        :t (:t db)})
            {}))))))
