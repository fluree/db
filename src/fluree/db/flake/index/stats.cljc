(ns fluree.db.flake.index.stats
  "Statistics tracking for ledger indexing.
   Tracks property usage per class including datatypes, reference classes, and language tags."
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index.hyperloglog :as hll-persist]
            [fluree.db.indexer.hll :as hll]
            [fluree.db.query.range :as query-range]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn- get-subject-classes
  "Returns a channel with set of class SIDs for the given subject."
  [db subject-sid]
  (go-try
    (let [flakes (<? (query-range/index-range db nil :spot = [subject-sid const/$rdf:type] {}))]
      ;; flakes is already a sorted set from the index, just extract object values
      (set (map flake/o flakes)))))

(defn- get-lang-tag
  "Extracts language tag from flake's m field."
  [flake]
  (:lang (flake/m flake)))

(defn- track-property-usage
  "Track property usage for a flake on given classes.
   Updates the class-props map with:
   - Property SID used
   - Datatype of the value
   - Referenced class SIDs (if datatype is @id)
   - Language tags (if datatype is langString)

   Only processes assertion (add) flakes.
   Excludes @type/rdf:type as it's an internal JSON-LD construct."
  [db class-props classes flake]
  (go-try
    (if-not (flake/op flake)
      class-props
      (let [prop-sid (flake/p flake)] 
        (if (= prop-sid const/$rdf:type) ;; Skip tracking for @type/rdf:type property
          class-props
          (let [dt-sid (flake/dt flake)]
            (loop [class-props* class-props
                   [cls & rest-classes] (seq classes)]
              (if-not cls
                class-props*
                (let [cls-data (get class-props* cls {})
                      props    (get cls-data :properties {})
                      prop-data    (get props prop-sid {:types #{} :ref-classes #{} :langs #{}})
                      prop-data*   (update prop-data :types conj dt-sid)
                      prop-data**  (if (= dt-sid const/$id)
                                     (let [ref-sid (flake/o flake)
                                           ref-classes (<? (get-subject-classes db ref-sid))]
                                       (update prop-data* :ref-classes into ref-classes))
                                     prop-data*)
                      prop-data*** (if (= dt-sid const/$rdf:langString)
                                     (if-let [lang (get-lang-tag flake)]
                                       (update prop-data** :langs conj lang)
                                       prop-data**)
                                     prop-data**)
                      props* (assoc props prop-sid prop-data***)
                      cls-data* (assoc cls-data :properties props*)
                      class-props** (assoc class-props* cls cls-data*)]
                  (recur class-props** rest-classes))))))))))

(defn process-subject-group
  "Process all flakes for a single subject, tracking property usage on its classes.
   Returns updated class-props map.

   subject-flakes: All flakes for a single subject (already grouped)
   db: Database to query for class information
   class-props: Accumulated class property tracking map"
  [db subject-flakes class-props]
  (go-try
    (let [subject-sid (flake/s (first subject-flakes))
          classes (<? (get-subject-classes db subject-sid))]
      (if (empty? classes)
        class-props
        (loop [class-props* class-props
               [f & rest-flakes] subject-flakes]
          (if-not f
            class-props*
            (let [updated-props (<? (track-property-usage db class-props* classes f))]
              (recur updated-props rest-flakes))))))))

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

(defn compute-class-property-stats-async
  "Compute enhanced class property statistics by processing all subjects.
   Returns a map of {class-sid {:properties {prop-sid {:types #{} :ref-classes #{} :langs #{}}}}}}"
  [db]
  (go-try
    (let [post-novelty (get-in db [:novelty :post])
          prev-classes (get-in db [:stats :classes] {})]
      (if (empty? post-novelty)
        {}
        (try*
          (let [subject-groups (partition-by flake/s post-novelty)
                ;; Extract only :properties from previous classes
                prev-class-props (reduce-kv (fn [acc class-sid class-data]
                                              (if-let [props (:properties class-data)]
                                                (assoc acc class-sid {:properties props})
                                                acc))
                                            {}
                                            prev-classes)]
            (loop [[sg & rest-sgs] subject-groups
                   class-props* prev-class-props]
              (if sg
                (let [updated (<? (process-subject-group db sg class-props*))]
                  (recur rest-sgs updated))
                class-props*)))
          (catch* e
            (log/error e "Class property stats computation failed"
                       {:novelty-size (get-in db [:novelty :size])
                        :ledger-alias (:alias db)
                        :t (:t db)})
            {}))))))
