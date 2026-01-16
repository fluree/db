(ns fluree.db.virtual-graph.iceberg.transitive
  "Transitive property path execution for Iceberg VG.

   Implements SPARQL property paths like pred+ and pred* using BFS traversal
   over Iceberg tables."
  (:require [fluree.db.query.exec.where :as where]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.iceberg.query :as query]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Transitive Property Path Execution
;;; ---------------------------------------------------------------------------

(def ^:dynamic *transitive-depth-limit*
  "Maximum depth for transitive property path traversal.
   Prevents runaway queries on very deep hierarchies."
  100)

(defn- transitive-forward-step
  "Execute one forward hop: given a set of current IRIs, find all directly connected objects.

   For each IRI in current-iris, queries the Iceberg table to find all objects
   connected via the specified predicate.

   Returns a set of new object IRIs (not in visited)."
  [source mapping pred-iri current-iris visited-iris time-travel]
  (when (seq current-iris)
    (let [subject-template (:subject-template mapping)
          table-name (:table mapping)
          ;; Get the column that maps to this predicate
          obj-col (query/get-column-for-predicate pred-iri mapping)
          ;; Get the subject column(s) from template
          subj-cols (r2rml/extract-template-cols subject-template)]
      (when (and obj-col (seq subj-cols))
        ;; For simplicity, assume single-column subject template
        (let [subj-col (first subj-cols)
              ;; Extract IDs from current IRIs to use as predicates
              current-ids (->> current-iris
                               (keep #(query/extract-id-from-iri % subject-template))
                               vec)
              ;; Build IN predicate for subject column
              predicates (when (seq current-ids)
                           [{:column subj-col :op :in :value current-ids}])
              ;; Query Iceberg for matching rows
              rows (when predicates
                     (tabular/scan-rows source table-name
                                        (cond-> {:columns [subj-col obj-col]
                                                 :predicates predicates}
                                          (:snapshot-id time-travel)
                                          (assoc :snapshot-id (:snapshot-id time-travel))
                                          (:as-of-time time-travel)
                                          (assoc :as-of-time (:as-of-time time-travel)))))]
          ;; Extract object values and build IRIs
          ;; Note: Objects may be IDs (for self-referential FK) or scalar values
          (->> rows
               (keep #(get % obj-col))
               (map #(query/build-iri-from-id (str %) subject-template))
               (remove nil?)
               (remove visited-iris)
               set))))))

(defn- transitive-backward-step
  "Execute one backward hop: given a set of current IRIs, find all subjects that point to them.

   For each IRI in current-iris (as objects), queries the Iceberg table to find
   all subjects connected via the specified predicate.

   Returns a set of new subject IRIs (not in visited)."
  [source mapping pred-iri current-iris visited-iris time-travel]
  (when (seq current-iris)
    (let [subject-template (:subject-template mapping)
          table-name (:table mapping)
          ;; Get the column that maps to this predicate (object column)
          obj-col (query/get-column-for-predicate pred-iri mapping)
          ;; Get the subject column(s) from template
          subj-cols (r2rml/extract-template-cols subject-template)]
      (when (and obj-col (seq subj-cols))
        (let [subj-col (first subj-cols)
              ;; Extract IDs from current IRIs (these are the object values we're looking for)
              current-ids (->> current-iris
                               (keep #(query/extract-id-from-iri % subject-template))
                               vec)
              ;; Build IN predicate for object column
              predicates (when (seq current-ids)
                           [{:column obj-col :op :in :value current-ids}])
              ;; Query Iceberg for matching rows
              rows (when predicates
                     (tabular/scan-rows source table-name
                                        (cond-> {:columns [subj-col obj-col]
                                                 :predicates predicates}
                                          (:snapshot-id time-travel)
                                          (assoc :snapshot-id (:snapshot-id time-travel))
                                          (:as-of-time time-travel)
                                          (assoc :as-of-time (:as-of-time time-travel)))))]
          ;; Extract subject column values and build IRIs
          (->> rows
               (keep #(get % subj-col))
               (map #(query/build-iri-from-id (str %) subject-template))
               (remove nil?)
               (remove visited-iris)
               set))))))

(defn- resolve-transitive-forward
  "Resolve transitive path where subject is bound: ?s pred+ ?o or ?s pred* ?o

   Uses BFS from the bound subject to find all reachable objects.

   Args:
     source       - IcebergSource for the table
     mapping      - R2RML mapping with subject-template and predicates
     pred-iri     - The predicate IRI to traverse
     start-iri    - The bound subject IRI
     tag          - :one+ or :zero+
     time-travel  - Time travel options {:snapshot-id or :as-of-time}

   Returns a vector of reachable object IRIs."
  [source mapping pred-iri start-iri tag time-travel]
  ;; Note: visited always includes start-iri to prevent re-discovery via cycle
  ;; For zero+, start-iri is also added to results (reflexive)
  (loop [depth 0
         frontier #{start-iri}
         visited #{start-iri}
         results (if (= :zero+ tag) [start-iri] [])]
    (if (or (empty? frontier) (>= depth *transitive-depth-limit*))
      (do
        (when (>= depth *transitive-depth-limit*)
          (log/warn "Transitive path depth limit reached"
                    {:limit *transitive-depth-limit*
                     :predicate pred-iri
                     :start start-iri}))
        results)
      (let [next-iris (transitive-forward-step source mapping pred-iri
                                               frontier visited time-travel)
            new-visited (into visited next-iris)
            new-results (into results next-iris)]
        (recur (inc depth) next-iris new-visited new-results)))))

(defn- resolve-transitive-backward
  "Resolve transitive path where object is bound: ?s pred+ :obj or ?s pred* :obj

   Uses BFS backward from the bound object to find all subjects that can reach it.

   Args:
     source       - IcebergSource for the table
     mapping      - R2RML mapping with subject-template and predicates
     pred-iri     - The predicate IRI to traverse
     end-iri      - The bound object IRI
     tag          - :one+ or :zero+
     time-travel  - Time travel options {:snapshot-id or :as-of-time}

   Returns a vector of subject IRIs that can reach the object."
  [source mapping pred-iri end-iri tag time-travel]
  ;; Note: visited always includes end-iri to prevent re-discovery via cycle
  ;; For zero+, end-iri is also added to results (reflexive)
  (loop [depth 0
         frontier #{end-iri}
         visited #{end-iri}
         results (if (= :zero+ tag) [end-iri] [])]
    (if (or (empty? frontier) (>= depth *transitive-depth-limit*))
      (do
        (when (>= depth *transitive-depth-limit*)
          (log/warn "Transitive path depth limit reached"
                    {:limit *transitive-depth-limit*
                     :predicate pred-iri
                     :end end-iri}))
        results)
      (let [next-iris (transitive-backward-step source mapping pred-iri
                                                frontier visited time-travel)
            new-visited (into visited next-iris)
            new-results (into results next-iris)]
        (recur (inc depth) next-iris new-visited new-results)))))

(defn- resolve-transitive-both
  "Resolve transitive path where both subject and object are variables: ?s pred+ ?o

   Finds all connected pairs - this is expensive so requires/recommends LIMIT.

   For each distinct subject in the table, computes the forward closure
   and emits all (subject, object) pairs.

   Args:
     source       - IcebergSource for the table
     mapping      - R2RML mapping with subject-template and predicates
     pred-iri     - The predicate IRI to traverse
     tag          - :one+ or :zero+
     time-travel  - Time travel options
     limit        - Maximum number of pairs to return

   Returns a vector of [subject-iri object-iri] pairs."
  [source mapping pred-iri tag time-travel limit]
  (let [subject-template (:subject-template mapping)
        table-name (:table mapping)
        subj-cols (r2rml/extract-template-cols subject-template)
        subj-col (first subj-cols)]
    (when subj-col
      ;; First, get all distinct subjects that have this predicate
      (let [obj-col (query/get-column-for-predicate pred-iri mapping)
            rows (tabular/scan-rows source table-name
                                    (cond-> {:columns [subj-col obj-col]}
                                      (:snapshot-id time-travel)
                                      (assoc :snapshot-id (:snapshot-id time-travel))
                                      (:as-of-time time-travel)
                                      (assoc :as-of-time (:as-of-time time-travel))))
            ;; Get distinct subject IRIs
            distinct-subjects (->> rows
                                   (keep #(get % subj-col))
                                   distinct
                                   (map #(query/build-iri-from-id (str %) subject-template))
                                   (remove nil?))]
        ;; For each subject, compute forward closure
        (loop [subjects distinct-subjects
               pairs []
               pair-count 0]
          (if (or (empty? subjects) (>= pair-count (or limit 10000)))
            pairs
            (let [subj-iri (first subjects)
                  reachable (resolve-transitive-forward source mapping pred-iri
                                                        subj-iri tag time-travel)
                  new-pairs (mapv #(vector subj-iri %) reachable)
                  updated-pairs (into pairs new-pairs)
                  new-count (+ pair-count (count new-pairs))]
              (recur (rest subjects) updated-pairs new-count))))))))

(defn- get-binding-value
  "Extract the IRI or value from a solution binding.
   Returns the string IRI if the binding is an IRI match, nil otherwise."
  [binding]
  (when binding
    (or (where/get-iri binding)
        (where/get-value binding))))

(defn- apply-single-transitive-pattern
  "Execute a single transitive pattern against a solution, returning expanded solutions.

   Checks if subject/object variables are bound in the solution before defaulting
   to the pattern's constant values."
  [sources mappings routing-indexes {:keys [subject predicate object tag]} solution time-travel]
  (let [s-var (where/get-variable subject)
        o-var (where/get-variable object)
        ;; Check if variables are already bound in solution
        s-from-solution (when s-var (get-binding-value (get solution s-var)))
        o-from-solution (when o-var (get-binding-value (get solution o-var)))
        ;; Use solution binding if available, else use pattern constant
        s-bound (or s-from-solution (when-not s-var (where/get-iri subject)))
        o-bound (or o-from-solution (when-not o-var (where/get-iri object)))
        pred-iri (where/get-iri predicate)
        ;; Find the mapping that handles this predicate
        mapping (query/find-mapping-for-predicate pred-iri mappings routing-indexes)
        ;; Get the source for this mapping's table
        source (when mapping (get sources (:table mapping)))]
    (log/debug "Processing transitive pattern:"
               {:pred-iri pred-iri
                :s-var s-var :o-var o-var
                :s-bound s-bound :o-bound o-bound
                :s-from-solution s-from-solution
                :o-from-solution o-from-solution
                :tag tag
                :mapping-table (:table mapping)
                :has-source? (some? source)})
    (if-not (and mapping source)
      (do
        (log/warn "No mapping or source found for transitive predicate"
                  {:predicate pred-iri})
        [])
      (cond
        ;; [:v :v :?] - subject bound (from pattern or solution), find objects
        (and s-bound (not o-bound))
        (let [objects (resolve-transitive-forward source mapping pred-iri
                                                  s-bound tag time-travel)]
          (log/debug "Transitive forward resolved:" {:count (count objects)})
          (for [obj objects]
            (assoc solution o-var (where/match-iri {} obj))))

        ;; [:? :v :v] - object bound (from pattern or solution), find subjects
        (and (not s-bound) o-bound)
        (let [subjects (resolve-transitive-backward source mapping pred-iri
                                                    o-bound tag time-travel)]
          (log/debug "Transitive backward resolved:" {:count (count subjects)})
          (for [subj subjects]
            (assoc solution s-var (where/match-iri {} subj))))

        ;; [:? :v :?] - both variables unbound
        (and (not s-bound) (not o-bound))
        (let [limit 1000  ;; Default limit for both-unbound case
              pairs (resolve-transitive-both source mapping pred-iri
                                             tag time-travel limit)]
          (log/debug "Transitive both resolved:" {:count (count pairs)})
          (for [[subj obj] pairs]
            (assoc solution
                   s-var (where/match-iri {} subj)
                   o-var (where/match-iri {} obj))))

        ;; [:v :v :v] - both bound (reachability check) - not supported yet
        :else
        (throw (ex-info "Transitive path with both subject and object bound is not yet supported for Iceberg VG"
                        {:status 400
                         :error :db/unsupported-transitive-path
                         :subject s-bound
                         :object o-bound
                         :predicate pred-iri}))))))

(defn apply-transitive-patterns
  "Execute transitive patterns and return solutions.

   This is called from -finalize after detecting transitive patterns in -reorder.
   Uses reduce over trans-specs to properly join multiple transitive patterns
   (rather than union via mapcat).

   Args:
     sources         - Map of {table-name -> IcebergSource}
     mappings        - Map of {table-key -> R2RML mapping}
     routing-indexes - {:predicate->mappings {pred -> [mappings...]}}
     trans-specs     - Vector of {:subject :predicate :object :tag :original-pattern}
     base-solution   - Base solution map to extend
     time-travel     - Time travel options

   Returns a sequence of solution maps."
  [sources mappings routing-indexes trans-specs base-solution time-travel]
  (log/debug "Applying transitive patterns:"
             {:count (count trans-specs)
              :tags (mapv :tag trans-specs)})
  ;; Use reduce to join multiple transitive patterns sequentially
  ;; Each pattern expands the current solution set, feeding into the next
  (reduce
   (fn [solutions spec]
     (if (empty? solutions)
       []  ;; Short-circuit if no solutions
       (mapcat #(apply-single-transitive-pattern
                 sources mappings routing-indexes spec % time-travel)
               solutions)))
   [base-solution]
   trans-specs))
