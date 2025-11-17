(ns fluree.db.flake.match
  (:refer-clojure :exclude [load vswap!])
  (:require [clojure.core.async :as async :refer [<! >!]]
            [clojure.set :as set]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.range :as query-range]
            [fluree.db.util :as util :refer [cartesian-product vswap!]]
            [fluree.db.util.async :refer [<? go-try inner-join-by outer-join-by
                                          repartition-each-by]]))

#?(:clj (set! *warn-on-reflection* true))

(defn match-id
  [db tracker solution s-mch error-ch]
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
    (if-let [s (where/compute-sid s-mch* db)]
      (-> db
          (where/resolve-flake-range tracker error-ch [s])
          (async/pipe matched-ch))
      (async/close! matched-ch))
    matched-ch))

(defn match-flakes-xf
  [db solution triple]
  (comp cat
        (map (fn [flake] (where/match-flake solution triple db flake)))))

(defn var-pattern
  [triple]
  (mapv #(if (where/get-variable %) :? :v) triple))

(defmulti resolve-transitive
  (fn [_db _tracker _solution triple _error-ch]
    (var-pattern triple)))

(defmethod resolve-transitive :default
  [_ _ _ _triple error-ch]
  (async/put! error-ch (ex-info "Unsupported transitive path." {:status 400 :error :db/unsupported-transitive-path}))
  (doto (async/chan) async/close!))

(defn get-match-iri
  [var soln]
  (-> soln (get var) where/get-iri))

(defmethod resolve-transitive [:? :v :v]
  [db tracker solution [s p o] error-ch]
  (let [tag          (where/get-transitive-property p)
        p*           (where/remove-transitivity p)
        s-var        (where/get-variable s)
        get-s-iri    (partial get-match-iri s-var)
        initial-soln {s-var o}
        out-ch       (async/chan)]
    (async/go
      (when (= :zero+ tag) (>! out-ch initial-soln))
      (loop [[soln & to-visit] [initial-soln]
             visited-iris      (if (= :zero+ tag) #{(where/get-iri o)} #{})]
        (if soln
          (let [step-solns (<! (async/into [] (where/match-clause db tracker solution [[s p* (get soln s-var)]] error-ch)))
                remove-visited-xf (remove (comp visited-iris (partial get-match-iri s-var)))
                visited-step      (sequence remove-visited-xf step-solns)]
            (<! (async/onto-chan! out-ch visited-step false))
            (recur (into to-visit visited-step)
                   (into visited-iris (map get-s-iri) step-solns)))
          (async/close! out-ch))))
    out-ch))

(defmethod resolve-transitive [:v :v :?]
  [db tracker solution [s p o] error-ch]
  (let [tag          (where/get-transitive-property p)
        p*           (where/remove-transitivity p)
        o-var        (where/get-variable o)
        get-o-iri    (partial get-match-iri o-var)
        initial-soln {o-var s}
        out-ch       (async/chan)]
    (async/go
      (when (= :zero+ tag) (>! out-ch initial-soln))
      (loop [[soln & to-visit] [initial-soln]
             visited-iris      (if (= :zero+ tag) #{(where/get-iri s)} #{})]
        (if soln
          (let [step-solns (<! (async/into [] (where/match-clause db tracker solution [[(get soln o-var) p* o]] error-ch)))
                remove-visited-xf (remove (comp visited-iris get-o-iri))
                visited-step      (sequence remove-visited-xf step-solns)]
            (<! (async/onto-chan! out-ch visited-step false))
            (recur (into to-visit visited-step)
                   (into visited-iris (map get-o-iri) step-solns)))
          (async/close! out-ch))))
    out-ch))

(defn o-match->s-match
  "Strip extra keys from a match on an o-var so taht it can be compared to a match
  from an s-var."
  [mch]
  (select-keys mch [::where/var ::where/sids ::where/iri]))

(defn add-reflexive-solutions
  [s-var o-var solns]
  (into #{}
        (mapcat (fn [{s s-var o o-var :as soln}]
                  (let [s* (assoc s ::where/var o-var)
                        o* (assoc (o-match->s-match o) ::where/var s-var)]
                    [{s-var s o-var s*}
                     {s-var o* o-var (o-match->s-match o)}
                     soln])))
        solns))

(defn transitive-step
  [s-var o-var solns]
  (let [get-solution-iris (juxt (partial get-match-iri s-var)
                                (partial get-match-iri o-var))
        solution-iris     (into #{} (map get-solution-iris) solns)]
    (reduce (fn [results {o o-var s s-var :as soln}]
              (into results
                    ;; do not include solns we've already found
                    (remove #(solution-iris (get-solution-iris %)))
                    ;; join each result o-match to each other result's s-match and create a new solution
                    (reduce (fn [joins {o-match o-var s-match s-var}]
                              (if (= (where/get-iri o) (where/get-iri s-match))
                                (conj joins (assoc soln s-var s o-var o-match))
                                joins))
                            []
                            solns)))
            []
            solns)))

(defn transitive-steps
  [step0 [s p o]]
  (let [tag   (where/get-transitive-property p)
        s-var (where/get-variable s)
        o-var (where/get-variable o)]
    (loop [steps      0
           step-solns step0
           result     (if (= :zero+ tag)
                        (add-reflexive-solutions s-var o-var step0)
                        step0)]
      (if (seq step-solns)
        (let [next-step-solns (transitive-step s-var o-var result)]
          (recur (inc steps) next-step-solns (into result next-step-solns)))
        (async/to-chan! result)))))

(defmethod resolve-transitive [:? :v :?]
  [db tracker solution [s p o] error-ch]
  (let [step-ch (async/into #{} (where/match-clause db tracker solution [[s (where/remove-transitivity p) o]] error-ch))
        soln-ch (async/chan)]
    (async/pipeline-async 2
                          soln-ch
                          (fn [step ch]
                            (-> (transitive-steps step [s p o])
                                (async/pipe ch)))
                          step-ch)
    soln-ch))

(defn match-triple
  [db tracker solution tuple error-ch]
  (let [out-ch   (async/chan 2)
        db-alias (:alias db)
        triple   (where/assign-matched-values tuple solution)]
    (if-some [[s p o] (where/compute-sids db triple)]
      (let [pid (where/get-sid p db)]
        (if-some [props (and pid (where/get-child-properties db pid))]
          (let [prop-ch (-> props (conj pid) async/to-chan!)]
            (async/pipeline-async 2
                                  out-ch
                                  (fn [prop ch]
                                    (let [p* (where/match-sid p db-alias prop)]
                                      (-> db
                                          (where/resolve-flake-range tracker error-ch [s p* o])
                                          (async/pipe (async/chan 2 (match-flakes-xf db solution tuple)))
                                          (async/pipe ch))))
                                  prop-ch))

          (if (where/get-transitive-property p)
            (-> (resolve-transitive db tracker solution [s p o] error-ch)
                (async/pipe out-ch))
            (-> db
                (where/resolve-flake-range tracker error-ch [s p o])
                (async/pipe (async/chan 2 (match-flakes-xf db solution tuple)))
                (async/pipe out-ch)))))
      (async/close! out-ch))
    out-ch))

(defn subclasses
  [{:keys [schema] :as _db} class]
  (get @(:subclasses schema) class))

(defn class-matches
  [{:keys [alias] :as db} mch]
  (let [cls     (where/get-sid mch db)
        sub-obj (dissoc mch ::sids ::iri)]
    (into [mch]
          (map (fn [cls]
                 (where/match-sid sub-obj alias cls)))
          (subclasses db cls))))

(defn match-flake-against-triples
  "Matches a single flake against a vector of triple patterns.

  Tries each triple in sequence until one matches. Returns the updated solution
  if any triple matches, nil otherwise."
  [solution triples db flake]
  (some (fn [triple]
          (where/match-flake solution triple db flake))
        triples))

(defn build-solution-from-flake-tuple
  "Builds a solution from a tuple of flakes by matching each against its triples.

  Reduces over the flake tuple, matching each flake against its corresponding
  triples vector and accumulating variable bindings in the solution.

  Returns the final solution if all flakes match successfully, nil if any fail."
  [initial-solution triple-groups db flake-tuple]
  (reduce (fn [soln [triples flake]]
            (if-let [next-soln (and soln (match-flake-against-triples soln triples db flake))]
              next-soln
              (reduced nil)))
          initial-solution
          (map vector triple-groups flake-tuple)))

(defn match-inner-join
  "Joins flake chunks across triples vectors.

  `triple-groups` is a sequence of triples vectors.
  `join-tuple` is a vector of flake chunks, one per triples vector, all for the same subject.

  Computes the cartesian product of the flake chunks to get all combinations of
  flakes (one from each chunk). For each combination, builds a solution by reducing
  over the flakes and matching each against its corresponding triples.

  Returns a sequence of solutions."
  [solution triple-groups db join-tuple]
  (->> join-tuple
       cartesian-product
       (keep (fn [flake-tuple]
               (build-solution-from-flake-tuple solution triple-groups db flake-tuple)))))

(defn include-subproperties
  "Returns a vector containing the original triple plus variants for each subproperty.

  Queries the schema of `db` for subproperties of the triple's property, handling
  both rdfs:subPropertyOf and owl:equivalentProperty relationships. Equivalent
  properties are stored bidirectionally as subproperties, so if prop-a is equivalent
  to prop-b, both will be included when querying either one.

  Only expands one level deep (non-recursive) to match the behavior of match-triple.

  If no subproperties exist, returns a vector containing only the original triple."
  [db [s p o :as triple]]
  (let [pid (where/get-sid p db)]
    (if-some [child-props (where/get-child-properties db pid)]
      (let [db-alias (:alias db)]
        (into [triple]
              (map (fn [child-pid]
                     (let [p* (where/match-sid p db-alias child-pid)]
                       [s p* o])))
              (disj child-props pid)))
      [triple])))

(defn expand-subclasses
  "Expands a single class pattern triple to include all subclasses.

  For class patterns (rdf:type), returns a vector containing the original triple
  plus a variant for each subclass. For non-class patterns, returns a vector
  containing only the original triple."
  [db [s p o :as triple]]
  (if (= (where/get-sid p db) const/$rdf:type)
    (mapv (fn [class-obj]
            [s p class-obj])
          (class-matches db o))
    [triple]))

(defn include-subclasses
  "Expands class patterns in a triple collection to include subclasses.

  Applies `expand-subclasses` to each triple and concatenates results into a
  single vector."
  [db triple-group]
  (into []
        (mapcat (partial expand-subclasses db))
        triple-group))

(defn get-property-ids
  "Extracts all property IDs from the `triples` collection."
  [db triples]
  (into #{}
        (map (fn [[_s p _o]]
               (where/get-sid p db)))
        triples))

(defn group-by-pid
  "Builds a map from each property ID appearing in triples from `triple-groups` to
  a collection of all the triples it appears in."
  [db triple-groups]
  (reduce (fn [acc triples]
            (let [pids (get-property-ids db triples)]
              (reduce (fn [m pid]
                        (update m pid (fnil conj #{}) triples))
                      acc
                      pids)))
          {} triple-groups))

(defn consolidate
  "Consolidates a collection of sequences into a single distinct vector.

  Concatenates all elements from all sequences in `seqs`, removes duplicates,
  and returns the result as a vector."
  [seqs]
  (into []
        (comp cat
              (distinct))
        seqs))

(defn group-shared-properties
  "Finds all triples vectors transitively connected to the seed via shared properties.

  Uses breadth-first search to find all triples vectors that share at least one
  property ID with the seed or with any triples vector connected to it.

  Returns a set of all connected triples vectors including the seed."
  [db pid->triples seed]
  (loop [remaining #{seed}
         visited   #{}]
    (if-let [current (first remaining)]
      (let [remaining*      (disj remaining current)
            related-triples (->> (get-property-ids db current)
                                 (map (fn [pid]
                                        (get pid->triples pid)))
                                 (apply set/union))
            new-triples     (set/difference related-triples visited)]
        (recur (into remaining* new-triples)
               (conj visited current)))
      visited)))

(defn merge-related-property-groups
  "Merges groups of triples that share any properties (handles equivalent properties).

  When properties are equivalent (owl:equivalentProperty), they appear in each
  other's childProps. This means querying either property will expand to include
  both. If the query has separate patterns for both properties, we need to merge
  them into a single triples vector so the join treats them as the same
  constraint.

  Uses a union-find-like algorithm to group triples vectors that share properties.

  Returns a sequence of merged triples vectors."
  [db triple-groups]
  (if (empty? triple-groups)
    []
    (let [pid->triples (group-by-pid db triple-groups)]
      (loop [remaining (set triple-groups)
             result    []]
        (if (empty? remaining)
          result
          (let [seed            (first remaining)
                related-triples (group-shared-properties db pid->triples seed)
                merged-triples  (consolidate related-triples)]
            (recur (set/difference remaining related-triples)
                   (conj result merged-triples))))))))

(defn assign-patterns
  "Processes query patterns into property groups for matching.

  For each pattern:
  - Extracts the triple and assigns matched values from `solution`
  - Computes subject IDs
  - Expands to include subproperties
  - Expands class patterns to include subclasses

  Then merges any groups that share properties (e.g., equivalent properties)
  into single consolidated groups.

  Returns a sequence of triple vectors, where each vector contains triples
  that should be matched together as a group."
  [db solution patterns]
  (->> patterns
       (map (fn [pattern]
              (-> pattern
                  where/pattern-data ; extract triple from
                                     ; class patterns
                  (where/assign-matched-values solution))))
       (map (partial where/compute-sids db))
       (map (partial include-subproperties db))
       (map (partial include-subclasses db))
       (merge-related-property-groups db)))

(defn first-sid
  "Extracts the subject ID from the first flake of `flake-chunk`"
  [flake-chunk]
  (flake/s (first flake-chunk)))

(defn flatten-outer-join-tuple
  "Flattens an outer-join tuple of collections into a single vector

  Removes `nil` values (from non-matching sides of the join) and concatenates
  all remaining collections into a single vector."
  [outer-join-tuple]
  (into []
        (comp (remove nil?)
              cat)
        outer-join-tuple))

(defn combined-flake-range
  "Creates a channel of flake chunks, one chunk per subject appearing in any triple.

  For each triple in `triples`, queries the appropriate index (psot or post based
  on whether the object is matched) to retrieve matching flakes. Each channel is
  repartitioned by subject ID, then outer-joined by subject to ensure:
  - All subjects appearing in ANY property are included
  - Flakes are emitted in sorted order by subject ID
  - Each chunk combines flakes from all properties for the same subject

  The outer join allows properties to match different sets of subjects - if a
  subject has flakes for some properties but not others, it still produces a
  chunk containing flakes from only the matching properties.

  Returns a channel of flake vectors, where each vector contains all flakes for
  a single subject across all properties in `triples`."
  [db tracker error-ch triples]
  (let [flatten-xf (map flatten-outer-join-tuple)]
    (->> triples
         (map (fn [[_s _p o :as triple]]
                (let [idx (if (where/matched? o) :post :psot)]
                  (where/resolve-flake-range db tracker error-ch triple idx))))
         (repartition-each-by flake/s)
         (outer-join-by flake/cmp-sid first-sid 2 flatten-xf))))

(defn match-properties
  [db tracker solution patterns error-ch]
  (if (and (index/supports? db :psot)
           (index/supports? db :post))
    (let [triple-groups (assign-patterns db solution patterns)
          match-join-xf (mapcat (fn [join]
                                  (match-inner-join solution triple-groups db join)))]
      (->> triple-groups
           (map (fn [triples]
                  (combined-flake-range db tracker error-ch triples)))
           (inner-join-by flake/cmp-sid first-sid 2 match-join-xf)))
    (where/match-patterns db tracker solution patterns error-ch)))

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

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db tracker subject-id]
  (go-try
    (let [root (policy/root db)]
      (<? (query-range/index-range root tracker :spot = [subject-id const/$rdf:type]
                                   {:flake-xf (map flake/o)})))))

(defn match-class
  [db tracker solution triple error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (with-distinct-subjects)
                                       (map (fn [flake]
                                              (where/match-flake solution triple db flake)))))
        triple     (where/assign-matched-values triple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [class-ch (async/to-chan! (class-matches db o))]
        (async/pipeline-async 2
                              matched-ch
                              (fn [class-obj ch]
                                (-> (where/resolve-flake-range db tracker error-ch [s p class-obj])
                                    (async/pipe ch)))
                              class-ch))
      (async/close! matched-ch))
    matched-ch))
