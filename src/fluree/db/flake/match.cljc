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
            [fluree.db.util :as util :refer [vswap!]]
            [fluree.db.util.async :refer [<? go-try inner-join-by
                                          repartition-each-by]]))

#?(:clj (set! *warn-on-reflection* true))

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db tracker subject-id]
  (go-try
    (let [root (policy/root db)]
      (<? (query-range/index-range root tracker :spot = [subject-id const/$rdf:type]
                                   {:flake-xf (map flake/o)})))))

(defn subclasses
  [{:keys [schema] :as _db} class]
  (get @(:subclasses schema) class))

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

(defn class-matches
  [{:keys [alias] :as db} mch]
  (let [cls     (where/get-sid mch db)
        sub-obj (dissoc mch ::sids ::iri)]
    (into [mch]
          (map (fn [cls]
                 (where/match-sid sub-obj alias cls)))
          (subclasses db cls))))

(defn match-property-flakes
  [solution triple db property-flakes]
  (map (fn [flake]
         (where/match-flake solution triple db flake))
       property-flakes))

(defn match-subject-triple
  [initial-solutions triple db property-flakes]
  (mapcat (fn [solution]
            (match-property-flakes solution triple db property-flakes))
          initial-solutions))

(defn match-subject-property
  [initial-solutions triples db property-flakes]
  (mapcat (fn [triple]
            (match-subject-triple initial-solutions triple db property-flakes))
          triples))

(defn with-family
  "Adds mappings for a single property family to the `pid-map`.

  For each triple in the family, extracts its property ID and maps it to the
  complete set of triples for the family. This allows any property in the family
  (parent or child) to retrieve all triples when matching.

  Returns the updated map."
  [db pid-map family-triples]
  (reduce (fn [m [_s p _o]]
            (let [pid (where/get-sid p db)]
              (assoc m pid family-triples)))
          pid-map family-triples))

(defn match-join
  "Joins flake chunks across property families. `property-families` is a sequence
  of maps with `:root-pid` and :triples keys. `join` is a sequence of flake
  chunks grouped by subject."
  [solution property-families db join]
  ;; Build a map from property ID (parent or child) to the root property's triples
  (let [pid->triples (reduce (fn [pid-map {:keys [_root-pid triples]}]
                               (with-family db pid-map triples))
                             {} property-families)]
    (loop [[s-chunk & r] join
           new-solutions [solution]]
      (if s-chunk
        (let [pid            (->> s-chunk first flake/p)
              triples        (get pid->triples pid)
              new-solutions* (match-subject-property new-solutions triples db s-chunk)]
          (recur r new-solutions*))
        new-solutions))))

(defn expand-property-children
  "Expands a triple to include child properties if they exist.

  Handles both rdfs:subPropertyOf and owl:equivalentProperty relationships by
  querying the :childProps from the schema. Note that equivalent properties are
  stored bidirectionally as subproperties, so querying prop-a will also query
  prop-b if they are equivalent.

  Expansion is non-recursive (one level only) to match the behavior of match-triple.

  Returns a map with :root-pid (the original property ID) and :triples (expanded
  triples including the parent property plus all child properties)."
  [db [s p o :as triple]]
  (let [root-pid (where/get-sid p db)
        triples  [triple]]
    (if-some [child-props (and root-pid (where/get-child-properties db root-pid))]
      (let [db-alias (:alias db)
            triples* (into triples
                           (map (fn [child-pid]
                                  (let [p* (where/match-sid p db-alias child-pid)]
                                    [s p* o])))
                           child-props)]
        {:root-pid root-pid
         :triples  triples*})
      {:root-pid root-pid
       :triples  triples})))

(defn get-property-ids
  "Extracts all property IDs from a property family's triples."
  [db property-family]
  (into #{}
        (map (fn [[_s p _o]]
               (where/get-sid p db)))
        (:triples property-family)))

(defn group-by-pid
  "Builds a map from each property ID to all families containing it.

  This is used to find which families share properties, which is necessary for
  merging equivalent properties correctly."
  [db property-families]
  (reduce (fn [acc family]
            (let [pids (get-property-ids db family)]
              (reduce (fn [m pid]
                        (update m pid (fnil conj #{}) family))
                      acc
                      pids)))
          {} property-families))

(defn group-shared-properties
  "Finds all property families transitively connected to the seed family via shared properties.

  Uses breadth-first search to find all families that share at least one
  property ID with the seed family or with any family connected to it.

  Returns a set of all connected families including the seed."
  [db pid->families seed]
  (loop [remaining-pids #{seed}
         visited-pids   #{}]
    (if-let [current-pid (first remaining-pids)]
      (let [remaining-pids*  (disj remaining-pids current-pid)
            related-families (->> (get-property-ids db current-pid)
                                  (map (fn [pid]
                                         (get pid->families pid)))
                                  (apply set/union))
            new-families     (set/difference related-families visited-pids)]
        (recur (into remaining-pids* new-families)
               (conj visited-pids current-pid)))
      visited-pids)))

(defn merge-shared-properties
  "Merges a set of connected property families into a single family.

  Takes all triples from all families, removes duplicates, and creates a new
  family with the minimum root-pid from the connected families.

  Returns a merged property family map with :root-pid and :triples keys."
  [connected]
  (let [min-root    (->> connected
                         (map :root-pid)
                         (apply min))
        all-triples (->> connected
                         (mapcat :triples)
                         distinct)]
    {:root-pid min-root
     :triples  all-triples}))

(defn merge-related-property-families
  "Merges property families that share any properties (handles equivalent properties).

  When properties are equivalent (owl:equivalentProperty), they appear in each
  other's childProps. This means querying either property will expand to include
  both. If the query has separate patterns for both properties, we need to merge
  them into a single family so the join treats them as the same constraint.

  Uses a union-find-like algorithm to group families that share properties.

  Returns a sequence of merged property families."
  [db property-families]
  (if (empty? property-families)
    []
    (let [pid->families (group-by-pid db property-families)]
      ;; Group families that are transitively connected via shared properties
      (loop [remaining (set property-families)
             result    []]
        (if (empty? remaining)
          result
          (let [seed             (first remaining)
                related-families (group-shared-properties db pid->families seed)
                merged-family    (merge-shared-properties related-families)]
            (recur (set/difference remaining related-families)
                   (conj result merged-family))))))))

(defn assign-patterns
  "Assigns matched values to patterns and expands properties with children.
  Returns a sequence of maps with :root-pid and :triples keys."
  [db solution patterns]
  (->> patterns
       (map (fn [pattern]
              (-> pattern
                  where/pattern-data ; extract triple from
                                     ; class patterns
                  (where/assign-matched-values solution))))
       (map (partial where/compute-sids db))
       (map (partial expand-property-children db))
       (merge-related-property-families db)))

(defn match-properties
  [db tracker solution patterns error-ch]
  (if (and (index/supports? db :psot)
           (index/supports? db :post))
    (let [property-families (assign-patterns db solution patterns)
          property-ranges   (->> property-families
                                 (mapcat :triples)
                                 (map (fn [[_s _p o :as triple]]
                                        (let [idx (if (where/matched? o) :post :psot)]
                                          (where/resolve-flake-range db tracker error-ch triple idx))))
                                 (repartition-each-by flake/s))
          extract-sid       (comp flake/s first)
          join-xf           (mapcat (fn [join]
                                      (match-join solution property-families db join)))]
      (inner-join-by flake/cmp-sid extract-sid 2 join-xf property-ranges))
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
