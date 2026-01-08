(ns fluree.db.query.exec.where
  (:require [clojure.core.async :as async :refer [>! go]]
            [clojure.set :as set]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.range :as query-range]
            [fluree.db.track :as track]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? empty-channel]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.xhttp :as xhttp]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (clojure.lang MapEntry))))

#?(:clj (set! *warn-on-reflection* true))

(def unmatched
  {})

(defn unmatched-var
  [var-sym]
  (assoc unmatched ::var var-sym))

(defn optional-var
  [var-sym]
  (-> (unmatched-var var-sym)
      (assoc ::optional var-sym)))

(defn get-optional
  [match]
  (::optional match))

(defn match-value
  ([mch x]
   (assoc mch ::val x))
  ([mch x dt-iri]
   (-> mch
       (match-value x)
       (assoc ::datatype-iri dt-iri))))

(defn matched-value?
  [match]
  (-> match ::val some?))

(defn get-value
  [match]
  (::val match))

(defn get-variable
  [match]
  (::var match))

(defn get-datatype-iri
  [mch]
  (if (or (contains? mch ::iri)
          (contains? mch ::sids))
    const/iri-id
    (::datatype-iri mch)))

(defn match-sid
  [iri-mch db-alias sid]
  (update iri-mch ::sids assoc db-alias sid))

(defn match-iri
  ([iri]
   (match-iri unmatched iri))
  ([mch iri]
   (assoc mch ::iri iri)))

(defn matched-iri?
  [match]
  (-> match ::iri some?))

(defn iri-datatype?
  [mch]
  (-> mch get-datatype-iri (= const/iri-id)))

(defn get-iri
  [match]
  (cond
    (matched-iri? match)  (::iri match)
    (iri-datatype? match) (get-value match)))

(defn matched-sid?
  [mch]
  (and (map? mch)
       (contains? mch ::sids)))

(defn get-sid
  [iri-mch db]
  (let [db-alias (:alias db)]
    (get-in iri-mch [::sids db-alias])))

(defn match-meta
  [mch m]
  (assoc mch ::meta m))

(defn get-meta
  [match]
  (::meta match))

(defn match-lang
  [mch value lang]
  (-> mch
      (match-value value const/iri-lang-string)
      (update ::meta assoc :lang lang)))

(defn get-lang
  [mch]
  (-> mch get-meta (get :lang)))

(defn match-transaction
  [mch t]
  (assoc mch ::t t))

(defn get-transaction
  [mch]
  (::t mch))

(defn add-transitivity
  [mch tag]
  (assoc mch ::recur tag))

(defn remove-transitivity
  [mch]
  (dissoc mch ::recur))

(defn get-transitive-property
  [mch]
  (::recur mch))

(defn matched?
  [match]
  (or (matched-value? match)
      (matched-iri? match)
      (matched-sid? match)))

(defn get-binding
  [match]
  (or (get-value match)
      (get-iri match)))

(defn all-matched?
  [[s p o]]
  (and (matched-iri? s)
       (matched-iri? p)
       (matched? o)))

(def unmatched?
  "Returns true if the triple pattern component `match` represents a variable
  without an associated value."
  (complement matched?))

(defn untyped-value
  [v]
  (match-value unmatched v))

(defn anonymous-value
  "Build a pattern that already matches an explicit value."
  ([v]
   (let [dt-iri (datatype/infer-iri v)]
     (anonymous-value v dt-iri)))
  ([v dt-iri]
   (match-value unmatched v dt-iri)))

(defn unmatched-var?
  [match]
  (and (contains? match ::var)
       (unmatched? match)))

(defn link-var
  [mch var-type var]
  (assoc-in mch [::linked-vars var-type] var))

(defn get-linked-vars
  [mch]
  (::linked-vars mch))

(defn linked-vars?
  [mch]
  (contains? mch ::linked-vars))

(defn unlink-vars
  [mch]
  (dissoc mch ::linked-vars))

(defn link-lang-var
  [mch var]
  (link-var mch :lang (symbol var)))

(defn link-dt-var
  [mch var]
  (link-var mch :dt (symbol var)))

(defn link-t-var
  [mch var]
  (link-var mch :t (symbol var)))

(defn sanitize-match
  [match]
  (select-keys match [::iri ::val ::datatype-iri ::sids]))

(defn ->pattern
  "Build a new non-tuple match pattern of type `typ`."
  [typ data]
  #?(:clj  (MapEntry/create typ data)
     :cljs (MapEntry. typ data nil)))

(defn ->iri-ref
  [x]
  {::iri x})

(defn variable?
  [sym]
  (and (symbol? sym)
       (-> sym
           name
           first
           (= \?))))

(defn matched-lang?
  [mch lang]
  (-> mch get-lang (= lang)))

(defn lang-matcher
  "Return a function that returns true if the language metadata of a matched
  pattern equals the supplied language code `lang`."
  [lang]
  (fn [soln mch]
    (if (variable? lang)
      (if-let [lang* (some-> soln (get lang) get-value)]
        (matched-lang? mch lang*)
        true)
      (matched-lang? mch lang))))

(defn matched-datatype?
  [mch dt-iri]
  (-> mch get-datatype-iri (= dt-iri)))

(defn datatype-matcher
  "Return a function that returns true if the datatype of a matched pattern equals
  the supplied datatype iri `type`."
  [type context]
  (fn [soln mch]
    (if (variable? type)
      (if-let [dt-iri (some-> soln (get type) get-iri)]
        (matched-datatype? mch dt-iri)
        true)
      (let [dt-iri (json-ld/expand-iri type context)]
        (matched-datatype? mch dt-iri)))))

(defn matched-transaction?
  [mch t]
  (-> mch get-transaction (= t)))

(defn transaction-matcher
  [t]
  (fn [soln mch]
    (if (variable? t)
      (if-let [t* (some-> soln (get t) get-value)]
        (matched-transaction? mch t*)
        true)
      (matched-transaction? mch t))))

(defn with-filter
  [mch f]
  (assoc mch ::fn f))

(defn ->var-filter
  "Build a query function specification for the variable `var` out of the
  parsed function `f`."
  [var f]
  (-> var
      unmatched-var
      (with-filter f)))

(defn ->predicate
  "Build a pattern that already matches the explicit predicate value `value`."
  ([iri]
   (->iri-ref iri))
  ([iri reverse]
   (cond-> (->predicate iri)
     reverse (assoc ::reverse true))))

(defn get-reverse
  [mch]
  (::reverse mch))

(defn ->where-clause
  "Build a pattern that matches all the patterns in the supplied `patterns`
  collection."
  [patterns]
  (vec patterns))

(defprotocol Matcher
  (-match-id [s tracker solution s-match error-ch])
  (-match-triple [s tracker solution triple error-ch])
  (-match-class [s tracker solution triple error-ch])
  (-match-properties [s tracker solution triples error-ch])
  (-activate-alias [s alias])
  (-aliases [s])
  (-finalize [s tracker error-ch solution-ch]))

;; Optional extension point to allow a DB to execute an entire GRAPH clause at once
(defprotocol GraphClauseExecutor
  (-execute-graph-clause [db tracker solution clause error-ch]
    "Return a channel of solutions for the entire GRAPH clause. If not implemented or returns nil, the engine will fall back to per-pattern matching."))

(defn matcher?
  [x]
  (satisfies? Matcher x))

(defn pattern-type
  [pattern]
  (if (map-entry? pattern)
    (key pattern)
    :tuple))

(defn pattern-data
  [pattern]
  (if (map-entry? pattern)
    (val pattern)
    pattern))

(defn class-pattern?
  [pattern-type]
  (= :class pattern-type))

(defmulti match-pattern
  "Return a channel that will contain all pattern match solutions from flakes in
   `db` that are compatible with the initial solution `solution` and matches the
   additional where-clause pattern `pattern`."
  (fn [_ds _tracker _solution pattern _error-ch]
    (pattern-type pattern)))

(defn assign-solution-filter
  [component solution]
  (if (::fn component)
    (update component ::fn partial solution)
    component))

(defn assign-matched-component
  [component solution]
  (let [component* (assign-solution-filter component solution)]
    (if-let [match (some->> component ::var (get solution))]
      match
      component*)))

(defn assign-matched-values
  "Assigns the value of any variables within the supplied `triple-pattern` that
  were previously matched in the supplied solution map `solution` to their
  values from `solution`."
  [triple-pattern solution]
  (mapv (fn [component]
          (assign-matched-component component solution))
        triple-pattern))

(defn match-subject
  "Matches the subject of the supplied `flake` to the triple subject pattern
  component `s-match`, and marks the matched pattern component as a URI data
  type."
  [s-match db flake]
  (let [alias (:alias db)
        sid   (flake/s flake)
        s-iri (iri/decode-sid db sid)]
    (-> s-match
        (match-sid alias sid)
        (match-iri s-iri))))

(defn match-predicate
  "Matches the predicate of the supplied `flake` to the triple predicate pattern
  component `p-match`, and marks the matched pattern component as a URI data
  type."
  [p-match db flake]
  (let [alias    (:alias db)
        pid      (flake/p flake)
        p-iri    (iri/decode-sid db pid)]
    (-> p-match
        (match-sid alias pid)
        (match-iri p-iri))))

(defn match-object
  "Matches the object, data type, and metadata of the supplied `flake` to the
  triple object pattern component `o-match`."
  [o-match db flake]
  (let [o-match* (-> o-match
                     (match-transaction (flake/t flake))
                     (match-meta (flake/m flake)))
        dt (flake/dt flake)]
    (if (= const/$id dt)
      (let [alias (:alias db)
            oid   (flake/o flake)
            o-iri (iri/decode-sid db oid)]
        (-> o-match*
            (match-sid alias oid)
            (match-iri o-iri)))
      (let [dt-iri (iri/decode-sid db dt)]
        (match-value o-match* (flake/o flake) dt-iri)))))

(defn match-linked-datatype
  [var db flake]
  (let [var-mch (unmatched-var var)
        dt-sid (flake/dt flake)
        ;; Get datatype - either explicit from flake or inferred from value
        dt-iri (if dt-sid
                 (iri/decode-sid db dt-sid)
                 (datatype/infer-iri (flake/o flake)))]
    (match-iri var-mch dt-iri)))

(defn match-linked-lang
  [var flake]
  (let [var-mch (unmatched-var var)
        lang    (-> flake flake/m :lang)]
    (match-value var-mch lang const/iri-string)))

(defn match-linked-t
  [var flake]
  (let [var-mch (unmatched-var var)
        t       (flake/t flake)]
    (match-value var-mch t const/iri-long)))

(defn match-linked-var
  [var-type linked-var db flake]
  (case var-type
    :dt   (match-linked-datatype linked-var db flake)
    :lang (match-linked-lang linked-var flake)
    :t    (match-linked-t linked-var flake)))

(defn match-linked-vars
  [solution o-mch db flake]
  (reduce (fn [soln [var-type linked-var]]
            (let [var-mch (match-linked-var var-type linked-var db flake)]
              (assoc soln linked-var var-mch)))
          solution (get-linked-vars o-mch)))

(defn match-flake
  "Assigns the unmatched variables within the supplied `triple-pattern` to their
  corresponding values from `flake` in the supplied match `solution`."
  [solution triple-pattern db flake]
  (let [[s p o] triple-pattern]
    (cond-> solution
      (unmatched-var? s) (assoc (::var s) (match-subject s db flake))
      (unmatched-var? p) (assoc (::var p) (match-predicate p db flake))
      (unmatched-var? o) (assoc (::var o) (-> o
                                              unlink-vars
                                              (match-object db flake)))
      (linked-vars? o)   (match-linked-vars o db flake))))

(defn augment-object-fn
  "Returns a pair consisting of an object value and boolean function that will
  return false when applied to object values whose flake should be filtered out
  of query results. This function augments the original object function supplied
  in an object pattern under the `::fn` key (if any) by also checking if a
  prospective flake object is equal to the supplied `o` value if and only if the
  `:spot` index is used, the `p` value is `nil`, and the `s` and `o` values are
  not `nil`. In this case, the new object value returned by this function will
  be changed to `nil`. This ensures that all necessary flakes are considered
  from the spot index when scanned, and this is necessary because the `p` value
  is `nil`."
  [db idx s p o o-fn]
  (if (or (and (#{:spot} idx)
               (nil? p)
               s
               o)
          (and (#{:psot} idx)
               (nil? s)
               p
               o))
    (let [match-fn (fn [mch]
                     (when-let [v (or (get-value mch)
                                      (get-sid mch db))]
                       (= o v)))
          o-fn*    (if o-fn
                     (fn [mch]
                       (and (match-fn mch)
                            (o-fn mch)))
                     match-fn)]
      [nil o-fn*])
    [o o-fn]))

(defn comparable-iri?
  "When matching against an all-iri index (s or p - SIDs), the only values that can be
  compared are other SIDs or `nil`. Literal values are not comparable."
  [x]
  (or (iri/sid? x) (nil? x)))

(defn unmatched-optional-vars?
  "A triple pattern with any match components that are empty optional vars."
  [triple-pattern]
  (not-empty (keep get-optional triple-pattern)))

(defn resolve-flake-range
  ([db tracker error-ch pattern]
   (resolve-flake-range db tracker error-ch pattern nil))
  ([{:keys [t] :as db} tracker error-ch [s-mch p-mch o-mch :as triple-pattern] idx]
   (if (unmatched-optional-vars? triple-pattern)
     empty-channel
     (let [s (or (get-sid s-mch db)
                 (get-value s-mch))
           p (or (get-sid p-mch db)
                 (get-value p-mch))
           o (or (get-value o-mch)
                 (get-sid o-mch db))
           ;; Optional range constraint extracted from inlined single-var filters.
           ;; Used to narrow scans by setting start/end object bounds when possible.
           o-range (::range o-mch)]
       (if (or (not (comparable-iri? s))
               (not (comparable-iri? p)))
         ;; no flakes will ever match the given triple pattern
         empty-channel
         (let [s-fn (::fn s-mch)
               p-fn (::fn p-mch)
               o-fn (::fn o-mch)
               o-dt (some->> o-mch get-datatype-iri (iri/encode-iri db))

               idx*        (or idx
                               (try* (index/for-components s p o o-dt)
                                     (catch* e
                                       (log/error e "Error resolving flake range")
                                       (async/put! error-ch e))))
               [o* o-fn*]  (augment-object-fn db idx* s p o o-fn)
               ;; Use range bounds from filter analysis when object isn't explicitly bound
               o-start     (cond
                             (some? o*) o*
                             (some? (:start-o o-range)) (:start-o o-range)
                             :else nil)
               o-end       (cond
                             (some? o*) o*
                             (some? (:end-o o-range)) (:end-o o-range)
                             :else nil)
               start-flake (flake/create s p o-start o-dt nil nil util/min-integer)
               end-flake   (flake/create s p o-end o-dt nil nil util/max-integer)
               track-fuel  (track/track-fuel! tracker error-ch)
               subj-filter (when s-fn
                             (filter (fn [f]
                                       (-> unmatched
                                           (match-subject db f)
                                           s-fn))))
               pred-filter (when p-fn
                             (filter (fn [f]
                                       (-> unmatched
                                           (match-predicate db f)
                                           p-fn))))
               obj-filter  (when o-fn*
                             (filter (fn [f]
                                       (-> unmatched
                                           (match-object db f)
                                           o-fn*))))
               flake-xf    (->> [subj-filter pred-filter obj-filter track-fuel]
                                (remove nil?)
                                (apply comp))
               opts        {:idx         idx*
                            :to-t        t
                            :start-flake start-flake
                            :end-flake   end-flake
                            :flake-xf    flake-xf}]
           (query-range/resolve-flake-slices db tracker idx* error-ch opts)))))))

(defn compute-sid
  [s-mch db]
  (if (and (matched-iri? s-mch)
           (not (get-sid s-mch db)))
    (let [db-alias (:alias db)
          s-iri    (::iri s-mch)]
      (when-let [sid (iri/encode-iri db s-iri)]
        (match-sid s-mch db-alias sid)))
    s-mch))

(defn compute-datatype-sid
  [o-mch db]
  (let [db-alias (:alias db)]
    (if-let [dt-iri (::datatype-iri o-mch)]
      (when-let [sid (iri/encode-iri db dt-iri)]
        (assoc-in o-mch [::datatype-sid db-alias] sid))
      o-mch)))

(defn compute-sids
  [db [s p o]]
  (let [s* (compute-sid s db)
        p* (compute-sid p db)
        o* (if (unmatched-var? o)
             o
             (if (matched-iri? o)
               (compute-sid o db)
               (compute-datatype-sid o db)))]
    (when (and (some? s*) (some? p*) (some? o*))
      [s* p* o*])))

(defn get-child-properties
  [db prop]
  (-> db
      (get-in [:schema :pred prop :childProps])
      not-empty))

(defmethod match-pattern :id
  [ds tracker solution pattern error-ch]
  (let [s-mch (pattern-data pattern)]
    (-match-id ds tracker solution s-mch error-ch)))

(defmethod match-pattern :tuple
  [ds tracker solution pattern error-ch]
  (let [tuple (pattern-data pattern)]
    (-match-triple ds tracker solution tuple error-ch)))

(defmethod match-pattern :class
  [ds tracker solution pattern error-ch]
  (let [triple (pattern-data pattern)]
    (-match-class ds tracker solution triple error-ch)))

(defmethod match-pattern :property-join
  [ds tracker solution pattern error-ch]
  (let [triples (pattern-data pattern)]
    (-match-properties ds tracker solution triples error-ch)))

(defn filter-exception
  "Reformats raw filter exception to try to provide more useful feedback."
  [e f]
  (let [fn-str (->> f meta :forms (str/join " "))
        ex-msg (or (ex-message e)
                   ;; note: NullPointerException is common but has no ex-message, create one
                   (let [ex-type (str (type e))] ;; attempt to make JS compatible
                     (if (= ex-type "class java.lang.NullPointerException")
                       "Variable has null value, cannot apply filter"
                       "Unknown error")))
        e*     (ex-info (str "Exception in statement '[filter " fn-str "]': " ex-msg)
                        {:status 400
                         :error  :db/invalid-query}
                        e)]
    (log/warn (ex-message e*))
    e*))

;; this is the arg and return type of all built-in functions
(defrecord TypedValue [value datatype-iri lang])

(defn ->typed-val
  ([value] (->TypedValue value (datatype/infer-iri value) nil))
  ([value dt-iri]
   (->TypedValue (when (some? value) (datatype/coerce value dt-iri)) dt-iri nil))
  ([value dt-iri lang]
   (->TypedValue (when (some? value) (datatype/coerce value dt-iri)) dt-iri lang)))

(defn mch->typed-val
  [{::keys [val iri datatype-iri meta]}]
  (->typed-val (or iri val) (if iri const/iri-id datatype-iri) (:lang meta)))

(defn typed-val->mch
  [mch {v :value dt :datatype-iri lang :lang}]
  (if (= dt const/iri-id)
    (match-iri mch v)
    (if lang
      (match-lang mch v lang)
      (match-value mch v dt))))

(defmethod match-pattern :filter
  [_ds _tracker solution pattern error-ch]
  (go
    (let [f (pattern-data pattern)]
      (try*
        (let [result (f solution)]
          (when result
            solution))
        (catch* e (>! error-ch (filter-exception e f)))))))

(defn with-constraint
  "Return a channel of all solutions from the data set `ds` that extend from the
  solutions in `solution-ch` and also match the where-clause pattern `pattern`."
  [ds tracker pattern error-ch solution-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (-> (match-pattern ds tracker solution pattern error-ch)
                                (async/pipe ch)))
                          solution-ch)
    out-ch))

(defn match-patterns
  [ds tracker solution patterns error-ch]
  (reduce (fn [solution-ch pattern]
            (with-constraint ds tracker pattern error-ch solution-ch))
          (async/to-chan! [solution])
          patterns))

(defn subquery?
  [pattern]
  (and (sequential? pattern)
       (= :query (first pattern))))

(defn match-clause
  "Returns a channel that will eventually contain all match solutions in the
  dataset `ds` extending from `solution` that also match all the patterns in the
  parsed where clause collection `clause`."
  [ds tracker solution clause error-ch]
  (let [{subquery-patterns true
         other-patterns    false} (group-by subquery? clause)

        ;; process subqueries before other patterns
        patterns  (into (vec subquery-patterns) other-patterns)
        result-ch (match-patterns ds tracker solution patterns error-ch)]
    (-finalize ds tracker error-ch result-ch)))

(defn match-alias
  [ds alias tracker solution clause error-ch]
  (let [res-ch (async/chan)]
    (go
      (try*
        (when-let [graph (<? (-activate-alias ds alias))]
          (if (satisfies? GraphClauseExecutor graph)
            (if-let [ch (-execute-graph-clause graph tracker solution clause error-ch)]
              (async/pipe ch res-ch)
              (-> (match-clause graph tracker solution clause error-ch)
                  (async/pipe res-ch)))
            (-> (match-clause graph tracker solution clause error-ch)
                (async/pipe res-ch))))
        (catch* e
          (log/error e "Error activating alias" alias)
          (>! error-ch (ex-info (str "Error activating alias: " alias
                                     " due to exception: " (ex-message e))
                                {:status 400, :error :db/invalid-query}
                                e))
          (async/close! res-ch))))
    res-ch))

(defmethod match-pattern :exists
  [ds tracker solution pattern error-ch]
  (let [clause (pattern-data pattern)]
    (go
      ;; exists uses existing bindings
      (when (async/<! (match-clause ds tracker solution clause error-ch))
        solution))))

(defmethod match-pattern :not-exists
  [ds tracker solution pattern error-ch]
  ;; not exists removes a pattern
  (let [clause (pattern-data pattern)]
    (go
      ;; not-exists uses existing bindings
      (when-not (async/<! (match-clause ds tracker solution clause error-ch))
        solution))))

(defmethod match-pattern :minus
  [ds tracker solution pattern error-ch]
  ;; minus performs a set difference, removing a provided solution if the same solution
  ;; produced by the minus pattern
  (let [clause   (pattern-data pattern)
        minus-ch (async/chan 2 (filter (fn [minus-solution]
                                         ;; only keep minus-solutions that match the provided solution
                                         (and (not-empty minus-solution)
                                              (= minus-solution (select-keys solution (keys minus-solution)))))))]
    (go
      ;; minus does not use existing bindings
      ;; if a minus solutions equals the provided solution, remove the provided solution
      (when-not (-> (match-clause ds tracker {} clause error-ch)
                    (async/pipe minus-ch)
                    (async/<!))
        solution))))

(defmethod match-pattern :query
  [ds tracker solution pattern error-ch]
  (let [subquery-fn (pattern-data pattern)
        out-ch (async/chan 2 (map (fn [soln] (merge solution soln))))]
    (async/pipe (subquery-fn ds tracker error-ch) out-ch)))

(defmethod match-pattern :graph
  [ds tracker solution pattern error-ch]
  (let [[g clause] (pattern-data pattern)]
    (if-let [v (get-variable g)]
      (if-let [v-match (get solution v)]
        (let [alias (or (get-iri v-match)
                        (get-value v-match))]
          (match-alias ds alias tracker solution clause error-ch))
        (let [out-ch   (async/chan)
              alias-ch (-> ds -aliases async/to-chan!)]
          (async/pipeline-async 2
                                out-ch
                                (fn [alias ch]
                                  (let [solution* (update solution v match-iri alias)]
                                    (-> (match-alias ds alias tracker solution* clause error-ch)
                                        (async/pipe ch))))
                                alias-ch)
          out-ch))
      (match-alias ds g tracker solution clause error-ch))))

(defmethod match-pattern :union
  [db tracker solution pattern error-ch]
  (let [clauses   (pattern-data pattern)
        clause-ch (async/to-chan! clauses)
        out-ch    (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [clause ch]
                            (-> (match-clause db tracker solution clause error-ch)
                                (async/pipe ch)))
                          clause-ch)
    out-ch))

(defmethod match-pattern :values
  [_db _tracker solution pattern _error-ch]
  (let [inline-solutions (pattern-data pattern)
        ;; transform a match into its identity for equality checks
        match-identity   (juxt get-iri get-value get-datatype-iri (comp get-meta :lang))
        solution*        (update-vals solution match-identity)]
    (->> inline-solutions
         ;; filter out any inline solutions whose matches don't match the solution's matches
         (filterv (fn [inline-solution]
                    (let [matches (not-empty (select-keys solution* (keys inline-solution)))]
                      (or
                        ;; no overlapping matches
                       (nil? matches)
                        ;; matches are the same
                       (= matches (update-vals inline-solution match-identity))))))
         (mapv (fn [inline-solution]
                 (let [existing-vars (set (keys solution))
                       inline-vars   (set (keys inline-solution))
                       new-vars      (set/difference inline-vars existing-vars)]
                   ;; don't clobber existing vars, only add new data
                   (reduce (fn [solution new-var] (assoc solution new-var (get inline-solution new-var)))
                           solution
                           new-vars))))
         (async/to-chan!))))

(defn with-default
  "Return a transducer that transforms an input stream of solutions to include the
  `default-solution` if and only if the stream was empty."
  [default-solution]
  (fn [rf]
    (let [solutions? (volatile! false)]
      (fn
        ;; Initialization: do nothing but initialize the supplied reducing fn.
        ([]
         (rf))

        ;; Iteration: mark that a solution was processed, and pass it to the supplied
        ;; reducing fn.
        ([result solution]
         (vreset! solutions? true)
         (rf result solution))

        ;; Termination: if no other solutions were processed, then process the
        ;; default-solution with the supplied reducing fn before terminating it;
        ;; terminate as normal otherwise.
        ([result]
         (if @solutions?
           (rf result)
           (do (vreset! solutions? true) ; mark that a solution was processed in
                                         ; case the reducing fn is terminated
                                         ; again as can happen with buffers.
               (-> result
                   (rf default-solution)
                   rf))))))))

(defn clause-variables
  [where]
  (cond
    (nil? where) #{}
    (sequential? where) (into #{} (mapcat clause-variables) where)
    (map? where) (if (contains? where ::var)
                   #{(::var where)}
                   (into #{} (mapcat clause-variables) where))))

(defn assign-unmatched-optional-vars
  "In the case where an optional clause returns no results, add all the vars of that
  clause to the solution as optional vars. This is important if the vars are referenced
  in subsequent patterns so that we don't erroneously consider them unmatched and
  therefore able to match any value."
  [solution optional-vars]
  (if (empty? optional-vars)
    solution
    ;; Use a transient map so we can add optional vars in a single pass
    ;; with `assoc!` while keeping the original persistent map semantics.
    (let [t-solution (transient solution)]
      (persistent!
       (reduce (fn [sol var]
                 (let [match (get sol var)]
                   (if (nil? match)
                     (assoc! sol var (optional-var var))
                     sol)))
               t-solution
               optional-vars)))))

(defmethod match-pattern :optional
  [db tracker solution pattern error-ch]
  (let [clause    (pattern-data pattern)
        solution* (assign-unmatched-optional-vars solution (clause-variables clause))
        opt-ch    (async/chan 2 (with-default solution*))]
    (-> (match-clause db tracker solution clause error-ch)
        (async/pipe opt-ch))))

(defn update-solution-binding
  [solution var-name mch]
  (if-let [current (get solution var-name)]
    (when (and (= (get-binding mch) (get-binding current))
               (= (get-datatype-iri mch) (get-datatype-iri current))
               (= (get-lang mch) (get-lang current)))
      solution)
    (assoc solution var-name mch)))

(defmethod match-pattern :bind
  [_db _tracker solution pattern error-ch]
  (go
    (let [binds     (-> pattern pattern-data vals)
          solution* (reduce (fn [soln b]
                              (let [f        (::fn b)
                                    var-name (::var b)]
                                (try*
                                  (if f
                                    (let [result (f soln)
                                          result-mch (typed-val->mch (unmatched-var var-name) result)]
                                      (or (update-solution-binding soln var-name result-mch)
                                          (assoc soln ::invalidated true)))
                                    ;; static binding
                                    (or (update-solution-binding soln var-name b)
                                        (assoc soln ::invalidated true)))
                                  (catch* e (update soln ::errors conj e)))))
                            solution binds)]
      (if-let [errors (::errors solution*)]
        (async/onto-chan! error-ch errors)
        (when-not (::invalidated solution*)
          solution*)))))

(defn binding->solution
  [solution vars binding]
  ;; Build up a solution for this SPARQL binding using a transient map so
  ;; multiple var additions happen against a single, mutable backing map.
  ;; This mirrors `update-solution-binding` semantics while avoiding repeated
  ;; persistent map churn in tight loops.
  (let [t-solution (transient solution)]
    (loop [sol t-solution
           vs  vars]
      (if-let [var (first vs)]
        (if-let [{type "type" v "value" dt "datatype" lang "xml:lang"}
                 (get binding var)]
          (let [var-name (symbol (str "?" var))
                mch      (cond
                           (= "literal" type)
                           (cond-> (-> (unmatched-var var-name)
                                       (match-value v dt))
                             lang (match-lang v lang))

                           (#{"uri" "bnode"} type)
                           (-> (unmatched-var var-name)
                               (match-iri v))

                           :else
                           (throw (ex-info "Invalid SPARQL Query Results JSON Format."
                                           {:status 400, :error :db/invalid-query
                                            :spec "https://www.w3.org/TR/sparql11-results-json"
                                            :binding binding})))
                current  (get sol var-name)]
            ;; If there is an existing binding for this var, only keep it if it
            ;; exactly matches; otherwise abort the join for this binding.
            (if current
              (if (and (= (get-binding mch) (get-binding current))
                       (= (get-datatype-iri mch) (get-datatype-iri current))
                       (= (get-lang mch) (get-lang current)))
                (recur sol (rest vs))
                ;; conflicting binding – no join for this binding
                nil)
              (recur (assoc! sol var-name mch) (rest vs))))
          ;; No binding for this var in this row; move on.
          (recur sol (rest vs)))
        ;; All vars processed successfully – return persistent solution.
        (persistent! sol)))))

(defn sparql-service-error!
  [ex service sparql-q]
  (log/error ex "Error processing service response " service sparql-q)
  (ex-info (str "Error processing service response " service " due to exception: " (ex-message ex))
           {:status 400, :error :db/invalid-query}
           ex))

(defmethod match-pattern :service
  [_db _tracker solution pattern error-ch]
  (let [{:keys [service silent? sparql-q]} (pattern-data pattern)
        solution-ch                        (async/chan)]
    (go
      (let [response (async/<! (xhttp/post service sparql-q
                                           {:headers {"Content-Type" "application/sparql-query"
                                                      "Accept"       "application/sparql-results+json"}}))]
        (if (util/exception? response)
          (if silent?
            (async/onto-chan! solution-ch [solution])
            (async/>! error-ch (sparql-service-error! response service sparql-q)))
          (try*
            (let [response* (json/parse response false)
                  vars      (-> response* (get "head") (get "vars"))
                  bindings  (-> response* (get "results") (get "bindings"))]
              (->> bindings
                   (keep (partial binding->solution solution vars))
                   (async/onto-chan! solution-ch)))
            (catch* e (async/>! error-ch (sparql-service-error! e service sparql-q)))))))
    solution-ch))

(defmethod match-pattern :default
  [_db _tracker _solution pattern error-ch]
  (go
    (>! error-ch
        (ex-info (str "Unknown pattern type: " (pattern-type pattern))
                 {:status 400
                  :error  :db/invalid-query}))))

(def blank-solution {})

(defn values-initial-solution
  [q]
  (-> q
      :values
      not-empty
      (or [blank-solution])
      async/to-chan!))

(defn search
  ([ds q tracker error-ch]
   (search ds q tracker error-ch nil))
  ([ds q tracker error-ch initial-solution-ch]
   (let [out-ch               (async/chan 2)
         initial-solution-ch* (or initial-solution-ch
                                  (values-initial-solution q))]
     (if-let [where-clause (:where q)]
       (async/pipeline-async 2
                             out-ch
                             (fn [initial-solution ch]
                               (-> (match-clause ds tracker initial-solution where-clause error-ch)
                                   (async/pipe ch)))
                             initial-solution-ch*)
       (async/pipe initial-solution-ch* out-ch))
     out-ch)))
