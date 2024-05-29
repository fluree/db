(ns fluree.db.query.exec.where
  (:require [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [>! go]]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.datatype :as datatype]
            [fluree.db.query.dataset :as dataset]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri])
  #?(:clj (:import (clojure.lang MapEntry))))

#?(:clj (set! *warn-on-reflection* true))

(def unmatched
  {})

(defn unmatched-var
  [var-sym]
  (assoc unmatched ::var var-sym))

(defn match-value
  ([mch x dt-iri]
   (assoc mch
     ::val x
     ::datatype-iri dt-iri))
  ([mch x dt-iri m]
   (-> mch
       (match-value x dt-iri)
       (assoc ::meta m))))

(defn match-iri
  ([iri]
   (match-iri unmatched iri))
  ([mch iri]
   (assoc mch ::iri iri)))

(defn get-iri
  [match]
  (::iri match))

(defn matched-iri?
  [match]
  (-> match ::iri some?))

(defn matched-value?
  [match]
  (-> match ::val some?))

(defn match-sid
  [iri-mch db-alias sid]
  (update iri-mch ::sids assoc db-alias sid))

(defn matched-sid?
  [mch]
  (contains? mch ::sids))

(defn get-sid
  [iri-mch db]
  (let [db-alias (:alias db)]
    (get-in iri-mch [::sids db-alias])))

(defn get-datatype-iri
  [mch]
  (if (or (matched-iri? mch)
          (matched-sid? mch))
    const/iri-anyURI
    (::datatype-iri mch)))

(defn matched?
  [match]
  (or (matched-value? match)
      (matched-iri? match)
      (matched-sid? match)))

(defn all-matched?
  [[s p o]]
  (and (matched-iri? s)
       (matched-iri? p)
       (matched? o)))

(def unmatched?
  "Returns true if the triple pattern component `match` represents a variable
  without an associated value."
  (complement matched?))

(defn anonymous-value
  "Build a pattern that already matches an explicit value."
  ([v]
   (let [dt-iri (datatype/infer-iri v)]
     (anonymous-value v dt-iri)))
  ([v dt-iri]
   (match-value unmatched v dt-iri))
  ([v dt-iri m]
   (match-value unmatched v dt-iri m)))

(defn unmatched-var?
  [match]
  (and (contains? match ::var)
       (unmatched? match)))

(defn get-value
  [match]
  (::val match))

(defn get-variable
  [match]
  (::var match))

(defn get-binding
  [match]
  (or (get-value match)
      (get-iri match)))

(defn get-meta
  [match]
  (::meta match))

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

(defn lang-matcher
  "Return a function that returns true if the language metadata of a matched
  pattern equals the supplied language code `lang`."
  [lang]
  (fn [soln mch]
    (let [lang* (if (variable? lang)
                  (-> soln (get lang) get-value)
                  lang)]
      (-> mch ::meta :lang (= lang*)))))

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

(defn ->where-clause
  "Build a pattern that matches all the patterns in the supplied `patterns`
  collection."
  [patterns]
  (vec patterns))

(defprotocol Matcher
  (-match-id [s fuel-tracker solution s-match error-ch])
  (-match-triple [s fuel-tracker solution triple error-ch])
  (-match-class [s fuel-tracker solution triple error-ch]))

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

(defmulti match-pattern
  "Return a channel that will contain all pattern match solutions from flakes in
   `db` that are compatible with the initial solution `solution` and matches the
   additional where-clause pattern `pattern`."
  (fn [_ds _fuel-tracker _solution pattern _error-ch]
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
  (let [dt (flake/dt flake)]
    (if (= const/$xsd:anyURI dt)
      (let [alias (:alias db)
            oid   (flake/o flake)
            o-iri (iri/decode-sid db oid)]
        (-> o-match
            (match-sid alias oid)
            (match-iri o-iri)))
      (let [dt-iri (iri/decode-sid db dt)]
        (match-value o-match (flake/o flake) dt-iri (flake/m flake))))))

(defn match-flake
  "Assigns the unmatched variables within the supplied `triple-pattern` to their
  corresponding values from `flake` in the supplied match `solution`."
  [solution triple-pattern db flake]
  (let [[s p o] triple-pattern]
    (cond-> solution
      (unmatched-var? s) (assoc (::var s) (match-subject s db flake))
      (unmatched-var? p) (assoc (::var p) (match-predicate p db flake))
      (unmatched-var? o) (assoc (::var o) (match-object o db flake)))))


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
  (if (and (#{:spot} idx)
           (nil? p)
           (and s o))
    (let [f (if o-fn
              (fn [mch]
                (and (#{o} (or (get-value mch)
                               (get-sid mch db)))
                     (o-fn mch)))
              (fn [mch]
                (#{o} (get-value mch))))]
      [nil f])
    [o o-fn]))

(defn resolve-flake-range
  ([db fuel-tracker error-ch components]
   (resolve-flake-range db fuel-tracker nil error-ch components))

  ([{:keys [t] :as db} fuel-tracker flake-xf error-ch [s-mch p-mch o-mch]]
   (let [s    (get-sid s-mch db)
         s-fn (::fn s-mch)
         p    (get-sid p-mch db)
         p-fn (::fn p-mch)
         o    (or (get-value o-mch)
                  (get-sid o-mch db))
         o-fn (::fn o-mch)
         o-dt (some->> o-mch get-datatype-iri (iri/encode-iri db))

         idx         (try* (index/for-components s p o o-dt)
                           (catch* e
                             (log/error e "Error resolving flake range")
                             (async/put! error-ch e)))
         [o* o-fn*]  (augment-object-fn db idx s p o o-fn)
         start-flake (flake/create s p o* o-dt nil nil util/min-integer)
         end-flake   (flake/create s p o* o-dt nil nil util/max-integer)
         track-fuel  (when fuel-tracker
                       (fuel/track fuel-tracker error-ch))
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
         flake-xf*   (->> [subj-filter pred-filter obj-filter
                           flake-xf track-fuel]
                          (remove nil?)
                          (apply comp))
         opts        {:idx         idx
                      :from-t      t
                      :to-t        t
                      :start-flake start-flake
                      :end-flake   end-flake
                      :flake-xf    flake-xf*}]
     (query-range/resolve-flake-slices db idx error-ch opts))))


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

(def nil-channel
  (doto (async/chan)
    async/close!))

(defmethod match-pattern :id
  [ds fuel-tracker solution pattern error-ch]
  (let [s-mch (pattern-data pattern)]
    (if-let [active-graph (dataset/active ds)]
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [graph]
                    (-match-id graph fuel-tracker solution s-mch error-ch)))
             async/merge)
        (-match-id active-graph fuel-tracker solution s-mch error-ch))
      nil-channel)))

(defmethod match-pattern :tuple
  [ds fuel-tracker solution pattern error-ch]
  (let [tuple (pattern-data pattern)]
    (if-let [active-graph (dataset/active ds)]
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [graph]
                    (-match-triple graph fuel-tracker solution tuple error-ch)))
             async/merge)
        (-match-triple active-graph fuel-tracker solution tuple error-ch))
      nil-channel)))

(defmethod match-pattern :class
  [ds fuel-tracker solution pattern error-ch]
  (if-let [active-graph (dataset/active ds)]
    (let [triple (pattern-data pattern)]
      (log/debug "active graph:" active-graph)
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [graph]
                    (-match-class graph fuel-tracker solution triple error-ch)))
             async/merge)
        (-match-class active-graph fuel-tracker solution triple error-ch)))
    nil-channel))

(defmethod match-pattern :filter
  [_ds _fuel-tracker solution pattern error-ch]
  (go
    (try*
      (let [f (pattern-data pattern)]
        (when (f solution)
          solution))
      (catch* e
              (log/error e "Error applying filter")
              (>! error-ch e)))))

(defn with-constraint
  "Return a channel of all solutions from the data set `ds` that extend from the
  solutions in `solution-ch` and also match the where-clause pattern `pattern`."
  [ds fuel-tracker pattern error-ch solution-ch]
  (let [out-ch (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [solution ch]
                            (-> (match-pattern ds fuel-tracker solution pattern error-ch)
                                (async/pipe ch)))
                          solution-ch)
    out-ch))

(defn match-clause
  "Returns a channel that will eventually contain all match solutions in the
  dataset `ds` extending from `solution` that also match all the patterns in the
  parsed where clause collection `clause`."
  [ds fuel-tracker solution clause error-ch]
  (let [initial-ch (async/to-chan! [solution])]
    (reduce (fn [solution-ch pattern]
              (with-constraint ds fuel-tracker pattern error-ch solution-ch))
            initial-ch clause)))

(defn match-alias
  [ds alias fuel-tracker solution clause error-ch]
  (-> ds
      (dataset/activate alias)
      (match-clause fuel-tracker solution clause error-ch)))

(defmethod match-pattern :graph
  [ds fuel-tracker solution pattern error-ch]
  (let [[g clause] (pattern-data pattern)]
    (if-let [v (::var g)]
      (if-let [v-match (get solution v)]
        (let [alias (or (::iri v-match)
                        (get-value v-match))]
          (match-alias ds alias fuel-tracker solution clause error-ch))
        (let [out-ch  (async/chan)
              name-ch (-> ds dataset/names async/to-chan!)]
          (async/pipeline-async 2
                                out-ch
                                (fn [alias ch]
                                  (let [solution* (update solution v match-iri alias)]
                                    (-> (match-alias ds alias fuel-tracker solution* clause error-ch)
                                        (async/pipe ch))))
                                name-ch)
          out-ch))
      (match-alias ds g fuel-tracker solution clause error-ch))))

(defmethod match-pattern :union
  [db fuel-tracker solution pattern error-ch]
  (let [clauses   (pattern-data pattern)
        clause-ch (async/to-chan! clauses)
        out-ch    (async/chan 2)]
    (async/pipeline-async 2
                          out-ch
                          (fn [clause ch]
                            (-> (match-clause db fuel-tracker solution clause error-ch)
                                (async/pipe ch)))
                          clause-ch)
    out-ch))

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

(defmethod match-pattern :optional
  [db fuel-tracker solution pattern error-ch]
  (let [clause (pattern-data pattern)
        opt-ch (async/chan 2 (with-default solution))]
    (-> (match-clause db fuel-tracker solution clause error-ch)
        (async/pipe opt-ch))))

(defn add-fn-result-to-solution
  [solution var-name result]
  (let [dt  (datatype/infer-iri result)
        mch (-> var-name
                unmatched-var
                (match-value result dt))]
    (assoc solution var-name mch)))

(defmethod match-pattern :bind
  [_db _fuel-tracker solution pattern error-ch]
  (let [bind (pattern-data pattern)]
    (go
      (let [result
            (reduce (fn [solution* b]
                      (let [f        (::fn b)
                            var-name (::var b)]
                        (try*
                          (->> (f solution)
                               (add-fn-result-to-solution solution* var-name))
                          (catch* e (update solution* ::errors conj e)))))
                    solution (vals bind))]
        (when-let [errors (::errors result)]
          (async/onto-chan! error-ch errors))
        result))))

(def blank-solution {})

(defn search
  [ds q fuel-tracker error-ch]
  (let [out-ch (async/chan 2)
        initial-solution-ch (-> q
                                :values
                                not-empty
                                (or [blank-solution])
                                async/to-chan!)]
    (if-let [where-clause (:where q)]
      (async/pipeline-async 2
                            out-ch
                            (fn [initial-solution ch]
                              (-> (match-clause ds fuel-tracker initial-solution where-clause error-ch)
                                  (async/pipe ch)))
                            initial-solution-ch)
      (async/pipe initial-solution-ch out-ch))
    out-ch))

(defn bound-variables
  [where]
  (cond
    (nil? where) #{}
    (sequential? where) (into #{} (mapcat bound-variables) where)
    (map? where) (if (contains? where ::var)
                   #{(::var where)}
                   (into #{} (mapcat bound-variables) where))))
