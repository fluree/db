(ns fluree.db.json-ld.shacl
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [clojure.set :as set])
  #?(:clj (:import (java.util.regex Pattern))))

(comment
  ;; a raw SHACL shape looks something like this:
  {:id             :ex/UserShape,
   :rdf/type       [:sh/NodeShape],
   :sh/targetClass {:id :ex/User},
   :sh/property    [{:id          "_:f211106232533000",
                     :sh/path     {:id :schema/name},
                     :sh/minCount 1,
                     :sh/maxCount 1,
                     :sh/datatype {:id :xsd/string}}
                    {:id          "_:f211106232533002",
                     :sh/path     {:id :schema/email},
                     :sh/minCount 1,
                     :sh/maxCount 1,
                     :sh/nodeKind {:id :sh/IRI}}]})




;; property min & max
;; -- if new, can just make sure for each property between min and max
;; -- if existing, need to get existing counts

;; property data type
;; - any "adds" just coerce, ignore retractions

;; sh:ignoredProperties - let pass through

;; sh:closed true
;; - have a set of allowed and reject if not in the list
;; - set includes all properties from above + ignoredProperties


(defn apply-flake-changes
  [existing-flakes changed-flakes]
  :TODO)



(defn throw-property-shape-exception!
  [msg]
  (throw (ex-info (str "SHACL PropertyShape exception - " msg ".")
                  {:status 400 :error :db/shacl-validation})))

(def numeric-types
  #{const/$xsd:int
    const/$xsd:short
    const/$xsd:float
    const/$xsd:unsignedLong
    const/$xsd:unsignedInt
    const/$xsd:unsignedShort
    const/$xsd:positiveInteger
    const/$xsd:nonPositiveInteger
    const/$xsd:negativeInteger
    const/$xsd:nonNegativeInteger
    const/$xsd:decimal
    const/$xsd:double
    const/$xsd:integer
    const/$xsd:long})

(def time-types
  #{const/$xsd:date
    const/$xsd:dateTime
    const/$xsd:duration
    const/$xsd:gDay
    const/$xsd:gMonth
    const/$xsd:gMonthDay
    const/$xsd:gYear
    const/$xsd:gYearMonth
    const/$xsd:time})

(defn flake-value
  [flake]
  [(flake/o flake) (flake/dt flake)])

(defn coalesce-validation-results
  ([results] (coalesce-validation-results results nil))
  ([results logical-constraint]
   (log/debug "coalesce-validation-results results:" results)
   (let [results-map   (reduce (fn [acc [valid? err-msg]]
                                 (if err-msg
                                   (update acc (-> valid? str keyword) conj err-msg)
                                   acc))
                               {} results)
         short-circuit (if (= :not logical-constraint) :true :false)
         err-msgs      (get results-map short-circuit)]
     (if (empty? err-msgs)
       [true]
       [false (str/join "; " err-msgs)]))))


(defn validate-string-properties
  "String-based constraint components specify conditions on the string representation of values,
  as defined the SPARQL `str` function. See:

    - https://www.w3.org/TR/shacl/#core-components-string
    - https://www.w3.org/TR/sparql11-query/#func-str

  Therefore, we transform the value to a string (if it isn't one already)
  before performing validation."
  [{:keys [min-length max-length pattern flags logical-constraint] :as _p-shape} p-flakes]
  (let [results (for [flake p-flakes
                      :let  [[val dt] (flake-value flake)
                             ref?     (and (number? val)
                                           (= const/$xsd:anyURI dt))
                             str-val  (if (string? val)
                                        val
                                        (str val))]]
                  (let [str-length        (count str-val)
                        min-length-result (if (and min-length (or ref? (> min-length str-length)))
                                            [false (str "sh:minLength: value " str-val
                                                        " has string length smaller than minimum: " min-length
                                                        " or it is not a literal value")]
                                            [true (when min-length (str "sh:not sh:minLength: value " str-val
                                                                        " must have string length less than " min-length))])
                        max-length-result (if (and max-length (or ref? (< max-length str-length)))
                                            [false (str "sh:maxLength: value " str-val
                                                        "has string length larger than " max-length
                                                        " or it is not a literal value")]
                                            [true (when max-length (str "sh:not sh:maxLength: value " str-val
                                                                        " must have string length greater than " max-length))])
                        flag-msg          (when flags (str " with provided sh:flags: " flags))
                        pattern-result    (if (and pattern (or ref? (not (some? (re-find pattern str-val)))))
                                            [false (str "sh:pattern: value " str-val
                                                        " does not match pattern \"" pattern "\"" flag-msg
                                                        " or it is not a literal value")]
                                            [true (when pattern (str "sh:not sh:pattern: value " str-val
                                                                     " must not match pattern \"" pattern "\"" flag-msg))])
                        flake-results     [min-length-result max-length-result pattern-result]]
                    (coalesce-validation-results flake-results logical-constraint)))]
    (coalesce-validation-results results)))


(defn validate-count-properties
  [{:keys [min-count max-count logical-constraint] :as _p-shape} p-flakes]
  (let [n          (count p-flakes)
        min-result (if (and min-count (> min-count n))
                     [false (str "sh:minCount of " min-count " higher than actual count of " n)]
                     [true (when min-count (str "sh:not sh:minCount of " min-count " requires lower count but actual count was " n))])
        max-result (if (and max-count (> n max-count))
                     [false (str "sh:maxCount of " max-count " lower than actual count of " n)]
                     [true (when max-count (str "sh:not sh:maxCount of " max-count " requires higher count but actual count was " n))])
        results    [min-result max-result]]
    (coalesce-validation-results results logical-constraint)))

(defn validate-value-range-properties
  [{:keys [min-inclusive min-exclusive max-inclusive max-exclusive
           logical-constraint] :as _p-shape} p-flakes]
  (let [results (for [flake p-flakes
                      :let [[val dt] (flake-value flake)
                            non-numeric-val? (not (contains? numeric-types dt))]]
                  (let [flake-results
                        [(if (and min-inclusive (or non-numeric-val? (< val min-inclusive)))
                           [false (str "sh:minInclusive: value " val " is either non-numeric or lower than minimum of " min-inclusive)]
                           [true (when min-inclusive (str "sh:not sh:minInclusive: value " val " must be less than " min-inclusive))])

                         (if (and min-exclusive (or non-numeric-val? (<= val min-exclusive)))
                           [false (str "sh:minExclusive: value " val " is either non-numeric or lower than exclusive minimum of " min-exclusive)]
                           [true (when min-exclusive (str "sh:not sh:minExclusive: value " val " must be less than or equal to " min-exclusive))])

                         (if (and max-inclusive (or non-numeric-val? (> val max-inclusive)))
                           [false (str "sh:maxInclusive: value " val " is either non-numeric or higher than maximum of " max-inclusive)]
                           [true (when max-inclusive (str "sh:not sh:maxInclusive: value " val " must be greater than " max-inclusive))])

                         (if (and max-exclusive (or non-numeric-val? (>= val max-exclusive)))
                           [false (str "sh:maxExclusive: value " val " is either non-numeric or higher than exclusive maximum of " max-exclusive)]
                           [true (when max-exclusive (str "sh:not sh:maxExclusive: value " val " must be greater than or equal to " max-exclusive))])]]
                    (coalesce-validation-results flake-results logical-constraint)))]
    (coalesce-validation-results results)))

(defn validate-value-properties
  ;; TODO: Only supports 'in' so far. Add the others.
  [{:keys [in logical-constraint] :as _p-shape} p-flakes]
  (let [results (for [flake p-flakes
                      :let [[val] (flake-value flake)
                            in-set (set in)]]
                  (if (in-set val)
                    [true (str "sh:not sh:in: value " val " must not be one of " in)]
                    [false (str "sh:in: value " val " must be one of " in)]))]
    (coalesce-validation-results results logical-constraint)))

(defn validate-property
  "Validates a PropertyShape for a single predicate against a set of flakes.
  Returns a tuple of [valid? error-msg]."
  [{:keys [min-count max-count min-inclusive min-exclusive max-inclusive
           max-exclusive min-length max-length pattern in] :as p-shape} p-flakes]
  ;; TODO: Refactor this to thread a value through via e.g. cond->
  ;;       Should embed results and error messages and short-circuit as appropriate
  (let [validation (if (or min-count max-count)
                     (validate-count-properties p-shape p-flakes)
                     [true])
        validation (if (and (first validation)
                            (or min-inclusive min-exclusive max-inclusive max-exclusive))
                     (validate-value-range-properties p-shape p-flakes)
                     validation)
        validation (if (and (first validation)
                            (or min-length max-length pattern))
                     (validate-string-properties p-shape p-flakes)
                     validation)
        validation (if (and (first validation) in)
                     (validate-value-properties p-shape p-flakes)
                     validation)]
    validation))

(defn validate-pair-property
  "Validates a PropertyShape that compares values for a pair of predicates.
  Returns a tuple of [valid? error-msg]."
  [{:keys [pair-constraint logical-constraint] :as _p-shape} lhs-flakes rhs-flakes]
  (case pair-constraint

    (:equals :disjoint)
    (let [lhs-values (into #{} (map flake-value) lhs-flakes)
          rhs-values (into #{} (map flake-value) rhs-flakes)]
      (case pair-constraint
        :equals
        (if (not= lhs-values rhs-values)
          [(= :not logical-constraint)
           (str "sh:equals: "
                (mapv flake/o lhs-flakes)
                " not equal to "
                (mapv flake/o rhs-flakes))]
          [(not= :not logical-constraint)
           (str "sh:not sh:equals: "
                (mapv flake/o lhs-flakes)
                " is required to be not equal to "
                (mapv flake/o rhs-flakes))])
        :disjoint
        (if (seq (set/intersection lhs-values rhs-values))
          [(= :not logical-constraint)
           (str "sh:disjoint: "
                (mapv flake/o lhs-flakes)
                " not disjoint from "
                (mapv flake/o rhs-flakes))]
          [(not= :not logical-constraint)
           (str "sh:not sh:disjoint: "
                (mapv flake/o lhs-flakes)
                " is disjoint from "
                (mapv flake/o rhs-flakes))])))

    (:lessThan :lessThanOrEquals)
    (let [allowed-cmp-results (cond-> #{-1}
                                      (= pair-constraint :lessThanOrEquals) (conj 0))
          valid-cmp-types     (into numeric-types time-types)
          results             (for [l-flake lhs-flakes
                                    r-flake rhs-flakes
                                    :let [[l-flake-o l-flake-dt] (flake-value l-flake)
                                          [r-flake-o r-flake-dt] (flake-value r-flake)]]
                                (if (or (not= l-flake-dt
                                              r-flake-dt)
                                        (not (contains? valid-cmp-types l-flake-dt))
                                        (not (contains? allowed-cmp-results
                                                        (flake/cmp-obj l-flake-o l-flake-dt r-flake-o r-flake-dt))))
                                  [false
                                   (str "sh" pair-constraint ": " l-flake-o " not less than "
                                        (when (= pair-constraint :lessThanOrEquals) "or equal to ")
                                        r-flake-o ", or values are not valid for comparison")]
                                  [true
                                   (str "sh:not sh" pair-constraint ": " l-flake-o " is less than "
                                        (when (= pair-constraint :lessThanOrEquals) "or equal to ")
                                        r-flake-o)]))]
      (coalesce-validation-results results logical-constraint))))

(defn validate-shape
  [{:keys [property closed-props] :as shape}
   flake-p-partitions all-flakes]
  (log/trace "validate-shape shape:" shape)
  (loop [[p-flakes & r] flake-p-partitions
         required (:required shape)]
    (if p-flakes
      (let [pid      (flake/p (first p-flakes))
            p-shapes (get property pid)
            results  (map (fn [p-shape]
                           (if-let [rhs-property (:rhs-property p-shape)]
                             (let [rhs-flakes (filter #(= rhs-property (flake/p %)) all-flakes)]
                               (validate-pair-property p-shape p-flakes rhs-flakes))
                             (validate-property p-shape p-flakes)))
                          p-shapes)
            _ (log/debug "validate-shape results:" results)
            [valid? err-msg] (coalesce-validation-results results)]
        (when (not valid?)
          (throw-property-shape-exception! err-msg))
        (when closed-props
          (when-not (closed-props pid)
            (throw (ex-info (str "SHACL shape is closed, property: " pid
                                 " is not an allowed.")
                            {:status 400 :error :db/shacl-validation}))))
        (recur r (disj required pid)))
      (if (seq required)
        (throw (ex-info (str "Required properties not present: " required)
                        {:status 400 :error :db/shacl-validation}))
        true))))

(defn validate-target
  "Some new flakes don't need extra validation."
  [{:keys [shapes] :as _shape-map} all-flakes]
  (go-try
   (let [flake-p-partitions (partition-by flake/p all-flakes)]
     (doseq [shape shapes]
       (validate-shape shape flake-p-partitions all-flakes)))))


(defn build-property-shape
  "Builds map out of values from a SHACL propertyShape (target of sh:property)"
  [property-flakes]
  (reduce
    (fn [acc property-flake]
      (let [o (flake/o property-flake)]
        (condp = (flake/p property-flake)
          const/$sh:path
          (assoc acc :path o)

          ;; The datatype of all value nodes (e.g., xsd:integer).
          ;; A shape has at most one value for sh:datatype.
          const/$sh:datatype
          (assoc acc :datatype o)

          const/$sh:minCount
          (cond-> (assoc acc :min-count o)
                  (>= o 1) (assoc :required? true)) ; min-count >= 1 means property is required

          const/$sh:maxCount
          (assoc acc :max-count o)

          ;; values of sh:nodeKind in a shape are one of the following six instances of the
          ;; class sh:NodeKind: sh:BlankNode, sh:IRI, sh:Literal sh:BlankNodeOrIRI,
          ;; sh:BlankNodeOrLiteral and sh:IRIOrLiteral.
          ;; A shape has at most one value for sh:nodeKind.
          const/$sh:nodeKind
          (assoc acc :node-kind o)

          ;; Note that multiple values for sh:class are interpreted as a conjunction,
          ;; i.e. the values need to be SHACL instances of all of them.
          const/$sh:class
          (update acc :class (fnil conj []) o)

          const/$sh:pattern
          (assoc acc :pattern o)

          const/$sh:minLength
          (assoc acc :min-length o)

          const/$sh:maxLength
          (assoc acc :max-length o)

          const/$sh:flags
          (update acc :flags (fnil conj []) o)

          const/$sh:languageIn
          (assoc acc :language-in o)

          const/$sh:uniqueLang
          (assoc acc :unique-lang o)

          const/$sh:hasValue
          (assoc acc :has-value o)

          const/$sh:in
          (update acc :in (fnil conj []) o)

          const/$sh:minExclusive
          (assoc acc :min-exclusive o)

          const/$sh:minInclusive
          (assoc acc :min-inclusive o)

          const/$sh:maxExclusive
          (assoc acc :max-exclusive o)

          const/$sh:maxInclusive
          (assoc acc :max-inclusive o)

          const/$sh:equals
          (assoc acc :pair-constraint :equals :rhs-property o)

          const/$sh:disjoint
          (assoc acc :pair-constraint :disjoint  :rhs-property o)

          const/$sh:lessThan
          (assoc acc :pair-constraint :lessThan  :rhs-property o)

          const/$sh:lessThanOrEquals
          (assoc acc :pair-constraint :lessThanOrEquals  :rhs-property o)
          ;; else
          acc)))
    {}
    property-flakes))

(defn build-not-shape
  [property-flakes]
  (-> property-flakes
      build-property-shape
      (assoc :logical-constraint :not, :required? false)))

;; TODO - pass along additional shape metadata to provided better error message.
(defn register-datatype
  "Optimization to elevate data types to top of shape for easy coersion when processing transactions"
  [{:keys [dt validate-fn] :as dt-map} {:keys [datatype path] :as property-shape}]
  (when (and dt
             (not= dt
                   datatype))
    (throw (ex-info (str "Conflicting SHACL shapes. Property " path
                         " has multiple conflicting datatype declarations of: "
                         dt " and " datatype ".")
                    {:status 400 :error :db/shacl-validation})))
  {:dt          datatype
   :validate-fn validate-fn})

(defn register-nodetype
  "Optimization to elevate node type designations"
  [{:keys [dt validate-fn] :as dt-map} {:keys [class node-kind path] :as property-shape}]
  (let [dt-map* (condp = node-kind
                  const/$sh:BlankNode
                  {:dt          const/$xsd:anyURI
                   :class       class
                   :validate-fn (fn [x] (and (string? x)
                                             (str/starts-with? x "_:")))}

                  ;; common case, has to be an IRI
                  const/$sh:IRI
                  {:dt          const/$xsd:anyURI
                   :class       class
                   :validate-fn (fn [x] (and (string? x)
                                             (not (str/starts-with? x "_:"))))}

                  const/$sh:BlankNodeOrIRI
                  {:dt          const/$xsd:anyURI
                   :class       class
                   :validate-fn nil}

                  const/$sh:IRIOrLiteral
                  {:dt          nil
                   :class       class
                   :validate-fn nil}

                  const/$sh:BlankNodeOrLiteral
                  {:dt          nil
                   :class       class
                   :validate-fn nil}

                  ;; means it *cannot* be an IRI, but any literal is OK
                  const/$sh:Literal
                  {:dt          nil
                   :validate-fn nil})]
    (when (and dt
               (not= dt (:dt dt-map*)))
      (throw (ex-info (str "Conflicting SHACL shapes. Property " path
                           " has multiple conflicting datatype declarations of: "
                           dt " and " (:dt dt-map*) ".")
                      {:status 400 :error :db/shacl-validation})))
    dt-map*))

(defn register-class
  [{:keys [dt] :as dt-map} class-iris]
  (log/trace "register-class dt-map:" dt-map)
  (log/trace "register-class class-iris:" class-iris)
  {:dt          dt
   :class       class-iris
   :validate-fn (fn [{:keys [type]}]
                  (log/trace "class validate-fn class-iris:" class-iris)
                  (log/trace "class validate-fn type:" type)
                  (let [types (if (coll? type) type [type])]
                    (= (set class-iris) (set types))))})


(defn- merge-datatype
  "Merging functions for use with 'merge-with'.
  Ensures datatype merging values for each predicate are identical else throws."
  [{dt-result :dt, class-result :class, validate-result :validate-fn}
   {dt-latter :dt, class-latter :class, validate-latter :validate-fn}]
  (when (not= dt-result dt-latter)
    (throw (ex-info (str "Conflicting SHACL shapes. Property"
                         " has multiple conflicting datatype declarations of: "
                         dt-result " and " dt-latter ".")
                    {:status 400 :error :db/shacl-validation})))
  (when (not= dt-result dt-latter)
    (throw (ex-info (str "Conflicting SHACL shapes. Property"
                         " has multiple conflicting class declarations of: "
                         class-result " and " class-latter ".")
                    {:status 400 :error :db/shacl-validation})))
  {:dt          dt-result
   :class       class-result
   :validate-fn (cond
                  (and validate-result validate-latter)
                  (fn [x]
                    (and (validate-result x)
                         (validate-latter x)))

                  validate-result
                  validate-result

                  validate-latter
                  validate-latter)})

(defn add-closed-props
  "Given a Fluree formatted shape, returns list of predicate/property ids
  that can only be included in the shape. Any properties outside of this shoudl error."
  [{:keys [ignored-properties property] :as shape}]
  (let [closed-props (-> ignored-properties
                         (conj 0)                           ;; pid 0 holds the IRI and is always allowed
                         (into (keys property)))]
    (assoc shape :closed-props closed-props)))

(defn get-regex-flag
  "Given an `sh:flag` value, returns the corresponding regex flag
  for the current platform. If the provided flag is not found,
  it will be ignored by validation.

  Note that js does not have support for `x` or `q` flag behavior."
  [flag]
  #?(:clj (case flag
            "i" Pattern/CASE_INSENSITIVE
            "m" Pattern/MULTILINE
            "s" Pattern/DOTALL
            "q" Pattern/LITERAL
            "x" Pattern/COMMENTS
            0)
     :cljs (if (#{"i" "m" "s"} flag)
             flag
             "")))


(defn build-pattern
  "Builds regex pattern out of input string
  and any flags that were provided."
  [{:keys [:pattern :flags] :as _shape}]
  (let [valid-flags (->> (map get-regex-flag flags)
                         #?(:clj (apply +)
                            :cljs (apply str)))]
    (-> pattern
        #?(:clj (Pattern/compile (or valid-flags 0))
           :cljs (js/RegExp. (or valid-flags ""))))))


(defn build-class-shapes
  "Given a class SID, returns class shape"
  [db type-sid]
  (go-try
    (let [shape-sids (->> (<? (query-range/index-range db :post = [const/$sh:targetClass type-sid]))
                          (map flake/s))]
      (when (seq shape-sids)
        (loop [[shape-sid & r] shape-sids
               datatype nil
               shapes   []]
          (if shape-sid
            (let [shape-flakes (<? (query-range/index-range db :spot = [shape-sid]))
                  shape        (loop [[flake & r'] shape-flakes
                                      shape    {}
                                      p-shapes {}]
                                 (if flake
                                   (let [p (flake/p flake)
                                         o (flake/o flake)]
                                     (if (#{const/$sh:property const/$sh:not} p)
                                       (let [flakes (<? (query-range/index-range db :spot = [o]))
                                             {:keys [path] :as property-shape} (condp = p
                                                                                 const/$sh:property
                                                                                 (build-property-shape flakes)
                                                                                 const/$sh:not
                                                                                 (build-not-shape flakes))
                                             ;; we key the property shapes map with the property subj id (sh:path)
                                             property-shape* (if (:pattern property-shape)
                                                               (assoc property-shape :pattern (build-pattern property-shape))
                                                               property-shape)
                                             p-shapes*      (update p-shapes path util/conjv property-shape*)
                                             ;; elevate following conditions to top-level custom keys to optimize validations when processing txs
                                             class-iris      (when-let [class-sids (:class property-shape)]
                                                               (let [id-path (fn [sid] [sid const/iri-id])]
                                                                 (loop [[csid & csids] class-sids
                                                                        ciris []]
                                                                   (let [ciri (->> csid
                                                                                   id-path
                                                                                   (query-range/index-range db :spot =)
                                                                                   <?
                                                                                   first
                                                                                   flake/o)
                                                                         next-ciris (conj ciris ciri)]
                                                                     (log/trace "next-ciris:" next-ciris)
                                                                     (if (seq csids)
                                                                       (recur csids next-ciris)
                                                                       next-ciris)))))
                                             shape*         (cond-> shape
                                                                    (:required? property-shape)
                                                                    (update :required util/conjs (:path property-shape))

                                                                    (:datatype property-shape)
                                                                    (update-in [:datatype (:path property-shape)]
                                                                               register-datatype property-shape)

                                                                    (:node-kind property-shape)
                                                                    (update-in [:datatype (:path property-shape)]
                                                                               register-nodetype property-shape)

                                                                    (:class property-shape)
                                                                    (update-in [:datatype (:path property-shape)]
                                                                               register-class class-iris))]

                                         (recur r' shape* p-shapes*))
                                       (let [shape* (condp = p
                                                      const/$xsd:anyURI
                                                      (assoc shape :id o)

                                                      const/$sh:targetClass
                                                      (assoc shape :target-class o)

                                                      const/$sh:closed
                                                      (if (true? o)
                                                        (assoc shape :closed? true)
                                                        shape)

                                                      const/$sh:ignoredProperties
                                                      (update shape :ignored-properties util/conjs o)

                                                      ;; else
                                                      shape)]
                                         (recur r' shape* p-shapes))))
                                   (cond-> (assoc shape :property p-shapes)
                                           (:closed? shape) add-closed-props)))]
              (let [datatype* (merge-with merge-datatype datatype (:datatype shape))]
                (recur r datatype* (conj shapes shape))))
            {:shapes   shapes
             :datatype datatype}))))))

(defn merge-shapes
  "Merges multiple shape maps together when multiple classes have shape
  constraints on the same subject."
  [shape-maps]
  (reduce (fn [{:keys [datatype shapes] :as acc} shape]
            (assoc acc :datatype (merge-with merge-datatype datatype (:datatype shape))
                       :shapes (into shapes (:shapes shape))))
          shape-maps))


(defn class-shapes
  "Takes a list of target classes and returns shapes that must pass validation,
  or nil if none exist."
  [{:keys [schema] :as db} type-sids]
  (go-try
    (let [shapes-cache (:shapes schema)]
      (loop [[type-sid & r] type-sids
             shape-maps nil]
        (if type-sid
          (let [shape-map (if (contains? (:class @shapes-cache) type-sid)
                            (get-in @shapes-cache [:class type-sid])
                            (let [shapes (<? (build-class-shapes db type-sid))]
                              (swap! shapes-cache assoc-in [:class type-sid] shapes)
                              shapes))]
            (recur r (if shape-map
                       (conj shape-maps shape-map)
                       shape-maps)))
          (when shape-maps
            (merge-shapes shape-maps)))))))
