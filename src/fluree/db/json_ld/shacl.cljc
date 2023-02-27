(ns fluree.db.json-ld.shacl
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [clojure.set :as set]))

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
                     :sh/nodeKind {:id :sh/IRI}}]}

  )


;; property min & max
;; -- if new, can just make sure for each property between min and max
;; -- if existing, need to get existing counts

;; property data type
;; - any "adds" just coerce, ignore retractions

;; sh:ignoredProperties - let pass through

;; sh:closed true
;; - have a set of allowed and reject if not in the list
;; - set includes all properties from above + ignoredProperties

;; sh:pattern - assume datatype xsd:string if not specified


(defn apply-flake-changes
  [existing-flakes changed-flakes]
  :TODO

  )

(defn validate-property
  "Validates a PropertyShape for a single predicate
  against a set of flakes"
  [{:keys [min-count max-count node-kind]} p-flakes]
  (let [n (count p-flakes)]
    (when (and min-count
               (> min-count n))
      (throw (ex-info (str "SHACL PropertyShape exception - sh:minCount of " min-count
                           " higher than actual count of " n ".")
                      {:status 400 :error :db/shacl-validation})))
    (when (and max-count
               (> n max-count))
      (throw (ex-info (str "SHACL PropertyShape exception - sh:maxCount of " max-count
                           " lower than actual count of " n ".")
                      {:status 400 :error :db/shacl-validation})))))

(defn validate-pair-property
  "Validates a PropertyShape that compares values
  for a pair of predicates."
  [{:keys [pair-constraint] :as _p-shape} lhs-flakes rhs-flakes]
  (let [flake-o-dt (fn [flake] [(flake/o flake) (flake/dt flake)])]
    (case pair-constraint

     :equals (let [lhs-values (into #{} (map flake-o-dt) lhs-flakes)
                    rhs-values (into #{} (map flake-o-dt) rhs-flakes)]
                (if-not (= lhs-values rhs-values)
                  (throw (ex-info (str "SHACL PropertyShape exception - sh:equals: " (mapv flake/o lhs-flakes)
                                       " not equal to " (mapv flake/o rhs-flakes))
                                  {:status 400 :error :db/shacl-validation}))))

      :disjoint (let [lhs-values (into #{} (map flake-o-dt) lhs-flakes)
                      rhs-values (into #{} (map flake-o-dt) rhs-flakes)]
                  (if-not (empty? (set/intersection lhs-values rhs-values))
                    (throw (ex-info (str "SHACL PropertyShape exception - sh:disjoint: " (mapv flake/o lhs-flakes)
                                         " not disjoint from " (mapv flake/o lhs-flakes))
                                    {:status 400 :error :db/shacl-validation}))))


      :lessThan (doseq [l-flake lhs-flakes
                        r-flake rhs-flakes]
                  (let [[l-flake-o l-flake-dt] (flake-o-dt l-flake)
                        [r-flake-o r-flake-dt] (flake-o-dt r-flake)]
                    (when (or (not= l-flake-dt
                                    r-flake-dt)
                              (not= -1 (flake/cmp-obj l-flake-o l-flake-dt r-flake-o r-flake-dt)))
                      (throw (ex-info (str "SHACL PropertyShape exception - sh:lessThan: "
                                           l-flake-o " not less than " r-flake-o)
                                      {:status 400 :error :db/shacl-validation})))))
      :lessThanOrEquals (doseq [l-flake lhs-flakes
                                r-flake rhs-flakes]
                          (let [[l-flake-o l-flake-dt] (flake-o-dt l-flake)
                                [r-flake-o r-flake-dt] (flake-o-dt r-flake)]
                            (when (or (not= l-flake-dt
                                            r-flake-dt)
                                      (not (contains? #{0 -1}
                                                      (flake/cmp-obj l-flake-o l-flake-dt r-flake-o r-flake-dt))))
                              (throw (ex-info (str "SHACL PropertyShape exception - sh:lessThanOrEquals: "
                                                   l-flake-o " not less than or equal to " r-flake-o)
                                              {:status 400 :error :db/shacl-validation}))))))))

(defn validate-shape
  [{:keys [property closed-props] :as shape} p-flakes all-flakes]
  (loop [[p-flakes & r] p-flakes
         required (:required shape)]
    (if p-flakes
      (let [pid      (flake/p (first p-flakes))
            p-shapes (get property pid)
            error?   (some (fn [p-shape]
                             (if-let [pair-property (:rhs-property p-shape)]
                               (let [rhs-flakes (filter #(= pair-property (flake/p %)) all-flakes)]
                                 (validate-pair-property p-shape p-flakes rhs-flakes))
                               (validate-property p-shape p-flakes)))
                           p-shapes)]
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
  [db {:keys [shapes datatype] :as shape-map} all-flakes]
  (go-try
   (let [p-flakes (partition-by flake/p all-flakes)]
     (doseq [shape shapes]
       (validate-shape shape p-flakes all-flakes)))))

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
                  (>= o 1) (assoc :required? true))         ;; min-count >= 1 means property is required

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
          (assoc acc :node-kind o)

          const/$sh:pattern
          (assoc acc :pattern (re-pattern o))

          const/$sh:minLength
          (assoc acc :min-length o)

          const/$sh:maxLength
          (assoc acc :max-length o)

          const/$sh:languageIn
          (assoc acc :language-in o)

          const/$sh:uniqueLang
          (assoc acc :unique-lang o)

          const/$sh:hasValue
          (assoc acc :has-value o)

          const/$sh:in
          (assoc acc :in o)

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

;; TODO - pass along additional shape metadata to provided better error message.
(defn register-datatype
  "Optimization to elevate data types to top of shape for easy coersion when processing transactions"
  [{:keys [dt validate-fn] :as dt-map} {:keys [datatype pattern path] :as property-shape}]
  (when (and dt
             (not= dt
                   datatype))
    (throw (ex-info (str "Conflicting SHACL shapes. Property " path
                         " has multiple conflicting datatype declarations of: "
                         dt " and " datatype ".")
                    {:status 400 :error :db/shacl-validation})))
  (when (and pattern
             (not= const/$xsd:string datatype))
    (log/warn (str "SHACL PropertyShape defines a pattern, " pattern
                   " however the datatype defined is not xsd:string."
                   " Ignoring pattern for validation.")))
  (if pattern
    (if validate-fn
      {:dt          datatype
       :validate-fn (fn [x]
                      (when (re-matches pattern x)
                        ;; if prior condition fails, return falsey and stop evaluation
                        (validate-fn x)))}
      {:dt          datatype
       :validate-fn (fn [x] (re-matches pattern x))})
    {:dt          datatype
     :validate-fn validate-fn}))

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
                                     (if (= const/$sh:property p)
                                       (let [{:keys [path] :as property-shape} (-> (<? (query-range/index-range db :spot = [o]))
                                                                                   (build-property-shape))
                                             ;; we key the property shapes map with the property subj id (sh:path)
                                             p-shapes*      (update p-shapes path util/conjv property-shape)
                                             ;; elevate following conditions to top-level custom keys to optimize validations when processing txs
                                             shape*         (cond-> shape
                                                                    (:required? property-shape)
                                                                    (update :required util/conjs (:path property-shape))

                                                                    (:datatype property-shape)
                                                                    (update-in [:datatype (:path property-shape)]
                                                                               register-datatype property-shape)

                                                                    (:node-kind property-shape)
                                                                    (update-in [:datatype (:path property-shape)]
                                                                               register-nodetype property-shape))]

                                         (recur r' shape* p-shapes*))
                                       (let [shape* (condp = p
                                                      const/$iri
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
   (let [shapes-atom (:shapes schema)
         cached-shapes @shapes-atom]
      (loop [[type-sid & r] type-sids
             shape-maps nil]
        (if type-sid
          (let [shape-map (if-let [cached (get-in cached-shapes [:class type-sid])]
                            cached
                            (let [shapes (<? (build-class-shapes db type-sid))]
                              (swap! shapes-atom assoc-in [:class type-sid] shapes)
                              shapes))]
            (recur r (if shape-map
                       (conj shape-maps shape-map)
                       shape-maps)))
          (when shape-maps
            (merge-shapes shape-maps)))))))
