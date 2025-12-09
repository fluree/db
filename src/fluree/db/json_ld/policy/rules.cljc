(ns fluree.db.json-ld.policy.rules
  (:require [clojure.core.async :as async]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake.index.novelty :as novelty]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]))

#?(:clj (set! *warn-on-reflection* true))

(defn view-restriction?
  [restriction-map]
  (:view? restriction-map))

(defn modify-restriction?
  [restriction-map]
  (:modify? restriction-map))

(defn policy-cids
  "Returns class subject ids for a given policy restriction map.

  Relevant classes are specified in the :on-class key of the restriction map."
  [db restriction-map]
  (when-let [classes (:on-class restriction-map)]
    (->> classes
         (map #(iri/encode-iri db %))
         set)))

(defn add-default-restriction
  [restriction policy]
  (cond-> policy

    (view-restriction? restriction)
    (update-in [:view :default] util/conjv restriction)

    (modify-restriction? restriction)
    (update-in [:modify :default] util/conjv restriction)))

(defn- get-class-properties
  "Returns the set of property SIDs used by instances of the given class.
   Uses stats from db :stats field (either from indexing or from cached ledger-info computation)."
  [db cid]
  (when-let [class-stats (get-in db [:stats :classes cid])]
    (set (keys (:properties class-stats)))))

(defn- all-classes-using-property
  "Returns the set of all class IDs that use a given property according to stats.
   Used to determine if a class policy needs class membership checking."
  [db pid]
  (when-let [all-classes (get-in db [:stats :classes])]
    (reduce-kv
     (fn [acc cid class-data]
       (if (contains? (:properties class-data) pid)
         (conj acc cid)
         acc))
     #{}
     all-classes)))

(defn- implicit-property?
  "Returns true if the property is an implicit property (@id or @type).
   Every subject has these properties, so class policies indexed by them
   always require class membership checking."
  [pid]
  (or (= pid const/$id) (= pid const/$rdf:type)))

(defn- build-property-to-classes-map
  "Builds a reverse mapping from property SID to the set of classes that use it.
   Returns {pid #{cid1 cid2 ...}}"
  [db cids]
  (reduce
   (fn [acc cid]
     (let [properties (get-class-properties db cid)]
       (reduce (fn [m pid]
                 (update m pid (fnil conj #{}) cid))
               acc
               properties)))
   {}
   cids))

(defn- add-implicit-class-properties
  "Adds implicit properties (@id, @type) to the property-classes map.
   Every subject of a class has @id and @type flakes, so class-derived
   policies must also apply to these properties."
  [property-classes-map cids]
  (-> property-classes-map
      (update const/$id (fnil into #{}) cids)
      (update const/$rdf:type (fnil into #{}) cids)))

(defn add-class-restriction
  "Adds class-targeted policies with O(1) indexed lookup.

   Stores class-derived policies directly in [:view :property pid] and [:modify :property pid]
   (the same map as regular property policies) with a :class-policy? flag. This enables
   a single O(1) lookup at query time - the class check is done inline during policy evaluation.

   Each class-derived restriction includes metadata about which classes use that property,
   enabling efficient class membership verification during enforcement.

   When stats are available, policies are indexed by all properties used by the target classes.
   Additionally, implicit properties (@id, @type) are always indexed since every subject has them.
   This ensures policies apply even when restricting classes with no existing instances.

   Optimization: For each property, determines if class membership checking is needed:
   - If the property is ONLY used by the policy's target classes → no class check needed
   - If OTHER classes also use the property → class check needed to filter appropriately"
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)
        property-classes-map (build-property-to-classes-map db cids)
        property-classes-map* (add-implicit-class-properties property-classes-map cids)]
    ;; Store class-derived policies directly in [:view :property pid]
    (reduce-kv
     (fn [policy pid classes-using-property]
       (let [;; Determine if class check is needed for this property:
             ;; - Get ALL classes that use this property (from stats)
             ;; - If for-classes covers all of them, no class check needed
             ;; - For implicit properties (@id, @type), always need class check
             ;;   since every subject has them
             all-classes       (all-classes-using-property db pid)
             class-check-needed? (or (implicit-property? pid)
                                     (not (every? classes-using-property all-classes)))
             class-policy {:id                  (:id restriction-map)
                           :class-policy?       true
                           :class-check-needed? class-check-needed?
                           :for-classes         classes-using-property
                           :on-class            (:on-class restriction-map)
                           :required?           (:required? restriction-map)
                           :ex-message          (:ex-message restriction-map)
                           :view?               (:view? restriction-map)
                           :modify?             (:modify? restriction-map)
                           :allow?              (:allow? restriction-map)
                           :query               (:query restriction-map)}]
         (cond-> policy
           (view-restriction? restriction-map)
           (update-in [:view :property pid] util/conjv class-policy)

           (modify-restriction? restriction-map)
           (update-in [:modify :property pid] util/conjv class-policy))))
     policy-map
     property-classes-map*)))

(defn add-property-restriction
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy property]
       (let [pid              (if (iri/sid? property) property (iri/encode-iri db property))
             restriction-map* (assoc restriction-map :pid pid
                                     :cids cids)]
         (cond-> policy

           (view-restriction? restriction-map*)
           (update-in [:view :property pid] util/conjv restriction-map*)

           (modify-restriction? restriction-map*)
           (update-in [:modify :property pid] util/conjv restriction-map*))))
     policy-map
     (:on-property restriction-map))))

(defn add-subject-restriction
  "Adds subject-targeted policies with O(1) indexed lookup by subject ID."
  [restriction-map db policy-map]
  (let [cids (policy-cids db restriction-map)]
    (reduce
     (fn [policy subject]
       (let [sid              (if (iri/sid? subject) subject (iri/encode-iri db subject))
             restriction-map* (assoc restriction-map :sid sid
                                     :cids cids)]
         (cond-> policy

           (view-restriction? restriction-map*)
           (update-in [:view :subject sid] util/conjv restriction-map*)

           (modify-restriction? restriction-map*)
           (update-in [:modify :subject sid] util/conjv restriction-map*))))
     policy-map
     (:on-subject restriction-map))))

(defn query-target?
  "A target-expr can either be a static IRI or a query map."
  [target-expr]
  (map? target-expr))

(defn- query-contains-var?
  "Checks if a query map contains a specific variable (recursively searches all values)."
  [query-map var-name]
  (let [check-val (fn check-val [v]
                    (cond
                      (= v var-name) true
                      (map? v) (some check-val (vals v))
                      (sequential? v) (some check-val v)
                      :else false))]
    (check-val query-map)))

(defn- detect-target-var
  "Detects which target variable the user used in their query.
  Returns ?$target only if explicitly used (legacy), otherwise ?$this."
  [query-map]
  (if (query-contains-var? query-map "?$target")
    "?$target"
    "?$this"))

(defn parse-targets
  [db tracker error-ch policy-values target-exprs]
  (let [in-ch  (async/to-chan! target-exprs)
        out-ch (async/chan 2 (map (fn [iri] (iri/iri->sid iri (:namespaces db)))))]
    (async/pipeline-async 2
                          out-ch
                          (fn [target-expr ch]
                            (async/go
                              (try*
                                (if (query-target? target-expr)
                                  (let [target-var (detect-target-var target-expr) ;; Support both ?$this (preferred) and ?$target (legacy)
                                        target-q   (cond-> (assoc target-expr
                                                                  "select" target-var
                                                                  :selection-context {}) ;; don't compact selection results
                                                     policy-values (policy/inject-where-pattern ["values" policy-values]))]
                                    (->> (<? (dbproto/-query db tracker target-q))
                                         (async/onto-chan! ch)))
                                 ;; non-maps are literals
                                  (async/onto-chan! ch [target-expr]))
                                (catch* e
                                  (async/>! error-ch e)))))
                          in-ch)
    (async/into #{} out-ch)))

(defn unwrap
  [targets]
  (not-empty (mapv #(or (util/get-id %) (util/get-value %)) targets)))

(defn parse-policy
  [db tracker error-ch policy-values policy-doc]
  (async/go
    (try*
      (let [id (util/get-id policy-doc) ;; @id name of policy-doc

            ;; Subject targeting via onSubject (preferred) or targetSubject (legacy)
            ;; Both support static IRIs and queries via parse-targets
            on-subject-specs    (unwrap (get policy-doc const/iri-onSubject))
            target-subject      (unwrap (get policy-doc const/iri-targetSubject))
            subject-specs       (not-empty (into (vec (or on-subject-specs []))
                                                 (or target-subject [])))
            subject-targets-ch  (when subject-specs
                                  (parse-targets db tracker error-ch policy-values subject-specs))

            ;; Property targeting via targetProperty (dynamic resolution via parse-targets)
            target-property     (unwrap (get policy-doc const/iri-targetProperty))
            property-targets-ch (when target-property
                                  (parse-targets db tracker error-ch policy-values target-property))

            ;; Property targeting via onProperty
            ;; Supports both static IRIs and queries - both go through parse-targets
            ;; This unifies behavior with targetProperty for backward compatibility
            on-property-specs   (unwrap (get policy-doc const/iri-onProperty))
            on-property-ch      (when on-property-specs
                                  (parse-targets db tracker error-ch policy-values on-property-specs))

            on-class    (when-let [classes (util/get-all-ids policy-doc const/iri-onClass)]
                          (set classes))

            allow?    (util/get-first-value policy-doc const/iri-allow)

            src-query (util/get-first-value policy-doc const/iri-query)
            query     (cond
                        ;; f:allow takes precedence - no query needed
                        (some? allow?)
                        nil

                        (map? src-query)
                        (assoc src-query "select" "?$this" "limit" 1)

                        (nil? src-query)
                        nil

                        :else
                        (throw (ex-info (str "Invalid policy query. Query must be a map, instead got: " src-query)
                                        {:status 400
                                         :error  :db/invalid-policy})))
            actions   (set (util/get-all-ids policy-doc const/iri-action))
            view?     (or (empty? actions) ;; if actions is not specified, default to all actions
                          (contains? actions const/iri-view))
            modify?   (or (empty? actions)
                          (contains? actions const/iri-modify))

            subject-targets     (when subject-targets-ch (<? subject-targets-ch))
            property-targets    (when property-targets-ch (<? property-targets-ch))
            ;; Resolved onProperty targets (static IRIs or queries)
            on-property-targets (when on-property-ch (<? on-property-ch))]

        (when (and (nil? allow?)
                   (nil? query)
                   (nil? target-subject)
                   (nil? target-property)
                   (nil? on-property-targets)
                   (nil? on-class))
          (throw (ex-info (str "Invalid policy, unable to extract a target subject, property, or on-property. "
                               "Did you forget @context?. Parsed restriction: " policy-doc)
                          {:status 400
                           :error  :db/invalid-policy})))

        (if (or view? modify?)
          (cond-> {:id          id
                   :on-class    on-class
                   :required?   (util/get-first-value policy-doc const/iri-required)
                   ;; with no class or property restrictions, becomes a default policy-doc
                   :default?    (and (nil? on-class)
                                     (nil? subject-targets)
                                     (nil? property-targets)
                                     (nil? on-property-targets))
                   :ex-message  (util/get-first-value policy-doc const/iri-exMessage)
                   :view?       view?
                   :modify?     modify?
                   :allow?      allow?
                   :query       query}
            ;; Raw specs for modify refresh logic (when they contain queries)
            ;; Store combined subject specs from both onSubject and targetSubject
            subject-specs                   (assoc :subject-specs subject-specs)
            target-property                 (assoc :target-property target-property)
            ;; Store raw on-property specs for modify refresh (when they contain queries)
            (some query-target? on-property-specs) (assoc :on-property-specs on-property-specs)
            (not-empty property-targets)    (assoc :p-targets property-targets)
            (not-empty subject-targets)     (assoc :on-subject subject-targets)
            (not-empty on-property-targets) (assoc :on-property on-property-targets))
          (throw (ex-info (str "Invalid policy definition. Policies must have f:action of {@id: f:view} or {@id: f:modify}. "
                               "Policy data that failed: " policy-doc)
                          {:status 400
                           :error  :db/invalid-policy}))))
      (catch* e
        (async/put! error-ch e)))))

(defn enforcement-report
  [db]
  (some-> db
          :policy
          :trace
          (update-vals (fn [p-report]
                         (update-vals p-report deref)))))

(defn build-wrapper
  [db]
  (fn [wrapper policy]
    (cond
      (seq (:on-property policy))
      (add-property-restriction policy db wrapper)

      (seq (:on-subject policy))
      (add-subject-restriction policy db wrapper)

      (or (:p-targets policy)
          (:o-targets policy))
      (add-default-restriction policy wrapper)

      (seq (:on-class policy))
      (add-class-restriction policy db wrapper)

      (:default? policy)
      (add-default-restriction policy wrapper)

      :else
      wrapper)))

(defn parse-policies
  [db tracker error-ch policy-values policy-docs]
  (let [policy-ch     (async/chan)]
    (->> policy-docs
         async/to-chan!
         (async/pipeline-async 2
                               policy-ch
                               (fn [policy-doc ch]
                                 (-> (parse-policy db tracker error-ch policy-values policy-doc)
                                     (async/pipe ch)))))

    (async/reduce (build-wrapper db) {} policy-ch)))

(defn ensure-ground-identity
  "A policy must never have a \"fresh\" ?$identity variable, otherwise it may match any
  identity in the db. This ensures the ?$identity is always provided as a \"ground\"
  value."
  [[vars :as policy-values]]
  (if (contains? (set vars) "?$identity")
    ;; already has a ground value for $?identity
    policy-values
    ;; bind ?$identity to a ground value that will never match anything
    (policy/inject-value-binding policy-values "?$identity" {const/iri-value (str ":" (random-uuid))
                                                             const/iri-type const/iri-id})))

(defn- has-class-policies?
  "Checks if any policy document has f:onClass targeting.
   Used to determine if we need to compute class->property stats."
  [policy-docs]
  (some #(get % const/iri-onClass) policy-docs))

(defn- get-stats-for-class-policies
  "Gets class->property stats for f:onClass optimization.
   Uses shared LRU cache (same as ledger-info API).
   Returns db with :stats populated.
   Throws if stats computation fails (required for f:onClass policies)."
  [db]
  (async/go
    (let [stats (async/<! (novelty/cached-current-stats db))]
      (if (util/exception? stats)
        (throw (ex-info "Failed to compute stats for f:onClass policy optimization. Class restrictions require stats."
                        {:error :db/policy-error
                         :cause (ex-message stats)}))
        (assoc db :stats stats)))))

(defn wrap-policy
  ([db policy-rules policy-values]
   (wrap-policy db nil policy-rules policy-values nil))
  ([db tracker policy-rules policy-values]
   (wrap-policy db tracker policy-rules policy-values nil))
  ([db tracker policy-rules policy-values default-allow?]
   (async/go
     (let [policy-docs     (util/sequential policy-rules)
           ;; Only compute stats if there are f:onClass policies
           db*             (if (has-class-policies? policy-docs)
                             (async/<! (get-stats-for-class-policies db))
                             db)
           error-ch        (async/chan)
           policy-values*  (ensure-ground-identity policy-values)
           [wrapper _]     (async/alts! [error-ch (parse-policies db* tracker error-ch policy-values*
                                                                  policy-docs)])]
       (if (util/exception? wrapper)
         wrapper
         (assoc db :policy (assoc wrapper
                                  :cache (atom {})
                                  :policy-values policy-values*
                                  :default-allow? default-allow?)))))))
