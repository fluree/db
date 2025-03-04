(ns fluree.db.validation
  (:require [fluree.db.constants :as const]
            [fluree.db.util.docs :as docs]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]
            [clojure.string :as str]
            [clojure.walk :as walk]))

(defn decode-json-ld-keyword
  [v]
  (if (string? v)
    (if (= \@ (first v))
      (-> v (subs 1) keyword)
      (keyword v))
    v))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn bnode-variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \_))))

(def value? (complement sequential?))

(defn iri-key?
  [x]
  (= const/iri-id x))

(defn where-pattern-type
  [pattern]
  (if (sequential? pattern)
    (-> pattern first keyword)
    :node))

(defn explain-error
  [error]
  (-> error ex-data :data :explain))

(defn nearest-or-parent
  "If a given error is the child of a disjunction,
   returns the error data corresponding to that disjunction."
  [{:keys [schema value] :as _explained-error} error]
  (let [{:keys [path in]} error]
    (loop [i (dec (count path))]
      (when-not (= 0 i)
        (let [subpath         (subvec path 0 i)
              schema-fragment (mu/get-in schema subpath)
              type            (some-> schema-fragment
                                      m/type)]
          (if (#{:or :orn} type)
            (let [in-length (count (mu/path->in schema subpath))
                  in'       (subvec in 0 in-length)]
              {:schema schema-fragment
               :path   subpath
               :type   type
               :in     in'
               :value  (get-in (walk/keywordize-keys value) in')})
            (recur (dec i))))))))

(defn error-specificity-score
  "Given an error, applies a heursitic
   intended to favor errors corresponding
   to smaller/more specific parts of a failing value.
   Those errors should hopefully be more relevant to
   users' intent.

   When used in sorting, will push those errors
   toward the start of the list. "
  [error]
  (let [{:keys [schema in type]} error
        properties (m/properties schema)]
    ;;When inline limit constraints, eg
    ;;`[:map {:max 1} ...]` are used, then both type and limit
    ;; failures will have the same `:in`, despite the limit failure
    ;; being more specific. This second number differentiates
    ;; those cases.
    [(- (count in)) (if (and (or (contains? properties :max)
                                 (contains? properties :min))
                             (= type :malli.core/limits))
                      -1
                      1)]))

(defn choose-relevant-error
  "Calculates the most specific error (per our heuristic).
   If there are more than one of equal specificity,
   chooses a chunk of errors which share the same `:in`
   (portion of the value whch failed), and attempts to
   find the nearest disjunction which contains all of
   those errors. "
  [{:keys [errors] :as explained-error}]
  (let [most-specific-errors  (->> errors
                                   (sort-by error-specificity-score)
                                   (partition-by error-specificity-score)
                                   first)]
    (if (= (count most-specific-errors) 1)
      (first most-specific-errors)
      (let [same-in (val (first (group-by :in most-specific-errors)))
            [e & es] same-in
            or-parent (loop [{:keys [path] :as parent} (nearest-or-parent explained-error e)]
                        (when parent
                          (if (every? (fn [err]
                                        (let [path' (:path err)]
                                          (when (<= (count path) (count path'))
                                            (= path (subvec path' 0 (count path)))))) es)
                            parent
                            (recur (nearest-or-parent explained-error parent)))))]
        (or or-parent (first same-in))))))

(defn resolve-root-error-for-in
  "Traverses the schema tree backwards from a given error message,
   resolving the highest eligible error.

   This is based on malli's `-resolve-root-error` fn
   (https://github.com/metosin/malli/blob/a43c28d90b4eb18054df2a21c91a18d4b58cacc2/src/malli/error.cljc#L268),
   But importantly, it will stop and return when it has reached the highest error
   which still has the same `:in` as the originating error, rather than continuing
   as far as possible.

  This limit on the traversal constrains us to errors which are still relevant to
  the part of the value for which we are returning an error.

  (This version also has some fixes to prevent returning a `nil` value, or
   blowing up with an `:invalid-schema` exception in certain cases.) "
  [{:keys [schema]} {:keys [path in] :as error} options]
  (let [options (assoc options :unknown false)]
    (loop [path path, l nil, mp path, p (m/properties (:schema error)), m (me/error-message error options)]
      (let [[path' m' p']
            (or
             (when-let [schema' (mu/get-in schema path)]
               (let [in' (mu/path->in schema  path)]
                 (when (= in in')
                   (or (let [value (get-in (:value error) (mu/path->in schema  path))]
                         (when-let [m' (me/error-message {:schema schema'
                                                          :value value} options)]
                           [path m' (m/properties schema')]))
                       (let [res (and l (mu/find (mu/get-in schema path) l))]
                         (when (vector? res)
                           (let [[_ props schema] res
                                 schema (mu/update-properties schema merge props)
                                 message (me/error-message {:schema schema} options)]
                             (when message [(conj path l) message (m/properties schema)]))))))))
             (when m [mp m p]))]
        (if (seq path)
          (recur (pop path) (last path) path' p' m')
          (when m [(if (seq in) (mu/path->in schema path') (me/error-path error options)) m' p']))))))

(def top-level-query-keys
  #{:select
    :where
    :group-by
    :groupBy
    :order-by
    :orderBy
    :commit-details
    :t
    :history
    :from})

(defn format-error
  [explained error error-opts]
  (let [{full-value :value} explained
        {:keys [path value]} error
        top-level-key (first (filter top-level-query-keys path))
        top-level-message (when top-level-key
                            (str "Error in value for \"" (name top-level-key) "\""))
        [_ root-message] (resolve-root-error-for-in
                          explained
                          error
                          error-opts)
        [_ direct-message] (me/-resolve-direct-error
                            explained
                            error
                            error-opts)
        docs-pointer-msg (when top-level-key
                           (str " See documentation for details: "
                                docs/error-codes-page "#query-invalid-"
                                (->> (str/replace (name top-level-key) #"-" "")
                                     (map str/lower-case)
                                     str/join)))
        provided-value    (or value full-value)]
    [top-level-message root-message direct-message
     (some->> provided-value
             pr-str
             (str "Provided: "))
     docs-pointer-msg]))

(defn top-level-fn-error
  [errors]
  (first (filter #(and (empty? (:in %))
                    (= :fn (m/type (:schema %)))) errors)))

(def default-error-overrides
  {:errors
   (-> me/default-errors
       (assoc
        ::m/missing-key
        {:error/fn
         (fn [{:keys [in]} _]
           (let [k (-> in last name)]
             (str "Query is missing a '" k "' clause. "
                  "'" k "' is required in queries. "
                  "See documentation here for details: "
                  docs/error-codes-page "#query-missing-" k)))}
        ::m/extra-key
        {:error/fn
         (fn [{:keys [in]} _]
           (let [k (-> in last name)]
             (str "Query contains an unknown key: '" k "'. "
                  "See documentation here for more information on allowed query keys: "
                  docs/error-codes-page "#query-unknown-key")))}
        ::m/invalid-type
        {:error/fn (fn [{:keys [schema value]} _]
                     (if-let [expected-type (-> schema m/type)]
                       (str "should be a " (case expected-type
                                             (:map-of :map) "map"
                                             (:cat :catn :sequential) "sequence"
                                             :else (name type)))
                       (str "type of " (pr-str value) " does not match expected type")))}))})

(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the failure in plain english. The string
  contains contextual information about a specific error
  chosen from all the errors.

  Prefers top-level `:fn` errors, if present, otherwise
  chooses an error based on heuristics."
  [explained-error opts]
  (let [error-opts (or opts default-error-overrides)
        {:keys [errors]} explained-error
        e          (or (top-level-fn-error errors)
                       (choose-relevant-error explained-error))]
    (str/join "; " (remove nil? (distinct  (format-error explained-error e error-opts))))))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/predicate-schemas)
   {::iri                  [:or {:error/message "invalid iri"}
                            :string :keyword]
    ::json-ld-keyword      [:keyword {:decode/json decode-json-ld-keyword
                                      :decode/fql  decode-json-ld-keyword}]
    ::var                  [:fn {:error/message "variable should be one or more characters beginning with `?`"}
                            variable?]
    ::val                  [:fn value?]
    ::subject              ::iri
    ::function             [:orn
                            [:string-fn [:and :string [:re #"^\(.+\)$"]]]
                            [:list-fn [:and list? [:cat :symbol [:* any?]]]]
                            [:vector-fn [:and vector? [:cat [:enum :expr] [:* any?]]]]]
    ::as-function          [:orn {:error/message "subquery aggregates must be bound to a variable with 'as' e.g. '(as (sum ?x) ?x-sum)"}
                            [:string-fn [:and :string [:re #"^\(as .+\)$"]]]
                            [:list-fn [:and list? [:cat :symbol [:* any?]]]]]
    ::optional             [:+ {:error/message "optional pattern must be a sequence of valid where clauses."}
                            [:schema [:ref ::where]]]
    ::union                [:+ {:error/message "union pattern must be a sequence of valid where clauses."}
                            [:schema [:ref ::where]]]
    ::exists               [:+ {:error/message "exists pattern must be a sequence of valid where clauses."}
                            [:schema [:ref ::where]]]
    ::not-exists           [:+ {:error/message "not-exists pattern must be a sequence of valid where clauses."}
                            [:schema [:ref ::where]]]
    ::minus                [:+ {:error/message "minus pattern must be a sequence of valid where clauses."}
                            [:schema [:ref ::where]]]
    ::bind                 [:+ {:error/message "bind values must be mappings from variables to functions"}
                            [:catn [:var ::var]
                             [:binding ::function]]]
    ::where-op             [:and
                            :keyword
                            [:enum {:error/message "unrecognized where operation, must be one of: graph, filter, optional, union, bind, values, exists, not-exists, minus"}
                             :graph :filter :optional :union :bind :query :values :exists :not-exists :minus]]
    ::graph                [:orn {:error/message "value of graph. Must be a ledger name or variable"}
                            [:ledger ::ledger]
                            [:variable ::var]]
    ::node-map-key         [:orn {:error/message "node map keys must be an iri or variable"}
                            [:iri ::iri]
                            [:var ::var]]
    ::node-map-value       [:orn {:error/message "node map values must be an iri, string, number, boolean, map, variable, or array"}
                            [:var ::var]
                            [:string :string]
                            [:boolean :boolean]
                            [:int :int]
                            [:double :double]
                            [:nil :nil]
                            [:iri ::iri]
                            [:map [:ref ::node-map]]
                            [:collection [:sequential [:ref ::node-map-value]]]]
    ::node-map             [:map-of {:error/message "Invalid node map"
                                     :min 1}
                            [:ref ::node-map-key] [:ref ::node-map-value]]
    ::where-pattern        [:multi {:dispatch where-pattern-type
                                    :error/message "where clause patterns must be either a node map or a filter, optional, union, bind, query, or graph array."}
                            [:node ::node-map]
                            [:filter [:catn
                                      [:op ::where-op]
                                      [:fns [:* ::function]]]]
                            [:optional [:catn
                                        [:op ::where-op]
                                        [:clauses ::optional]]]
                            [:union [:catn
                                     [:op ::where-op]
                                     [:clauses ::union]]]
                            [:values [:catn
                                      [:op ::where-op]
                                      [:patterns ::values]]]
                            [:minus [:catn
                                     [:op ::where-op]
                                     [:patterns ::minus]]]
                            [:exists [:catn
                                      [:op ::where-op]
                                      [:patterns ::exists]]]
                            [:not-exists [:catn
                                          [:op ::where-op]
                                          [:patterns ::not-exists]]]
                            [:bind [:catn
                                    [:op ::where-op]
                                    [:bindings ::bind]]]
                            [:graph [:tuple ::where-op ::graph [:ref ::where]]]
                            ;; TODO - because ::subquery is a separate registry it cannot be called here, validated in f.d.q.fql.syntax/coerce-subquery until resolved
                            [:query [:catn
                                     [:op ::where-op]
                                     [:query [:map]]]]]
    ::where                [:orn {:error/message "where clause must be a single node map pattern or a sequence of where patterns"}
                            [:single ::where-pattern]
                            [:collection [:sequential ::where-pattern]]]
    ::construct            [:sequential ::node-map]
    ::ledger               ::iri
    ::from                 [:orn {:error/message "from must be a ledger iri or vector of ledger iris"}
                            [:single ::ledger]
                            [:collection [:sequential
                                          {:error/message "all values in `from`/`from-named` must be ledger iris"}
                                          ::ledger]]]
    ::from-named           ::from
    ::delete               [:orn {:error/message "delete statements must be a node map or sequence of node maps"}
                            [:single ::node-map]
                            [:collection [:sequential ::node-map]]]
    ::insert               [:orn {:error/message "insert statements must be a node map or sequence of node maps"}
                            [:single ::node-map]
                            [:collection [:sequential ::node-map]]]
    ::single-var-binding   [:tuple ::var [:sequential ::val]]
    ::multiple-var-binding [:tuple
                            [:sequential ::var]
                            [:sequential [:sequential ::val]]]
    ::values               [:orn
                            [:single ::single-var-binding]
                            [:multiple ::multiple-var-binding]]
    ::modification-txn     [:and
                            [:map-of ::json-ld-keyword :any]
                            [:map
                             [:context {:optional true} ::context]
                             [:delete {:optional true} ::delete]
                             [:insert {:optional true} ::insert]
                             [:where {:optional true} ::where]
                             [:values {:optional true} ::values]]]
    ::context              :any}))
