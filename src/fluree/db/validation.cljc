(ns fluree.db.validation
  (:require [fluree.db.constants :as const]
            [fluree.db.util.core :refer [pred-ident?]]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]
            [clojure.string :as str]))

(defn iri?
  [v]
  (or (keyword? v) (string? v)))

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

(def value? (complement coll?))

(defn sid?
  [x]
  (int? x))

(defn iri-key?
  [x]
  (= const/iri-id x))

(defn where-op [x]
  (when (map? x)
    (-> x first key)))

(defn string->keyword
  [x]
  (if (string? x)
    (keyword x)
    x))

(defn humanize-error
  [error]
  (-> error ex-data :data :explain me/humanize))

(defn explain-error
  [error]
  (-> error ex-data :data :explain))

(defn nearest-or-parent
  "If a given error is the child of a disjunction,
   returns the error data corresponding to that disjunction."
  [error schema]
  (let [{:keys [value path]} error]
    (loop [i (dec (count path))]
      (when-not (= 0 i)
        (let [subpath (subvec path 0 i)
              schema-fragment (mu/get-in schema subpath)
              type (some-> schema-fragment
                           m/type)]
          (if (#{:or :orn} type)
            (let [props (m/properties schema-fragment)
                  in (mu/path->in schema subpath)]
              {:schema schema-fragment
               :path subpath
               :type type
               :in in
               :value value})
            (recur (dec i) )))))))

(defn error-specificity-score
  "Given an error, applies a heursitic
   intended to favor errors corresponding
   to smaller/more specific parts of a failing value.
   Those errors should hopefully be more relevant to
   users' intent.

   When used in sorting, will push those errors
   toward the start of the list. "
  [error]
  (let [{:keys [schema value in path]} error
        type (m/type schema)]
    ;;When inline property constraints, eg limits like
    ;;`[:map {:max 1} ...]` are used, then both type and limit
    ;; failures will have the same `:in`, despite the limit failure
    ;; being more specific. This second number differentiates
    ;; those cases.
    [(- (count in)) (if (and (#{:map :map-of} type)
                             (map? value))
                      -1
                      1)]))

(defn choose-relevant-error
  "Calculates the most specific error (per our heuristic).
   If there are more than one of equal specificity,
   chooses a chunk of errors which share the same `:in`
   (portion of the value whch failed), and attempts to
   find the nearest disjunction which contains all of
   those errors. "
  [{:keys [errors schema] :as _explained-error}]
  (let [most-specific-errors  (->> errors
                                   (sort-by error-specificity-score)
                                   (partition-by error-specificity-score)
                                   first)]
    (if (= (count most-specific-errors) 1)
      (first most-specific-errors)
      (let [same-in (val (first (group-by :in most-specific-errors)))
            [e & es] same-in
            or-parent (loop [{:keys [path] :as parent} (nearest-or-parent e schema)]
                        (when parent
                          (if (every? (fn [err]
                                        (let [path' (:path err)]
                                          (when (<= (count path) (count path'))
                                            (= path (subvec path' 0 (count path)))))) es)
                            parent
                            (recur (nearest-or-parent parent schema)))))]
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


(defn format-error
  [explained error error-opts]
  (let [{:keys [path value]} error
        top-level-message (when-not (= ::m/extra-key (:type error))
                            (when-let [top-level-key (first (filter keyword? path))]
                              (str "Error in value for \"" (name top-level-key) "\"")))
        [_ root-message] (resolve-root-error-for-in
                          explained
                          error
                          error-opts)
        [_ direct-message] (me/-resolve-direct-error
                            explained
                            error
                            error-opts)]
    [top-level-message root-message direct-message (str "Provided: " (pr-str value))]))

(defn top-level-fn-error
  [errors]
  (first (filter #(and (empty? (:in %))
                    (= :fn (m/type (:schema %)))) errors)))

(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the failure in plain english.

  Prefers top-level `:fn` errors, if present, otherwise
  chooses an error based on heuristics."
  ([explained-error] (format-explained-errors explained-error {}))
  ([explained-error error-opts]
   (let [{:keys [errors schema value]} explained-error
         [first-e] errors
         e (or (top-level-fn-error errors)
               (choose-relevant-error explained-error))]
     (str/join "; " (remove nil? (distinct  (format-error explained-error e error-opts)))))))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/predicate-schemas)
   {::iri                  [:or :string :keyword]
    ::iri-key              [:fn iri-key?]
    ::iri-map              [:map-of {:max 1}
                            ::iri-key ::iri]
    ::json-ld-keyword      [:keyword {:decode/json decode-json-ld-keyword
                                      :decode/fql  decode-json-ld-keyword}]
    ::var                  [:fn {:error/message "Invalid variable, should be one or more characters begin with `?`"}
                            variable?]
    ::val                  [:fn value?]
    ::subject              [:orn {:error/message "Subject must be a subject id, ident, or iri"}
                            [:sid [:fn {:error/message "Invalid subject id"} sid?]]
                            [:ident [:fn {:error/message "Invalid pred ident, must be two-tuple of [pred-name-or-id pred-value] "}pred-ident?]]
                            [:iri ::iri]]
    ::triple               [:catn
                            [:subject [:orn
                                       [:var ::var]
                                       [:val ::subject]]]
                            [:predicate [:orn
                                         [:var ::var]
                                         [:iri ::iri]]]
                            [:object [:orn
                                      [:var ::var]
                                      [:ident [:fn {:error/message "Invalid pred ident, must be two-tuple of [pred-name-or-id pred-value] "}pred-ident?]]
                                      [:iri-map ::iri-map]
                                      [:val :any]]]]
    ::function             [:orn
                            [:string-fn [:and :string [:re #"^\(.+\)$"]]]
                            [:list-fn [:and list? [:cat :symbol [:* any?]]]]]
    ::where-pattern        [:orn {:error/message "Invalid where pattern, must be a where map or tuple"}
                            [:map ::where-map]
                            [:tuple ::where-tuple]]
    ::filter               [:sequential {:error/message "Filter must be a function call wrapped in a vector"} ::function]
    ::optional             [:orn {:error/message "Invalid optional, must be a signle where pattern or vector of where patterns."}
                            [:single ::where-pattern]
                            [:collection [:sequential ::where-pattern]]]
    ::union                [:sequential [:sequential ::where-pattern]]
    ::bind                 [:map-of {:error/message "Invalid bind, must be a map with variable keys"} ::var :any]
    ::where-op             [:enum {:decode/fql  string->keyword
                                   :decode/json string->keyword
                                   :error/message "Unrecognized operation in where map, must be one of: filter, optional, union, bind"}
                            :filter :optional :union :bind]
    ::where-map            [:and
                            [:map-of {:max 1 :error/message "Where map can only have 1 key/value pair"}
                             ::where-op :any]
                            [:multi {:dispatch where-op}
                             [:filter [:map [:filter [:ref ::filter]]]]
                             [:optional [:map [:optional [:ref ::optional]]]]
                             [:union [:map [:union [:ref ::union]]]]
                             [:bind [:map [:bind [:ref ::bind]]]]]]
    ::where-tuple          [:orn {:error/message "Invalid tuple"}
                            [:triple ::triple]
                            [:remote [:sequential {:max 4} :any]]]
    ::where                [:sequential {:error/message "Where must be a vector of clauses"}
                            [:orn {:error/message "where clauses must be valid tuples or maps"}
                             [:where-map ::where-map]
                             [:tuple ::where-tuple]]]
    ::delete               [:orn {:error/message "delete statements must be a triple or vector of triples"}
                            [:single ::triple]
                            [:collection [:sequential ::triple]]]
    ::insert               [:orn {:error/message "insert statements must be a triple or vector of triples"}
                            [:single ::triple]
                            [:collection [:sequential ::triple]]]
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
                             [:delete ::delete]
                             [:insert {:optional true} ::insert]
                             [:where ::where]
                             [:values {:optional true} ::values]]]
    ::context              :any}))
