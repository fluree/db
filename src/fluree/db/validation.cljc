(ns fluree.db.validation
  (:require [clojure.string :as str]
            [fluree.db.util.core :refer [pred-ident?]]
            [fluree.db.constants :as const]
            [malli.core :as m]
            [malli.error :as me]
            [malli.util :as mu]))

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

(defn fn-string?
  [x]
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))

(defn fn-list?
  [x]
  (and (list? x)
       (-> x first symbol?)))

(defn query-fn?
  [x]
  (or (fn-string? x) (fn-list? x)))

(defn as-fn?
  [x]
  (or (and (fn-string? x) (str/starts-with? x "(as "))
      (and (fn-list? x) (-> x first (= 'as)))))

(defn humanize-error
  [error]
  (-> error ex-data :data :explain me/humanize))

(defn explain-error
  [error]
  (-> error ex-data :data :explain))

(defn nearest-or-parent
  [error schema error-opts]
  (let [{:keys [value path]} error]
    (loop [i (count path)]
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

(defn longest-in-errors
  [errors schema error-opts]
  (let [in-with-tiebreak (fn [{:keys [schema value in path]}]
                           (let [type (m/type schema)]
                             [(- (count in)) (if (and (#{:map :map-of} type)
                                                      (map? value))
                                               -1
                                               1)]))]
    (->> errors
         (sort-by in-with-tiebreak)
         (partition-by in-with-tiebreak)
         first)))

(defn most-specific-relevant-error
  [errors schema error-opts]
  (let [longest-in-errors (longest-in-errors errors schema error-opts)]
    (if (= (count longest-in-errors) 1)
      (first longest-in-errors)
      (let [common-in (val (first (group-by :in longest-in-errors)))
            [e & es] common-in
            parent-or (loop [{:keys [path] :as parent} (nearest-or-parent e schema error-opts)]
                        (when parent
                          (if (every? (fn [err]
                                        (let [path' (:path err)]
                                          (when (<= (count path) (count path'))
                                            (= path (subvec path' 0 (count path)))))) es)
                            parent
                            (recur (nearest-or-parent parent schema error-opts)))))]
        (or parent-or (first common-in))))))

;;Based on `-resolve-root-error`: https://github.com/metosin/malli/blob/a43c28d90b4eb18054df2a21c91a18d4b58cacc2/src/malli/error.cljc#L268
;;with fix to correctly calculate `:value` in root error messages, and guard against `:invalid-schema` exceptions
;;due to values having keys that are not present in the schema
(defn resolve-parent-for-segment
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
        [_ direct-message] (me/-resolve-direct-error
                            explained
                            error
                            error-opts)
        [_ root-message] (resolve-parent-for-segment
                          explained
                          error
                          error-opts)
        top-level-message (when-not (= ::m/extra-key (:type error))
                            (when-let [top-level-key (first (filter keyword? path))]
                              (str "Error in value for \"" (name top-level-key) "\"")))]
    [top-level-message root-message direct-message (str "Provided: " (pr-str value))]))

(defn top-level-fn-error
  [errors]
  (first (filter #(and (empty? (:in %))
                    (= :fn (m/type (:schema %)))) errors)))

(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the error(s) in plain english. "
  ([explained-error] (format-explained-errors explained-error {}))
  ([explained-error error-opts]
   (let [{:keys [errors schema value]} explained-error
         [first-e] errors
         e (or (top-level-fn-errors errors)
               (most-specific-relevant-error errors schema error-opts))
         msgs (format-error explained-error e error-opts)]
     (str/join "; " (remove nil? (distinct  (format-error explained-error e error-opts)))))))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   {::iri                  [:or {:error/message "Not a valid iri"} :string :keyword]
    ::iri-key              [:fn iri-key?]
    ::iri-map              [:map-of {:max 1}
                            ::iri-key ::iri]
    ::json-ld-keyword      [:keyword {:decode/json decode-json-ld-keyword
                                      :decode/fql  decode-json-ld-keyword}]
    ::var                  [:fn {:error/message "Invalid variable, should be one or more characters begin with `?`"}
                            variable?]
    ::val                  [:fn value?]
    ::subject              [:orn
                            [:sid [:fn {:error/message "Invalid subject id"} sid?]]
                            [:ident [:fn pred-ident?]]
                            [:iri ::iri]]
    ::triple               [:catn {:error/message "Invalid triple"}
                            [:subject [:orn
                                       [:var ::var]
                                       [:val ::subject]]]
                            [:predicate [:orn
                                         [:var ::var]
                                         [:iri ::iri]]]
                            [:object [:orn
                                      [:var ::var]
                                      [:ident [:fn pred-ident?]]
                                      [:iri-map ::iri-map]
                                      [:val :any]]]]
    ::function             [:orn
                            [:string [:fn {:error/message "Not a valid function"}
                                      fn-string?]]
                            [:list [:fn {:error/message "Not a valid list of functions"}
                                    fn-list?]]]
    ::where-pattern        [:orn {:error/message "Invalid where pattern, must be a where map or tuple"}
                            [:map ::where-map]
                            [:tuple ::where-tuple]]
    ::filter               [:sequential {:error/message "Filter must be a function call wrapped in a vector"} ::function]
    ::optional             [:orn {:error/message "Invalid optional"}
                            [:single ::where-pattern]
                            [:collection [:sequential ::where-pattern]]]
    ::union                [:sequential [:sequential ::where-pattern]]
    ::bind                 [:map-of {:error/message "Invalid bind, must be a map with variable keys"} ::var :any]
    ::where-op             [:enum {:decode/fql  string->keyword
                                   :decode/json string->keyword
                                   :error/message "Unrecognized operation in where map, must be one of: filter, optional, union, bind"}
                            :filter :optional :union :bind]
    ::where-map            [:and
                            [:map-of {:max 1 :error/fn (fn [{:keys [value]} _]
                                                         ;;this can fail either the `:map-of` or the `:max`
                                                         (when (and (map? value)
                                                                    (not= 1 (count value)))
                                                           "Where map can only have one key/value pair"))}
                             ::where-op :any]
                            [:multi {:dispatch where-op}
                             [:filter [:map [:filter [:ref ::filter]]]]
                             [:optional [:map [:optional [:ref ::optional]]]]
                             [:union [:map [:union [:ref ::union]]]]
                             [:bind [:map [:bind [:ref ::bind]]]]]]
    ::where-tuple          [:orn {:error/message
                                  ;;TODO
                                  "Invalid tuple"}
                            [:triple ::triple]
                            [:remote [:sequential {:max 4} :any]]]
    ::where                [:sequential {:error/message "Where must be a vector of clauses"}
                            [:orn {:error/message "where clauses must be valid tuples or maps"}
                             [:where-map ::where-map]
                             [:tuple ::where-tuple]]]
    ::delete               [:orn {:error/message "delete statements must be a triple or collection of triples"}
                            [:single ::triple]
                            [:collection [:sequential ::triple]]]
    ::insert               [:orn
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
