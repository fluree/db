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

(def ERR (atom []))
(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the error(s) in plain english.

  Chooses the most general error (highest in the schema tree)"
  ([explained-error] (format-explained-errors explained-error {}))
  ([explained-error error-opts]
   (let [{:keys [errors schema value]} explained-error
         error-data (->> errors
                         (mapv (fn [e]
                                 (let [[root-path root-message :as root] (me/-resolve-root-error
                                                                           explained-error
                                                                           e
                                                                           error-opts)
                                       [child-path child-message :as direct] (me/-resolve-direct-error
                                                                               explained-error
                                                                               e
                                                                               error-opts)]
                                   (-> e
                                       (assoc :child-message child-message)
                                       (assoc :root-message root-message))))))
         sorted (sort-by #(count (:path %)) error-data)]
     (swap! ERR conj sorted)
     (str (str/join " " (distinct (mapcat (fn [{:keys [child-message root-message]}]
                                            [root-message child-message]) sorted)))
          " Provided: " value))))

;;TODO simplify
#_(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the error(s) in plain english.

  Chooses the most general error (highest in the schema tree)"
  ([explained-error] (format-explained-errors explained-error {}))
  ([explained-error error-opts]
   (let [{:keys [errors schema value]} explained-error
         error-data (->> errors
                         (mapv (fn [e]
                                 (let [[path message] (me/-resolve-root-error
                                                        explained-error
                                                        e
                                                        error-opts)]
                                   (-> e
                                       ;;  (assoc :path path)
                                       ;;  (assoc :in (mu/path->in schema path))
                                       ;;TODO  Provided value is wrong because mu/path->in is not
                                       ;; the right thing here.
                                       ;;  (assoc :value (get-in value (mu/path->in schema path)))
                                       (assoc :message message)
                                       (assoc :op (m/type (mu/get-in schema (pop (:path e)))))))))
                         ;;TODO now that we resolve-root-error, this group-by might not make sense
                         (group-by (comp pop :path))
                         (into (sorted-map-by (fn [l r]
                                                (compare (mapv pr-str l)
                                                         (mapv pr-str r))))) )]
     ;;TODO: check that this is even needed and works in the absence of root errors

     (let [[path data] (first error-data)]
       (let [{:keys [op in value]} (first data)
             position (peek in)
             messages (distinct
                        (map (fn [{inner-schema :schema
                                   :keys [message]}]
                               ;;override unhelpful type errors
                               (if (= message "invalid type")
                                 (str "should be "
                                      (name (m/type inner-schema))
                                      ".")
                                 message))
                             data))]
         (str
           (when (> (count messages) 1)
             (str "Errors: must meet "
                  (case op
                    (:or :orn) "one of "
                    :and "all of "
                    "")
                  "the following criteria: "))
           (str/join ", " messages )
           ;;TODO, wrong value is provided sometimes?
           " Provided: " (pr-str value)))))))

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   {::iri                  [:or :string :keyword]
    ::iri-key              [:fn iri-key?]
    ::iri-map              [:map-of {:max 1}
                            ::iri-key ::iri]
    ::json-ld-keyword      [:keyword {:decode/json decode-json-ld-keyword
                                      :decode/fql  decode-json-ld-keyword}]
    ::var                  [:fn {:error/message "Invalid variable, should begin with `?`"}
                            variable?]
    ::val                  [:fn value?]
    ::subject              [:orn
                            [:sid [:fn sid?]]
                            [:ident [:fn pred-ident?]]
                            [:iri ::iri]]
    ::triple               [:catn {:error/message "Invalid triple. "}
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
    ::function             [:orn {:error/message "Invalid function. TODO"}
                            [:string [:fn fn-string?]]
                            [:list [:fn fn-list?]]]
    ::where-pattern        [:orn
                            [:map ::where-map]
                            [:tuple ::where-tuple]]
    ::filter               [:sequential {:error/message "Filter must be sequential. "} ::function]
    ::optional             [:orn {:error/message "Invalid optional."}
                            [:single ::where-pattern]
                            [:collection [:sequential ::where-pattern]]]
    ::union                [:sequential [:sequential ::where-pattern]]
    ::bind                 [:map-of {:error/message "Invalid bind."} ::var :any]
    ::where-op             [:enum {:decode/fql  string->keyword
                                   :decode/json string->keyword}
                            :filter :optional :union :bind]
    ::where-map            [:and
                            [:map-of {:max 1
                                      :error/message "Where map can only have one key/value pair."} ::where-op :any]
                            [:multi {:dispatch where-op
                                     :error/message "Unrecognized operation in where map."}
                             [:filter [:map [:filter [:ref ::filter]]]]
                             [:optional [:map [:optional [:ref ::optional]]]]
                             [:union [:map [:union [:ref ::union]]]]
                             [:bind [:map [:bind [:ref ::bind]]]]]]
    ::where-tuple          [:orn
                            [:triple ::triple]
                            [:remote [:sequential {:max 4} :any]]]
    ::where                [:sequential
                            [:orn {:error/message "Not a valid where map or tuple."}
                             [:where-map ::where-map]
                             [:tuple ::where-tuple]]]
    ::delete               [:orn
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
