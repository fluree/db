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

(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the error(s) in plain english. "
  [explained-error]
  (let [{:keys [errors schema value]} explained-error
        error-data (->> errors
                        (mapv #(-> %
                                   (assoc :message (me/error-message %))
                                   (assoc :op (m/type (mu/get-in schema (pop (:path %)))))))
                        (group-by (comp pop :path))
                        ;;ensure errors always print in the same order
                        (into (sorted-map-by (fn [l r]
                                               (compare (mapv pr-str l)
                                                        (mapv pr-str r))))))]
    (if-let [top-level-error (get error-data [0])]
      (-> top-level-error first :message)
      (str/join "\n"(into [] (map (fn [[path data]]
                                    (let [{:keys [op in]} (first data)
                                          provided (get-in value in)
                                          position (peek in)]
                                      (str "Error"
                                           ;;TODO position might be a number, as in a vector.
                                           (when position (str " in value of field " position))
                                           ": must meet " (case op
                                                            :or "one of "
                                                            :and "all of "
                                                            "") "the following criteria: "
                                           (str/join ", "  (map :message data))
                                           "\n Provided: " (pr-str provided)))))
                          error-data)))))

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
    ::var                  [:fn variable?]
    ::val                  [:fn value?]
    ::subject              [:orn
                            [:sid [:fn sid?]]
                            [:ident [:fn pred-ident?]]
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
                                      [:ident [:fn pred-ident?]]
                                      [:iri-map ::iri-map]
                                      [:val :any]]]]
    ::function             [:orn
                            [:string [:fn fn-string?]]
                            [:list [:fn fn-list?]]]
    ::where-pattern        [:orn
                            [:map ::where-map]
                            [:tuple ::where-tuple]]
    ::filter               [:sequential ::function]
    ::optional             [:orn
                            [:single ::where-pattern]
                            [:collection [:sequential ::where-pattern]]]
    ::union                [:sequential [:sequential ::where-pattern]]
    ::bind                 [:map-of ::var :any]
    ::where-op             [:enum {:decode/fql  string->keyword
                                   :decode/json string->keyword}
                            :filter :optional :union :bind]
    ::where-map            [:and
                            [:map-of {:max 1} ::where-op :any]
                            [:multi {:dispatch where-op}
                             [:filter [:map [:filter [:ref ::filter]]]]
                             [:optional [:map [:optional [:ref ::optional]]]]
                             [:union [:map [:union [:ref ::union]]]]
                             [:bind [:map [:bind [:ref ::bind]]]]]]
    ::where-tuple          [:orn
                            [:triple ::triple]
                            [:remote [:sequential {:max 4} :any]]]
    ::where                [:sequential [:orn
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
