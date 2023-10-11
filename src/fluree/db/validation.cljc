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
;;maybe direct errors always, and root for the "top" ?
(def ERR (atom []))

(defn get-latest-common-or
  [errors schema]
  (let [first-same-ins (->> errors
                            (sort-by (fn [{:keys [in path]}]
                                       [(- (count in)) (count path)]))
                            (partition-by #(count (:in %)))
                            first
                            first)
        {:keys [path]} first-same-ins
        subpath (loop [i (count path)]
                  (let [subpath (subvec path 0 i)]
                    (cond
                      (= 0 i) nil
                      (#{:or :orn} (m/type (mu/get-in schema subpath))) subpath
                      :else (recur (dec i) ))))]
    {:value (:value first-same-ins)
     :subpath subpath}))


;;TODO better docstring
;;TODO if there isn't an or, use the message from the longest `:in`?
;;TODO: same length `:in` paths, but what if different paths?
(defn format-explained-errors
  "Takes the output of `explain` and emits a string
  explaining the error(s) in plain english. "
  ([explained-error] (format-explained-errors explained-error {}))
  ([explained-error error-opts]
   (let [{:keys [errors schema value]} explained-error
         {or-value :value :keys [subpath]} (get-latest-common-or errors schema)
         or-message (when subpath
                    (:error/message (m/properties (mu/get-in schema subpath))))
         [_ first-root-message] (me/-resolve-root-error
                                explained-error
                                (first errors)
                                error-opts)
         [_ first-direct-message] (me/-resolve-direct-error
                                  explained-error
                                  (first errors)
                                  error-opts)]
     (str (str/join " "  (distinct [first-root-message or-message first-direct-message]))
          " Provided: " (or or-value value)))))


(comment
  ;;idea: top error, root and child.
  ;; find the longest in, get its root and child., break tie breakers by shortest path
  (reset! ERR [])
  (def select-errs (first @ERR))
  (count (first select-errs))
  (let [[es] select-errs]
    (mapv #(select-keys % [:path :in :value :child-message :root-message]) es))

  (sort-by (comp count :in)
           select-errs)

  (sort-by (fn [{:keys [in path]}]
             [(- (count in)) (count path)])
           )
  ;; (require '[fluree.db.query.fql.syntax :as syntax])

  ;; (let [p (->> (get @ERR 3)
  ;;              (sort-by (fn [{:keys [in path]}]
  ;;                         [(- (count in)) (count path)]))
  ;;              (partition-by #(count (:in %)))
  ;;              first
  ;;              first
  ;;              :path)]
  ;;   (mu/get-in (m/schema ::syntax/query {:registry syntax/registry}) (subvec p (count p))))

  ;; (let [same-ins (->> (get @ERR 2)
  ;;                     (sort-by (fn [{:keys [in path]}]
  ;;                                [(- (count in)) (count path)]))
  ;;                     (partition-by #(count (:in %)))
  ;;                     first)
  ;;       {:keys [path]} (first same-ins)
  ;;       s (m/schema ::syntax/query {:registry syntax/registry})
  ;;       subpath (loop [i (count path)]
  ;;                 (let [subpath (subvec path 0 i)]
  ;;                   (cond
  ;;                     (= 0 i) path
  ;;                     (#{:or :orn} (m/type (mu/get-in s subpath))) subpath
  ;;                     :else (recur (dec i) ))))]
  ;;   {:same-ins-n (count same-ins)
  ;;    :same-ins same-ins
  ;;    :path path
  ;;    :subpath subpath
  ;;    :val (:value (first same-ins))
  ;;    :get-in (mu/get-in s subpath)
  ;;    :props (m/properties (mu/get-in s subpath))})
  ;;more than one with the same length of :in


  (->> @ERR
       first
       (group-by (comp pop :path))
       (into (sorted-map-by (fn [l r]
                              (compare (mapv pr-str l) (mapv pr-str r))))))
  (first @ERR)

  ;;TODO invalid type override.

  ;;when sorted. history examples:
  ;;1. want (distinct root child) of the  first. could get away with root of second.)
  ;;2. only one. want root-message.
  ;;3. (distinct root child) of the first. could get away with root of second.
  ;;4. (distinct root child) of the first. could get away with root of second.
  ;;5. (distinct root child) of the first. no second
  ;;6. all root messages. so I guess (distinct root child) of the first is also ok.
  ;; 7. (distinct root child), only one.
  ;; 8. (distinct root child, only one).

  ;;fql_query
  (def fql-errs @ERR)
  ;;order-by is not an error?
  ;;
  (get fql-errs 1)
  ;;when sorted. fql examples:
  ;;1. want root-messag eof the first. no child message.
  ;;2. root + child. 2nd root is a repeat.
  ;;3. root + child.
  ;;4.  just not good. where ?s.  just invalid type
  ;; 5. just not good. unwrapped where doesn't ever tell you what's wrong.
  ;;6. root + child of first (child is nil).
  ;;7. bind just not good. no child for bind?
  ;;8. want the samllest root message + child -message. could get away with second-smallest child-message.
  ;;   maybe filter just isn't set up well
  ;;9. last root+child. anotehr filter one.

  ;;to fix:
  ;; bind children
  ;; maybe make it so filter can also be the highest? tinker with filter
  ;; unwrapped where, etc. is a little broken.
  ;; bind?






  (reset! ERR [])
  (def errs (last @ERR))
  (group-by #(select-keys % [:child-message :root-message]) @ERR)

  (let [both (into [] (filter (fn [{:keys [root-message child-message]}]
                                (and root-message child-message)))errs)]
    (into [] (map (fn [{:keys [root-message child-message]}]
                    (str root-message child-message))) both)
    #_(into roots children))
  ;;
  ;;could group-by the select-keys, take both if you can, if not then the children
  (first @ERR)
  ;;bad-where-map
  ;;top failure
  errs

  ;;want in order of path length


  [{[0 2 :where 0 0 0 :tuple 0]
    [{:path [0 2 :where 0 0 0 :tuple 0 :remote],
      :schema [:sequential {:max 4} :any],
      :value
      {:union
       [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
       :filter ["(> ?age 30)"]},
      :type :malli.core/invalid-type,
      :root-message "Not a valid where map or tuple.",
      :root-path [:where 0],
      :root-op :malli.core/schema,
      :child-message nil,
      :in [:where 3]}],


    ;;failed where-map
    [0 2 :where 0 0 0 :where-map 0]
    [{:path [0 2 :where 0 0 0 :where-map 0 0],
      :schema
      [:map-of
       {:max 1, :error/message "Only one key/val"}
       :fluree.db.validation/where-op
       :any],
      :value
      {:union
       [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
       :filter ["(> ?age 30)"]},
      :type :malli.core/limits,
      :root-message "Not a valid where map or tuple.",
      :root-path [:where 0],
      :root-op :malli.core/schema,
      :child-message "Only one key/val",
      :in [:where 3]}],

    ;;failed triple
    [0 2 :where 0 0 0 :tuple 0 :triple]
    [{:path [0 2 :where 0 0 0 :tuple 0 :triple 0],
      :schema
      [:catn
       [:subject
        [:orn
         [:var :fluree.db.validation/var]
         [:val :fluree.db.validation/subject]]]
       [:predicate
        [:orn
         [:var :fluree.db.validation/var]
         [:iri :fluree.db.validation/iri]]]
       [:object
        [:orn
         [:var :fluree.db.validation/var]
         [:ident [:fn "[fluree.db.util.core/pred-ident?]"]]
         [:iri-map :fluree.db.validation/iri-map]
         [:val :any]]]],
      :value
      {:union
       [[['?s :ex/email '?email]] [['?s :schema/email '?email]]],
       :filter ["(> ?age 30)"]},
      :type :malli.core/invalid-type,
      :root-message "Not a valid where map or tuple.",
      :root-path [:where 0],
      :root-op :malli.core/schema,
      :child-message nil,
      :in [:where 3]}]}]

  )

(def registry
  (merge
   (m/base-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   {::iri                  [:or {:error/message "Not a valid iri."} :string :keyword]
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
                            [:string [:fn {:error/message "Not a valid function."} fn-string?]]
                            [:list [:fn {:error/message "Not a valid list of functions"}fn-list?]]]
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
    ::where-map            [:and {:error/message "Invalid where map."}
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
    ::where                [:sequential {:error/message "Invalid where statement."}
                            [:orn {:error/message "Where clause does not contain valid tuple or map operation."}
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
