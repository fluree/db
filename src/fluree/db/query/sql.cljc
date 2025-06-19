(ns fluree.db.query.sql
  (:require #?(:clj  [clojure.java.io :as io]
               :cljs [fluree.db.util.cljs-shim :refer-macros [inline-resource]])
            #?(:clj  [instaparse.core :as insta]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])
            [clojure.string :as str]
            [fluree.db.query.sql.template :as template]))

#?(:clj (set! *warn-on-reflection* true))

#?(:cljs
   (def inline-grammar
     "SQL grammar in instaparse compatible BNF format loaded at compile time so it's
     available to cljs and js artifacts."
     (inline-resource "fluree-sql.bnf")))

#?(:clj
   (def sql
     (-> "fluree-sql.bnf"
         io/resource
         (insta/parser :input-format :ebnf)))

   :cljs
   (defparser sql inline-grammar :input-format :ebnf))

(defn rule-tag
  [r]
  (first r))

(defn rule?
  [elt]
  (and (sequential? elt)
       (keyword? (rule-tag elt))))

(def reserved-words
  "Keyword rule tags representing the SQL reserved words"
  #{:all :and :as :asc :at :between :case :coalesce :collate :corresponding
    :cross :current-date :current-time :current-timestamp :desc :distinct :else
    :end :except :exists :false :from :full :group-by :having :in :inner
    :intersect :is :join :left :limit :local :natural :not :null :nullif :on
    :or :offset :order-by :right :select :some :table :then :trim :true :unique
    :unknown :using :values :when :where})

(def rules
  "Hierarchy of SQL BNF rule name keywords for parsing equivalence"
  (let [derive-all (fn [hier coll kw]
                     (reduce (fn [h elt]
                               (derive h elt kw))
                             hier coll))]
    (-> (make-hierarchy)
        (derive :column-name ::string)
        (derive :character-string-literal ::string)
        (derive-all reserved-words ::reserved))))

(defmulti rule-parser
  "Parse SQL BNF rules depending on their type. Returns a function whose return
  chain will eventually contain the parsed rule after repeated execution."
  rule-tag
  :hierarchy #'rules)

(defn parse-rule
  "Uses `trampoline` to allow for mutual recursion without consuming call stack
  space when executing the rule parser on deeply nested rule trees."
  [r]
  (-> r rule-parser trampoline))

(defn parse-element
  [e]
  (if (rule? e)
    (parse-rule e)
    e))

(defn parse-all
  [elts]
  (mapcat parse-element elts))

(defn parse-into-map
  [elts]
  (->> elts
       (group-by rule-tag)
       (reduce-kv (fn [rules tag lst]
                    (assoc rules tag (parse-all lst)))
                  {})))

(def merge-parsed
  "Function to merge parsed (sub) query trees"
  (partial merge-with into))

(defn bounce
  "Returns a function that, when executed, returns the argument supplied to this
  function, `v`, wrapped in a vector if `v` is a rule or scalar, or it returns
  `v` itself if `v` is a list of rules or scalars. Used for mutually recursive
  parse function's return values in conjunction with `trampoline` to conserve
  call-stack space."
  [v]
  (if (and (sequential? v)
           (not (rule? v)))
    (constantly v)
    (constantly [v])))

(defmethod rule-parser :default
  [[_ & rst]]
  (->> rst parse-all bounce))

(defmethod rule-parser ::reserved
  [[_ & words]]
  (->> words
       (map parse-element)
       flatten
       (str/join " ")
       str/upper-case
       bounce))

(defmethod rule-parser :unsigned-integer
  [[_ & rst]]
  (->> rst
       parse-all
       (apply str)
       #?(:clj  Long/parseLong
          :cljs js/Number.parseInt)
       bounce))

(defmethod rule-parser :double-quote
  [_]
  (bounce \"))

(defmethod rule-parser ::string
  [[_ & rst]]
  (->> rst
       parse-all
       (remove #{\'})
       (apply str)
       bounce))

(defmethod rule-parser :qualifier
  [[_ q]]
  (-> q
      parse-element
      first
      ::coll
      bounce))

(defmethod rule-parser :subject-placeholder
  [[_ _ & rst]]
  (bounce {::obj (->> rst
                      parse-all
                      (apply str)
                      (template/combine-str template/collection-var))}))

(defmethod rule-parser :unsigned-value-specification
  [[_ v]]
  (bounce {::obj (-> v parse-element first)}))

(defmethod rule-parser :column-reference
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        pred      (some-> parse-map
                          :column-name
                          first
                          template/field->predicate-template)
        subject   (some-> parse-map
                          :subject-placeholder
                          first)
        coll      (-> parse-map
                      :qualifier
                      first)]

    (cond->> (or subject
                 {::subj template/collection-var, ::pred pred})
      coll (template/fill-in-collection coll)
      true bounce)))

(defmethod rule-parser :set-quantifier
  [[_ q]]
  (let [quantifier (-> q parse-element first)
        k          (if (= quantifier "DISTINCT") :selectDistinct :select)]
    (bounce k)))

(defmethod rule-parser :asterisk
  [_]
  (bounce {::select {template/collection-var ["*"]}}))

(defmethod rule-parser :set-function-type
  [[_ t]]
  (-> t rule-tag name bounce))

(defmethod rule-parser :general-set-function
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        func      (-> parse-map :set-function-type first)
        val-exp   (-> parse-map
                      :value-expression
                      first)
        subj      (-> val-exp
                      ::subj
                      (or template/collection-var))
        pred      (::pred val-exp)
        val       (template/build-var pred)
        distinct? (-> parse-map
                      :set-quantifier
                      first
                      (= :selectDistinct))]
    (cond-> {::subj subj, ::pred pred, ::obj val}
      distinct? (update ::obj (partial template/build-fn-call "distinct"))
      true      (-> (update ::obj (partial template/build-fn-call func))
                    bounce))))

(defmethod rule-parser :select-list-element
  [[_ & rst]]
  (let [parse-map                (parse-into-map rst)
        {::keys [subj pred obj]} (->> parse-map :derived-column first)
        pred-var                 (some-> pred template/build-var)
        selected                 (or obj pred-var)
        triple                   [subj pred pred-var]]
    (cond-> {::select [selected]}
      (template/predicate? pred) (assoc ::where [triple])
      true                       bounce)))

(defmethod rule-parser :select-list
  [[_ & rst]]
  (->> rst
       parse-all
       (apply merge-parsed)
       bounce))

(defmethod rule-parser :between-predicate
  [[_ & rst]]
  (let [parsed     (parse-all rst)
        [col l u]  (filter (complement #{"AND" "BETWEEN" "NOT"})
                           parsed)
        pred       (::pred col)
        lower      (::obj l)
        upper      (::obj u)
        field-var  (template/build-var pred)
        selector   [template/collection-var pred field-var]
        refinement (if (some #{"NOT"} parsed)
                     {:union [{:filter [(template/build-fn-call ["<" field-var lower])]}
                              {:filter [(template/build-fn-call [">" field-var upper])]}]}
                     {:filter [(template/build-fn-call [">=" field-var lower])
                               (template/build-fn-call ["<=" field-var upper])]})]
    (bounce [selector refinement])))

(defmethod rule-parser :comparison-predicate
  [[_ & rst]]
  (let [parse-map    (parse-into-map rst)
        comp         (-> parse-map :comp-op first)
        [left right] (:row-value-constructor parse-map)]
    (bounce (cond
              (#{\=} comp) (cond
                             (or (::obj left)
                                 (::obj right))   (let [{::keys [subj pred obj]} (merge left right)]
                                                    [[subj pred obj]])
                             (and (::pred left)
                                  (::pred right)) (let [v (template/build-var (::pred right))]
                                                    [[(::subj right) (::pred right) v]
                                                     [(::subj left) (::pred left) v]]))

              ; this condition handles the <, <=, >, >= operations
              (#{\> \<} comp) (cond
                                (and (::pred left)
                                     (::obj right)) (let [pred      (::pred left)
                                                          obj       (::obj right)
                                                          comp'     (->> parse-map :comp-op (apply str))
                                                          field-var (template/build-var pred)
                                                          filter-fn (template/build-fn-call [comp' field-var obj])]
                                                      [[template/collection-var pred field-var]
                                                       {:filter [filter-fn]}]))))))

(defmethod rule-parser :in-predicate
  [[_ & rst]]
  (let [parse-map      (parse-into-map rst)
        pred           (-> parse-map :row-value-constructor first ::pred)
        field-var      (template/build-var pred)
        selector       [template/collection-var pred field-var]
        not?           (contains? parse-map :not)
        filter-pred    (if not? "not=" "=")
        filter-junc    (if not? "and" "or")
        filter-clauses (->> parse-map
                            :in-predicate-value
                            (map ::obj)
                            (map (fn [v]
                                   (template/build-fn-call [filter-pred field-var v])))
                            (str/join " "))
        filter-func    (str "(" filter-junc " " filter-clauses ")")]
    (bounce [selector {:filter filter-func}])))

(defmethod rule-parser :null-predicate
  [[_ & rst]]
  (let [parsed    (parse-all rst)
        pred      (-> parsed first ::pred)
        field-var (template/build-var pred)]
    (if (some #{"NOT"} parsed)
      (bounce [[template/collection-var pred field-var]])
      (bounce [[template/collection-var "type" template/collection]
               {:optional [[template/collection-var pred field-var]]}
               {:filter [(template/build-fn-call ["nil?" field-var])]}]))))

(defmethod rule-parser :boolean-term
  [[_ & rst]]
  (->> rst
       (filter (fn [r]
                 (not= :and (rule-tag r))))
       parse-all
       bounce))

(defmethod rule-parser :search-condition
  [[_ & rst]]
  (let [parsed (parse-all rst)]
    (if (some #{"OR"} parsed)
      (let [[front back] (split-with (complement #{"OR"})
                                     parsed)]
        (bounce {:union [(vec front)
                         (->> back rest vec)]}))
      (bounce parsed))))

(defmethod rule-parser :table-name
  [[_ & rst]]
  (let [parsed-name (->> rst
                         parse-all
                         (apply str))]
    (bounce {::coll [parsed-name]})))

(defmethod rule-parser :from-clause
  [[_ _ & rst]]
  (->> rst
       parse-all
       (apply merge-parsed)
       bounce))

(defmethod rule-parser :where-clause
  [[_ _ & rst]]
  (bounce {::where (->> rst parse-all vec)}))

(defmethod rule-parser :group-by-clause
  [[_ _ & rst]]
  (->> rst
       parse-all
       (map (comp template/build-var ::pred))
       bounce))

(defmethod rule-parser :table-expression
  [[_ & rst]]
  (let [parse-map    (parse-into-map rst)
        from-clause  (->> parse-map :from-clause first)
        where-clause (or (some->> parse-map :where-clause first)
                         {::where [[template/collection-var  "type" template/collection]]})
        grouping     (->> parse-map :group-by-clause vec)
        from         (-> from-clause ::coll first)]
    (-> (merge-parsed from-clause where-clause)
        (assoc ::group grouping)
        (->> (template/fill-in-collection from))
        bounce)))

(defmethod rule-parser :query-specification
  [[_ _ & rst]]
  (let [parse-map               (parse-into-map rst)
        select-key              (-> parse-map
                                    :set-quantifier
                                    first
                                    (or :select))
        table-expr              (-> parse-map :table-expression first)
        select-list             (-> parse-map :select-list first)
        {::keys [coll select
                 where group]}  (merge-parsed table-expr select-list)
        from                    (first coll)]

    (cond-> {select-key (template/fill-in-collection from select)
             :where     (template/fill-in-collection from where)
             ::coll     coll}
      (seq group) (assoc :opts {:groupBy group})
      true        bounce)))

(defmethod rule-parser :join-condition
  [[_ _ & rst]]
  (bounce {::where (->> rst parse-all vec)}))

(defmethod rule-parser :outer-join-type
  [[_ t]]
  (bounce (case t
            "LEFT"  ::left
            "RIGHT" ::right
            "FULL"  ::full)))

(defmethod rule-parser :join-type
  [[_ t & _rst]]
  (bounce (case t
            "INNER" ::inner
            "UNION" ::union
            (parse-element t)))) ; `:outer-join-type` case

(defmethod rule-parser :named-columns-join
  [[_ _ & rst]]
  (->> rst parse-all bounce))

(defmethod rule-parser :qualified-join
  [[_ & rst]]
  (let [parse-map (->> rst
                       (filter rule?)
                       parse-into-map)
        spec      (->> parse-map
                       :join-specification
                       (apply merge-parsed))
        join-ref  (->> parse-map
                       :table-reference
                       (apply merge-parsed spec))
        join-type (-> parse-map
                      :join-type
                      first
                      (or ::inner))]
    (bounce (case join-type
              ::inner join-ref))))

(defmethod rule-parser :sort-specification
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        pred      (-> parse-map
                      :sort-key
                      first
                      template/field->predicate-template)]
    (if-let [order (some->> parse-map :ordering-specification first)]
      (bounce [[order pred]])
      (bounce pred))))

(defmethod rule-parser :order-by-clause
  [[_ _ & rst]]
  (->> rst parse-all bounce))

(defmethod rule-parser :limit-clause
  [[_ _ lim]]
  (-> lim parse-rule bounce))

(defmethod rule-parser :offset-clause
  [[_ _ ofst]]
  (-> ofst parse-rule bounce))

(defmethod rule-parser :direct-select-statement
  [[_ & rst]]
  (let [parse-map                 (parse-into-map rst)
        {::keys [coll] :as query} (->> parse-map :query-expression first)
        ordering                  (some->> parse-map
                                           :order-by-clause
                                           first
                                           (template/fill-in-collection (first coll)))
        limit                     (some->> parse-map :limit-clause first)
        offset                    (some->> parse-map :offset-clause first)]
    (cond-> query
      ordering (assoc-in [:opts :orderBy] ordering)
      limit    (assoc-in [:opts :limit] limit)
      offset   (assoc-in [:opts :offset] offset)
      true     bounce)))

(defn parse
  [q]
  (-> q
      str/trim
      sql
      parse-rule
      first
      (select-keys [:select :selectDistinct :selectOne :where :prefixes
                    :vars :opts])))
