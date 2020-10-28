(ns fluree.db.query.sql
  (:require [fluree.db.query.sql.template :as template]
            [clojure.string :as str]
            #?(:clj [clojure.java.io :as io])
            #?(:clj [instaparse.core :as insta]
               :cljs [instaparse.core :as insta :refer-macros [defparser]])))

(def sql
  "Parses SQL query strings into hiccup-formatted BNF rule trees"
  (-> "sql-92.bnf"
      io/resource
      (insta/parser :input-format :ebnf
                    :string-ci    true)))


(defn rule-tag
  [r]
  (first r))

(defn rule?
  [elt]
  (and (sequential? elt)
       (keyword? (rule-tag elt))))

(def rules
  "Hierarchy of SQL BNF rule name keywords for parsing equivalence"
  (-> (make-hierarchy)
      (derive :column-name ::identifier)
      (derive :table-name ::identifier)))

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


(defmethod rule-parser :unsigned-integer
  [[_ & rst]]
  (->> rst
       parse-all
       (apply str)
       Integer/parseInt
       bounce))


(defmethod rule-parser :character-string-literal
  [[_ & rst]]
  (->> rst
       parse-all
       (apply str)
       bounce))


(defmethod rule-parser :double-quote
  [_]
  (bounce \"))


(defmethod rule-parser ::identifier    ; `:column-name`, `:table-name`
  [[_ & rst]]
  (->> rst
       parse-all
       (apply str)
       bounce))


(defmethod rule-parser :column-reference
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        column    (-> parse-map :column-name first)]
    (bounce (if-let [qualifier (-> parse-map :qualifier first)]
              (template/build-predicate qualifier column)
              (template/field->predicate-template column)))))


(defmethod rule-parser :set-quantifier
  [[_ quantifier]]
  (let [k  (if (= quantifier "DISTINCT") :selectDistinct :select)]
    (bounce k)))


(defmethod rule-parser :asterisk
  [_]
  (bounce {template/collection-var ["*"]}))


(defmethod rule-parser :select-list-element
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        column    (->> parse-map :column-name first)]
    (cond
      column (let [pred   (template/field->predicate-template column)
                   var    (template/build-var pred)
                   triple [template/collection-var pred var]]
               (bounce {::select-vars    [var]
                        ::select-triples [triple]})))))


(defmethod rule-parser :select-list
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        asterisk  (some->> parse-map :asterisk first)
        sublist   (some->> parse-map
                           :select-list-element
                           (apply merge-with into))]
    (-> sublist
        (or {::select-vars asterisk})
        bounce)))


(defmethod rule-parser :between-predicate
  [[_ & rst]]
  (let [[pred lower upper] (->> rst
                                (filter rule?)
                                parse-all)
        field-var           (template/build-var pred)
        selector            [template/collection-var pred field-var]
        refinement          (if (some #{"NOT"} rst)
                              {:union [{:filter [(template/build-fn-call ["<" field-var lower])]}
                                       {:filter [(template/build-fn-call [">" field-var upper])]}]}
                              {:filter [(template/build-fn-call [">=" field-var lower])
                                        (template/build-fn-call ["<=" field-var upper])]})]
    (bounce [selector refinement])))


(defmethod rule-parser :comparison-predicate
  [[_ & rst]]
  (let [parse-map  (parse-into-map rst)
        comp       (-> parse-map :comp-op first)
        [pred v]  (:row-value-constructor parse-map)]
    (bounce (cond
              (#{\=} comp)    [[template/collection-var pred v]]

              (#{\> \<} comp) (let [field-var (template/build-var pred)
                                    filter-fn (template/build-fn-call [comp field-var v])]
                                [[template/collection-var pred field-var]
                                 {:filter [filter-fn]}])))))


(defmethod rule-parser :in-predicate
  [[_ & rst]]
  (let [parse-map      (->> rst
                            (filter (fn [e]
                                      (not (contains? #{"IN" "NOT"} e))))
                            parse-into-map)
        pred           (-> parse-map :row-value-constructor first)
        field-var      (template/build-var pred)
        selector       [template/collection-var pred field-var]
        not?           (some #{"NOT"} rst)
        filter-pred    (if not? "not=" "=")
        filter-junc    (if not? "and" "or")
        filter-clauses (->> parse-map
                            :in-predicate-value
                            (map (fn [v]
                                   (template/build-fn-call [filter-pred field-var v])))
                            (str/join " "))
        filter-func    (str "(" filter-junc " " filter-clauses ")")]
    (bounce [selector {:filter filter-func}])))


(defmethod rule-parser :null-predicate
  [[_ p & rst]]
  (let [pred      (-> p parse-element first)
        field-var (template/build-var pred)]
    (if (some #{"NOT"} rst)
      (bounce [[template/collection-var pred field-var]])
      (bounce [[template/collection-var "rdf:type" template/collection]
               {:optional [[template/collection-var pred field-var]]}
               {:filter [(template/build-fn-call ["nil?" field-var])]}]))))


(defmethod rule-parser :boolean-term
  [[_ & rst]]
  (->> rst
       (filter (partial not= "AND"))
       parse-all
       bounce))


(defmethod rule-parser :search-condition
  [[_ & rst]]
  (if (some #{"OR"} rst)
    (let [[front _ back] rst]
      (bounce {:union [(-> front parse-element vec)
                       (-> back parse-element vec)]}))
    (->> rst parse-all bounce)))


(defmethod rule-parser :table-reference
  [[_ & rst]]
  (let [parse-map     (parse-into-map rst)
        table-name    (some->> parse-map :table-name first)
        joined-table  (some->> parse-map :joined-table first)
        derived-table (some->> parse-map :derived-table first)]
    (bounce (cond
              table-name   {::coll [table-name]}
              joined-table joined-table))))


(defmethod rule-parser :from-clause
  [[_ _ & rst]]
  (->> rst
       parse-all
       (apply merge-with into)
       bounce))


(defmethod rule-parser :group-by-clause
  [[_ _ & rst]]
  (->> rst
       parse-all
       (map template/build-var)
       bounce))


(defmethod rule-parser :table-expression
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        from      (-> parse-map :from-clause first ::coll first)
        where     (->> (:where-clause parse-map)
                       (template/fill-in-collection from)
                       vec)
        grouping  (some->> parse-map
                           :group-by-clause
                           (template/fill-in-collection from)
                           vec)]
    (bounce {::coll  from
             ::where (if (seq where)
                       where
                       [[(template/build-var from) "rdf:type" from]])
             ::group grouping})))


(defmethod rule-parser :query-specification
  [[_ _ & rst]]
  (let [parse-map                   (parse-into-map rst)
        select-key                  (-> parse-map
                                        :set-quantifier
                                        first
                                        (or :select))
        {::keys [coll where group]} (-> parse-map :table-expression first)
        {::keys [select-vars
                 select-triples]}   (->> parse-map
                                         :select-list
                                         first
                                         (template/fill-in-collection coll))
        where-clause                (reduce conj  where select-triples)]

    (cond-> {select-key select-vars
             :where     where-clause
             ::coll     coll}
      (seq group) (assoc :opts {:groupBy group})
      :finally    bounce)))


(defmethod rule-parser :ordering-specification
  [[_ order]]
  (-> order str/upper-case bounce))


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


(defmethod rule-parser :direct-select-statement
  [[_ & rst]]
  (let [parse-map                 (parse-into-map rst)
        {::keys [coll] :as query} (->> parse-map :query-expression first)
        ordering                  (some->> parse-map
                                           :order-by-clause
                                           first
                                           (template/fill-in-collection coll))]
    (cond-> query
      ordering (update :opts (fn [opts]
                               (-> opts
                                   (or {})
                                   (assoc :orderBy ordering))))
      :finally bounce)))


(defn parse
  [q]
  (-> q
      sql
      parse-rule
      first
      (select-keys [:select :selectDistinct :selectOne :where :block :prefixes
                    :vars :opts])))
