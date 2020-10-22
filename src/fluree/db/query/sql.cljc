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


(defmethod rule-parser :double-quote
  [_]
  (bounce \"))


(defmethod rule-parser ::identifier    ; `:column-name`, `:table-name`
  [[_ & rst]]
  (->> rst
       parse-all
       (apply str)
       bounce))


(defmethod rule-parser :set-quantifier
  [[_ quantifier]]
  (let [k  (if (= quantifier "DISTINCT") :selectDistinct :select)]
    (bounce k)))


(defmethod rule-parser :select-list
  [[_ & rst]]
  (let [{:keys [asterisk select-sublist]} (parse-into-map rst)]
    (cond
      asterisk       (bounce {::select-vars    {template/collection-var ["*"]}})
      select-sublist (let [vars    (->> select-sublist
                                        (map template/build-var)
                                        vec)
                           triples (map (fn [fld var]
                                          (let [pred (template/field->predicate-template fld)]
                                            [template/collection-var pred var]))
                                        select-sublist vars)]
                       (bounce {::select-vars    vars
                                ::select-triples triples})))))


(defmethod rule-parser :between-predicate
  [[_ & rst]]
  (let [[field lower upper] (->> rst
                                (filter rule?)
                                parse-all)
        pred-tmpl           (template/field->predicate-template field)
        field-var           (template/build-var field)
        selector            [template/collection-var pred-tmpl field-var]
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
        [field v]  (:row-value-constructor parse-map)
        pred-tmpl  (template/field->predicate-template field)]
    (bounce (cond
              (#{\=} comp)    [[template/collection-var pred-tmpl v]]

              (#{\> \<} comp) (let [field-var (template/build-var field)
                                    filter-fn (template/build-fn-call [comp field-var v])]
                                [[template/collection-var pred-tmpl field-var]
                                 {:filter [filter-fn]}])))))


(defmethod rule-parser :in-predicate
  [[_ & rst]]
  (let [parse-map      (->> rst
                            (filter (fn [e]
                                      (not (contains? #{"IN" "NOT"} e))))
                            parse-into-map)
        field          (-> parse-map :row-value-constructor first)
        pred-tmpl      (template/field->predicate-template field)
        field-var      (template/build-var field)
        selector       [template/collection-var pred-tmpl field-var]
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
  [[_ f & rst]]
  (let [field     (-> f parse-element first)
        field-var (template/build-var field)]
    (if (some #{"NOT"} rst)
      (bounce [[template/collection-var (template/field->predicate-template field) field-var]])
      (bounce [[template/collection-var "rdf:type" template/collection]
               {:optional [[template/collection-var (template/field->predicate-template field) field-var]]}
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


(defmethod rule-parser :table-expression
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        from      (-> parse-map :from-clause first)
        where     (->> (:where-clause parse-map)
                       (template/fill-in-collection from)
                       vec)]
    (bounce {::coll  from
             ::where (if (seq where)
                       where
                       [[(template/build-var from) "rdf:type" from]])})))


(defmethod rule-parser :query-specification
  [[_ _ & rst]]
  (let [parse-map                 (parse-into-map rst)
        select-key                (-> parse-map
                                      :set-quantifier
                                      first
                                      (or :select))
        {::keys [coll where]}     (-> parse-map :table-expression first)
        {::keys [select-vars
                 select-triples]} (->> parse-map
                                       :select-list
                                       first
                                       (template/fill-in-collection coll))
        where-clause              (reduce conj  where select-triples)]
    (bounce {select-key select-vars
             :where     where-clause})))


(defn parse
  [q]
  (-> q
      sql
      parse-rule
      first
      (select-keys [:select :selectDistinct :selectOne :where :block :prefixes
                    :vars :opts])))
