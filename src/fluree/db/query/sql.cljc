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
                    :start        :query-specification)))


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
       (reduce-kv (fn [rules t lst]
                    (assoc rules t (parse-all lst)))
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


(defmethod rule-parser :asterisk
  [_]
  (bounce \*))


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


(defmethod rule-parser :comparison-predicate
  [[_ & rst]]
  (let [parse-map  (parse-into-map rst)
        comp       (-> parse-map :comp-op first)
        [field v]  (:row-value-constructor parse-map)]
    (bounce (case comp
              \= (let [pred-tmpl (template/field->predicate-template field)]
                   [[template/subject-var pred-tmpl v]])))))


(defmethod rule-parser :table-expression
  [[_ & rst]]
  (let [parse-map (parse-into-map rst)
        from      (-> parse-map :from-clause first)
        [s r obj] (-> parse-map :where-clause first)
        subj      (template/fill-in-subject s from)
        rel       (template/fill-in-collection r from)]
    (bounce {::coll  from
             ::where [[subj rel obj]]})))


(defmethod rule-parser :query-specification
  [[_ spec & rst]]
  (if (= spec "SELECT")
    (let [parse-map             (parse-into-map rst)
          {::keys [coll where]} (-> parse-map :table-expression first)
          select-key            (-> parse-map
                                    :set-quantifier
                                    first
                                    (or :select))
          select-list           (:select-list parse-map)
          select-val            (->> select-list
                                     (map (partial str "?"))
                                     vec)
          subj                  (-> template/subject-var
                                    (template/fill-in-subject coll))
          select-threes         (when-not (= select-list [:*])
                                  (map (fn [fld var]
                                         (let [pred (-> fld
                                                        template/field->predicate-template
                                                        (template/fill-in-collection coll))]
                                           [subj pred var]))
                                       select-list select-val))
          where-clause          (reduce conj where select-threes)]
      (bounce {select-key select-val
               :where     where-clause}))
    (throw (ex-info "Non-select SQL queries are not currently supported by the transpiler"
                    {:status 400
                     :error  :db/invalid-query
                     :provided-query spec}))))


(defn parse
  [q]
  (->> q sql parse-rule first))
