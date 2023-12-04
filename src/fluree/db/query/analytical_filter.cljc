(ns fluree.db.query.analytical-filter
  (:require #?(:cljs [cljs.reader])
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log :include-macros true])
  #?(:clj (:import (java.time Instant))))

#?(:clj (set! *warn-on-reflection* true))

;; Change all filter functions to be: https://www.w3.org/TR/sparql11-query/#SparqlOps
;; https://docs.data.world/tutorials/sparql/list-of-sparql-filter-functions.html

(def filter-fns-with-ns
  {"bound"      'fluree.db.query.analytical-filter/bound
   "if"         'if
   "coalesce"   'fluree.db.query.analytical-filter/coalesce
   "!"          'fluree.db.query.analytical-filter/!
   "now"        'fluree.db.query.analytical-filter/now
   "not"        'not
   "&&"         '&&
   "and"        'and
   "||"         'fluree.db.query.analytical-filter/||
   "or"         'or
   ">"          '>
   "<"          '<
   ">="         '>=
   "<="         '<=
   "="          '=
   "+"          '+
   "-"          '-
   "*"          '*
   "/"          '/
   "nil?"       'nil?
   "not="       'not=
   "strStarts"  'fluree.db.query.analytical-filter/strStarts
   "strEnds"    'fluree.db.query.analytical-filter/strEnds
   "re-find"    're-find
   "re-pattern" 're-pattern})

(defn !
  [x]
  (not x))

(defmacro &&
  "Equivalent to and"
  ([] true)
  ([x] x)
  ([x & next]
   `(let [and# ~x]
      (if and# (and ~@next) and#))))

(defmacro ||
  "Equivalent to or"
  ([] nil)
  ([x] x)
  ([x & next]
   `(let [or# ~x]
      (if or# or# (or ~@next)))))

(defn bound
  [x]
  (not (nil? x)))

(defn now
  []
  #?(:clj (.toEpochMilli (Instant/now))))

(defn strStarts
  [s substr]
  (str/starts-with? s substr))

(defn strEnds
  [s substr]
  (str/ends-with? s substr))

;
;(defn bound
;  [{:keys [headers tuples] :as tuples-map} x]
;  (if-let [x-index (util/index-of headers x)]
;    (let [tuples (reduce (fn [acc tuple]
;                           (let [val? (boolean (nth tuple x-index))]
;                             (if val? (conj acc tuple) acc)))
;                         [] tuples)]
;      {:header headers :tuples tuples})
;    (throw (ex-info (str "Cannot evaluate 'bound' for unknown variable: " (str x))
;                    {:status 400 :error :db/invalid-query}))))

;; BOUND in SPARQL
;; FILTER ( bound(?date)  )
;; FILTER ( !bound(?date) )

(defn extract-filter-fn
  "Takes a filter fn as a string and a set/fn of allowed symbols that can be used within the fn.

  Returns two-tuple of parsed function and variables (symbols) used within the function as a set."
  [func symbols-allowed]
  (let [func'            (if (string? func)
                           (#?(:cljs cljs.reader/read-string
                               :clj  read-string) func)
                           func)
        symbols-allowed* (or symbols-allowed (fn [sym] (not= \? (first (name sym)))))
        fn-name          (first func')
        fn-w-ns          (or (and (set? fn-name)
                                  (every? #(or (number? %) (string? %)) fn-name)
                                  fn-name)
                             (filter-fns-with-ns (str fn-name))
                             (throw (ex-info (str "Invalid filter function: " fn-name
                                                  " used in function argument: " (pr-str func))
                                             {:status 400
                                              :error  :db/invalid-fn})))
        args             (rest func')
        [args* vars] (reduce (fn [[args* vars] arg]
                               (cond
                                 (list? arg)
                                 (let [[args' vars'] (extract-filter-fn arg symbols-allowed*)]
                                   [(conj args* args') (into vars vars')])

                                 (symbol? arg)
                                 (if-not (symbols-allowed* arg)
                                   (throw (ex-info (str "Invalid symbol: " arg
                                                        " used in function argument: " (pr-str func))
                                                   {:status 400 :error :db/invalid-fn}))
                                   [(conj args* arg)
                                    (conj vars arg)])

                                 (or (string? arg)
                                     (number? arg)
                                     (boolean? arg)
                                     (nil? arg)
                                     #?(:clj  (= (type arg) java.util.regex.Pattern)
                                        :cljs (regexp? arg)))
                                 [(conj args* arg) vars]

                                 :else
                                 (throw (ex-info (str "Illegal element " (pr-str arg) " of type: " (type arg)
                                                      " in function argument: " (pr-str func) ".")
                                                 {:status 400 :error :db/invalid-fn}))))
                             [[] #{}] args)
        fn*              (cons fn-w-ns args*)]
    [fn* vars]))

(defn SPARQL-filter-parser
  "Takes a SPARQL-formatted filer, and returns "
  [code]
  (throw (ex-info "Filter functions written in standard SPARQL format are not yet accepted."
                  {:status 400
                   :error  :db/invalid-query})))

(defn extract-combined-filter
  [filter-maps]
  (some->> filter-maps
           (map :function)
           (apply every-pred)))

(defmacro coalesce
  "Evaluates args in order. The result of the first arg not to return error gets returned."
  ([] (throw (ex-info "COALESCE evaluation failed on all forms." {:status 400 :error :db/invalid-query})))
  ([arg] `(let [res# (try ~arg (catch Exception e# nil))]
            (if (nil? res#)
              (throw (ex-info "Coalesce evaluation failed on all forms." {:status 400 :error :db/invalid-query})) res#)))
  ([arg & args]
   `(let [res# (try ~arg (catch Exception e# nil))]
      (if (nil? res#)
        (coalesce ~@args) res#))))

;; COALESCE
;;
;; BIND (
;  COALESCE(
;    IF(?grade >= 90, "A", 1/0),
;    IF(?grade >= 80, "B", 1/0),
;    IF(?grade >= 70, "C", 1/0),
;    IF(?grade >= 60, "D", 1/0),
;    "F"
;  ) AS ?result
;)

(defn filter-row
  [headers clause fil]
  ((eval `(fn [~headers] ~(#?(:clj read-string :cljs cljs.reader/read-string) fil))) clause))

(defn get-internal-filter-fn
  [var fun]
  (eval `(fn [~var]
           ~fun)))

(defn make-executable
  "Like the legacy get-internal-filter-fn, but allows for multiple vars."
  [params fun]
  (eval `(fn ~params
           ~fun)))

(comment

  (filter-row ['?nums '?fruit '?age] [1 "apple" 15] "(bound ?nums)"))
