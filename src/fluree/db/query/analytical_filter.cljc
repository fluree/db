(ns fluree.db.query.analytical-filter
  (:require [fluree.db.util.core :as util]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            #?(:cljs [cljs.reader]))
  #?(:clj (:import (java.time Instant))))


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

(defn valid-filter?
  ([func symbol-whitelist]
   (valid-filter? func symbol-whitelist nil))
  ([func symbol-whitelist var-atom]
   (let [func'   (if (string? func) (#?(:cljs cljs.reader/read-string
                                        :clj  read-string) func) func)
         fn-name (first func')
         fn-w-ns (or (and (set? fn-name) (every? #(or (number? %) (string? %)) fn-name) fn-name) (filter-fns-with-ns (str fn-name))
                     (throw (ex-info (str "Invalid filter function: " fn-name " used in function argument: " (pr-str func))
                                     {:status 400
                                      :error  :db/invalid-fn})))
         args    (rest func')
         args*   (mapv (fn [arg] (cond
                                   (list? arg) (first (valid-filter? arg symbol-whitelist var-atom))
                                   (string? arg) arg
                                   (number? arg) arg
                                   (symbol? arg) (do (or (symbol-whitelist arg) (throw (ex-info (str "Invalid symbol: " arg " used in function argument: " (pr-str func))
                                                                                                {:status 400
                                                                                                 :error  :db/invalid-fn})))
                                                     (if var-atom (swap! var-atom conj arg))
                                                     arg)
                                   (or (true? arg) (false? arg) (nil? arg)) arg
                                   (= (type arg) java.util.regex.Pattern) arg
                                   :else (throw
                                           (-> (str "Illegal element " (pr-str arg) " of type: " (type arg)
                                                    (= (type arg) "class java.util.regex.Pattern")
                                                    ") in function argument: " (pr-str func) ".")
                                               #?(:clj  (Exception.)
                                                  :cljs (js/Error.)))))) args)
         fn*     (cons fn-w-ns args*)]
     [fn* (if var-atom var-atom true)])))

(defn SPARQL-filter-parser
  "Takes a SPARQL-formatted filer, and returns "
  [code]
  (throw (ex-info "Filter functions written in standard SPARQL format are not yet accepted."
                  {:status 400
                   :error  :db/invalid-query})))

(defn get-filters
  ;; TODO - refactor now that optional filters can also be non-labelled filters inside optional map
  "optional? indicates we are looking for optional filters. "
  [filters optional?]
  (reduce (fn [acc fil]
            (cond (string? fil) (if optional? acc (conj acc fil))

                  (and (vector? fil) optional?
                       (= "optional" (str/lower-case (first fil)))) (conj acc (second fil))

                  (and (map? fil)
                       (= "clojure" (str/lower-case (:language fil)))
                       (if optional?
                         (:optional fil)
                         (not (true? (:optional fil))))
                       (contains? fil :code)) (conj acc (:code fil))

                  (and (map? fil)
                       (= "sparql" (str/lower-case (:language fil)))
                       (if optional?
                         (:optional fil)
                         (not (true? (:optional fil))))
                       (contains? fil :code)) (conj acc (SPARQL-filter-parser fil))

                  :else acc)) [] filters))

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

(comment

  (filter-row ['?nums '?fruit '?age] [1 "apple" 15] "(bound ?nums)")

  )
