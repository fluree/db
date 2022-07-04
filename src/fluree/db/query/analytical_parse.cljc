(ns fluree.db.query.analytical-parse
  (:require [clojure.string :as str]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.subject-crawl.legacy :refer [basic-to-analytical-transpiler]]
            [fluree.db.query.subject-crawl.reparse :refer [re-parse-as-simple-subj-crawl]]))

#?(:clj (set! *warn-on-reflection* true))

(declare parse-where)

(def read-str-fn #?(:clj read-string :cljs cljs.reader/read-string))

(defn safe-read-fn
  [code-str]
  (when-not (string? code-str)
    (throw (ex-info (code-str "Invalid function: " code-str)
                    {:status 400 :error :db/invalid-query})))
  (try*
    (let [code-str* (if (str/starts-with? code-str "#")
                      (subs code-str 1)
                      code-str)
          res       (read-str-fn code-str*)]
      (when-not (list? res)
        (throw (ex-info (code-str "Invalid function: " code-str)
                        {:status 400 :error :db/invalid-query})))
      res)
    (catch* e
            (log/warn "Invalid query function attempted: " code-str " with error message: " (ex-message e))
            (throw (ex-info (code-str "Invalid query function: " code-str)
                            {:status 400 :error :db/invalid-query})))))

(def built-in-aggregates
  (letfn [(sum [coll] (reduce + 0 coll))
          (avg [coll] (/ (sum coll) (count coll)))
          (median
            [coll]
            (let [terms (sort coll)
                  size  (count coll)
                  med   (bit-shift-right size 1)]
              (cond-> (nth terms med)
                      (even? size)
                      (-> (+ (nth terms (dec med)))
                          (/ 2)))))
          (variance
            [coll]
            (let [mean (avg coll)
                  sum  (sum (for [x coll
                                  :let [delta (- x mean)]]
                              (* delta delta)))]
              (/ sum (count coll))))
          (stddev
            [coll]
            (Math/sqrt (variance coll)))]
    {'abs            (fn [n] (max n (- n)))
     'avg            avg
     'ceil           (fn [n] (cond (= n (int n)) n
                                   (> n 0) (-> n int inc)
                                   (< n 0) (-> n int)))
     'count          count
     'count-distinct (fn [coll] (count (distinct coll)))
     'floor          (fn [n]
                       (cond (= n (int n)) n
                             (> n 0) (-> n int)
                             (< n 0) (-> n int dec)))
     'groupconcat    concat
     'median         median
     'min            (fn
                       ([coll] (reduce (fn [acc x]
                                         (if (neg? (compare x acc))
                                           x acc))
                                       (first coll) (next coll)))
                       ([n coll]
                        (vec
                          (reduce (fn [acc x]
                                    (cond
                                      (< (count acc) n)
                                      (sort compare (conj acc x))
                                      (neg? (compare x (last acc)))
                                      (sort compare (conj (butlast acc) x))
                                      :else acc))
                                  [] coll))))
     'max            (fn
                       ([coll] (reduce (fn [acc x]
                                         (if (pos? (compare x acc))
                                           x acc))
                                       (first coll) (next coll)))
                       ([n coll]
                        (vec
                          (reduce (fn [acc x]
                                    (cond
                                      (< (count acc) n)
                                      (sort compare (conj acc x))
                                      (pos? (compare x (first acc)))
                                      (sort compare (conj (next acc) x))
                                      :else acc))
                                  [] coll))))
     'rand           (fn
                       ([coll] (rand-nth coll))
                       ([n coll] (vec (repeatedly n #(rand-nth coll)))))
     'sample         (fn [n coll]
                       (vec (take n (shuffle coll))))
     'stddev         stddev
     'str            str
     'sum            sum
     'variance       variance}))

(defn aggregate?
  "Aggregate as positioned in a :select statement"
  [x]
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))

(defn query-fn?
  "Query function as positioned in a :where statement"
  [x]
  (and (string? x)
       (re-matches #"^#\(.+\)$" x)))

(defn q-var->symbol
  "Returns a query variable as a symbol, else nil if not a query variable."
  [x]
  (when (or (keyword? x)
            (and (string? x)
                 (= \? (first x)))
            (and (symbol? x)
                 (= \? (first (name x)))))
    (symbol x)))

(defn extract-aggregate-as
  "Returns as var symbol if 'as' function is used in an aggregate,
  e.g. (as (sum ?nums) ?sum).

  Checks that has 3 elements to the form, and the last element
  is a symbol that starts with a '?'. Else will throw."
  [as-fn-parsed]
  (when-not (and (= 3 (count as-fn-parsed))                 ;; e.g. (as (sum ?nums) ?sum) - will always have 3 elements
                 (symbol? (last as-fn-parsed)))
    (throw (ex-info (str "Invalid aggregate function using 'as': " (pr-str as-fn-parsed))
                    {:status 400 :error :db/invalid-query})))
  (last as-fn-parsed))


(defn parse-aggregate*
  [fn-parsed fn-str as]
  (let [list-count (count fn-parsed)
        [fun arg var] (cond (= 3 list-count)
                            [(first fn-parsed) (second fn-parsed) (last fn-parsed)]

                            (and (= 2 list-count) (= 'sample (first fn-parsed)))
                            (throw (ex-info (str "The sample aggregate function takes two arguments: n and a variable, provided: " fn-str)
                                            {:status 400 :error :db/invalid-query}))

                            (= 2 list-count)
                            [(first fn-parsed) nil (last fn-parsed)]

                            :else
                            (throw (ex-info (str "Invalid aggregate selection, provided: " fn-str)
                                            {:status 400 :error :db/invalid-query})))
        agg-fn     (if-let [agg-fn (built-in-aggregates fun)]
                     (if arg (fn [coll] (agg-fn arg coll)) agg-fn)
                     (throw (ex-info (str "Invalid aggregate selection function, provided: " fn-str)
                                     {:status 400 :error :db/invalid-query})))
        [agg-fn variable] (let [distinct? (and (coll? var) (= (first var) 'distinct))
                                variable  (if distinct? (second var) var)
                                agg-fn    (if distinct? (fn [coll] (-> coll distinct agg-fn))
                                                        agg-fn)]
                            [agg-fn variable])
        as'        (or as (symbol (str variable "-" fun)))]
    (when-not (and (symbol? variable)
                   (= \? (first (name variable))))
      (throw (ex-info (str "Variables used in aggregate functions must start with a '?'. Provided: " fn-str)
                      {:status 400 :error :db/invalid-query})))
    {:variable variable
     :as       as'
     :fn-str   fn-str
     :function agg-fn}))


(defn parse-aggregate
  "Parses an aggregate function string and returns map with keys:
  :variable - input variable symbol
  :as - return variable/binding name
  :fn-str - original function string, for use in reporting errors
  :function - executable function."
  [aggregate-fn-str]
  (let [list-agg  (safe-read-fn aggregate-fn-str)
        as?       (= 'as (first list-agg))
        func-list (if as?
                    (second list-agg)
                    list-agg)
        _         (when-not (coll? func-list)
                    (throw (ex-info (str "Invalid aggregate selection. As can only be used in conjunction with other functions. Provided: " aggregate-fn-str)
                                    {:status 400 :error :db/invalid-query})))
        as        (when as?
                    (extract-aggregate-as list-agg))]
    (parse-aggregate* func-list aggregate-fn-str as)))


(defn variable-in-where?
  "Returns true if provided variable exists as a variable
  somewhere within the where clause."
  [variable where]
  (some (fn [{:keys [s o optional bind union] :as _where-smt}]
          (or (= (:variable o) variable)
              (= (:variable s) variable)
              (cond
                optional (map #(variable-in-where? variable %) optional)
                bind (contains? (-> bind keys set) variable)
                union (or (variable-in-where? variable (first union))
                          (variable-in-where? variable (second union))))))
        where))

(defn parse-map
  [select-map]
  (let [[var selection] (first select-map)
        var-as-symbol (q-var->symbol var)]
    (when (or (not= 1 (count select-map))
              (nil? var-as-symbol))
      (throw (ex-info (str "Invalid select statement, maps must have only one key/val. Provided: " select-map)
                      {:status 400 :error :db/invalid-query})))
    {:variable  var-as-symbol
     :selection selection}))

(defn parse-having-code
  "Returns two-tuple of [params updated-code]
  where params are the function parameters and updated-code is a revised version of
  code-parsed where all functions within the code are mapped to actual executable functions."
  [code-parsed code-string]
  (let [[form-f & form-r] code-parsed
        form-f' (or (get built-in-aggregates form-f)
                    (get filter/filter-fns-with-ns (str form-f)))
        vars    (into #{} (filter symbol? form-r))]
    (loop [[form-next & form-rest] form-r
           vars* vars
           acc   []]
      (if form-next
        (let [[params item] (if (list? form-next)
                              (parse-having-code form-next code-string)
                              [nil (cond
                                     (symbol? form-next) (if-not (str/starts-with? (str form-next) "?")
                                                           (throw (ex-info (str "Invalid variable name '" form-next
                                                                                "' in having function: " code-string
                                                                                ". All vars must start with '?'.")
                                                                           {:status 400 :error :db/invalid-query}))
                                                           form-next)
                                     (string? form-next) form-next
                                     (boolean? form-next) form-next
                                     (number? form-next) form-next
                                     :else (throw (ex-info (str "Invalid having function: " code-string
                                                                ". Only scalar types allowed besides functions: " form-next ".")
                                                           {:status 400 :error :db/invalid-query})))])
              vars** (if params
                       (into vars* params)
                       vars*)]
          (recur form-rest vars** (conj acc item)))
        [(vec vars*) (cons form-f' acc)]))))


(defn parse-having
  [having]
  (when-not (aggregate? having)
    (throw (ex-info (str "Invalid 'having' statement aggregate: " having)
                    {:status 400 :error :db/invalid-query})))
  (let [code (safe-read-fn having)
        [params code*] (parse-having-code code having)]
    {:variable nil                                          ;; not used for 'having' fn execution
     :params   params
     :fn-str   (str "(fn " params " " code)
     :function (filter/make-executable params code*)}))


(defn parse-select
  [select-smt]
  (let [_ (or (every? #(or (string? %) (map? %)) select-smt)
              (throw (ex-info (str "Invalid select statement. Every selection must be a string or map. Provided: " select-smt) {:status 400 :error :db/invalid-query})))]
    (map (fn [select]
           (let [var-symbol (q-var->symbol select)]
             (cond var-symbol {:variable var-symbol}
                   (aggregate? select) (parse-aggregate select)
                   (map? select) (parse-map select)
                   :else (throw (ex-info (str "Invalid select in statement, provided: " select)
                                         {:status 400 :error :db/invalid-query})))))
         select-smt)))


(defn add-select-spec
  [{:keys [group-by order-by limit offset pretty-print] :as parsed-query}
   {:keys [selectOne select selectDistinct selectReduced opts orderBy groupBy having] :as _query-map'}]
  (let [select-smt    (or selectOne select selectDistinct selectReduced)
        selectOne?    (boolean selectOne)
        limit*        (if selectOne? 1 limit)
        inVector?     (vector? select-smt)
        select-smt    (if inVector? select-smt [select-smt])
        parsed-select (parse-select select-smt)
        aggregates    (filter #(contains? % :function) parsed-select)
        expandMap?    (some #(contains? % :selection) parsed-select)
        ;; legacy orderBy handling below
        orderBy*      (when-let [orderBy (or (:orderBy opts) orderBy)]
                        (if (or (string? orderBy)
                                (and (vector? orderBy)
                                     (#{"DESC" "ASC"} (first orderBy))))
                          (if (vector? orderBy) orderBy ["ASC" orderBy])
                          (throw (ex-info (str "Invalid orderBy clause, must be variable or two-tuple formatted ['ASC' or 'DESC', var]. Provided: " orderBy)
                                          {:status 400
                                           :error  :db/invalid-query}))))
        having*       (or having (:having opts))
        having-parsed (when having* (parse-having having*))]
    (assoc parsed-query :limit limit*
                        :selectOne? selectOne?
                        :select {:select           parsed-select
                                 :aggregates       (not-empty aggregates)
                                 :expandMaps?      expandMap?
                                 :orderBy          orderBy*
                                 :having           having-parsed
                                 :groupBy          (or (:groupBy opts) groupBy)
                                 :componentFollow? (:component opts)
                                 :limit            limit*
                                 :offset           (or offset 0)
                                 :selectOne?       selectOne?
                                 :selectDistinct?  (boolean (or selectDistinct selectReduced))
                                 :inVector?        inVector?
                                 :prettyPrint      pretty-print})))


(defn symbolize-var-keys
  "Turns keys of var maps into symbols"
  [q-map]
  (let [keys (map symbol (keys q-map))
        vals (vals q-map)]
    (zipmap keys vals)))

(defn add-filter-where
  "Adds a valid filter fn into the respective where statement(s).
  If the filter fn uses a var not found in a where statement, throws
  an exception"
  [where {:keys [variable] :as filter-fn-map}]
  (loop [[{:keys [o] :as where-smt} & r] where
         found-var? false
         where*     []]
    (if where-smt
      (let [match? (= variable (:variable o))]
        (if match?
          (recur r true (conj where* (assoc where-smt :o {:variable variable
                                                          :filter   filter-fn-map})))
          (recur r found-var? (conj where* where-smt))))
      (if found-var?
        where*
        (throw (ex-info (str "Filter function uses variable: " variable
                             " however that variable is not used in a where statement "
                             "or was already used in another filter function.")
                        {:status 400 :error :db/invalid-query}))))))

(defn get-vars
  "Returns a set of valid vars."
  [code]
  (reduce (fn [acc code-segment]
            (if (coll? code-segment)
              (into acc (get-vars code-segment))
              (if-let [allowed-var (q-var->symbol code-segment)]
                (conj acc allowed-var)
                acc)))
          #{} code))


(defn get-object-var
  "Returns the var that will represent flake/o when passed in a flake to execute filter fn.

  There can be multiple vars in the filter function which can utilize the original query's 'vars' map,
  however there should be exactly one var in the filter fn that isn't in that map - which should be the
  var that will receive flake/o."
  [params supplied-vars]
  (let [non-assigned-vars (remove #(contains? supplied-vars %) params)]
    (case (count non-assigned-vars)
      1 (first non-assigned-vars)
      0 (throw (ex-info (str "Query filter function has no variable assigned to it, all parameters "
                             "exist in the 'vars' map. Filter function params: " params ". "
                             "Vars assigned in query: " (vec (keys supplied-vars)) ".")
                        {:status 400
                         :error  :db/invalid-query}))
      ;; else
      (throw (ex-info (str "Vars used in a filter function are not included in the 'vars' map "
                           "or as a binding. Should only be missing one var, but missing: " (vec non-assigned-vars) ".")
                      {:status 400
                       :error  :db/invalid-query})))))


(defn parse-filter-fn
  "Evals, and returns query function."
  [filter-fn supplied-vars]
  (let [filter-code (safe-read-fn filter-fn)
        fn-vars     (or (not-empty (get-vars filter-code))
                        (throw (ex-info (str "Filter function must contain a valid variable. Provided: " key)
                                        {:status 400 :error :db/invalid-query})))
        params      (vec fn-vars)
        o-var       (get-object-var params supplied-vars)
        [fun _] (filter/extract-filter-fn filter-code fn-vars)]
    {:variable o-var
     :params   params
     :fn-str   (str "(fn " params " " fun)
     :function (filter/make-executable params fun)}))


(defn add-filter
  [{:keys [where] :as parsed-query} filter supplied-vars]
  (if-not (sequential? filter)
    (throw (ex-info (str "Filter clause must be a vector/array, provided: " filter)
                    {:status 400 :error :db/invalid-query}))
    (loop [[filter-fn & r] filter
           parsed-query* parsed-query]
      (if filter-fn
        (let [parsed (parse-filter-fn filter-fn supplied-vars)]
          (recur r (assoc parsed-query* :where (add-filter-where where parsed))))
        parsed-query*))))

(defn parse-binding
  "Parses binding map. Returns a two-tuple of binding maps
  including aggregates and scalars, for bindings that are
  aggregate functions vs static scalar values.

  Scalars end up getting hoisted, and essentially merged
  with query :vars - this way filter functions that get
  merged into their respective where statements will have
  the variable available to them."
  [bind-map]
  (reduce-kv (fn [[aggregates scalars] k v]
               (if (query-fn? v)
                 [(assoc aggregates k (parse-aggregate v))
                  scalars]
                 [aggregates (assoc scalars k v)]))
             [{} {}] bind-map))


(defn parse-where-map
  "When a where clause is a map, parses it into accumulating parsed where.
  Note parsed-where is not complete, but only has where clauses parsed that
  preceded the map clause.

  Returns a two-tuple of where-map type (i.e. :filter, :bind, :union .. etc.) and
  updated where clause. Updates where clause because a filter function may impact
  a prior where statement.
  "
  [db parsed-where map-clause supplied-vars]
  (when (not= 1 (count map-clause))
    (throw (ex-info (str "Where clause maps can only have one key/val, provided: " map-clause)
                    {:status 400 :error :db/invalid-query})))
  (let [[clause-type clause-val] (first map-clause)]
    (case clause-type
      :filter
      {:type   :filter                                      ;; will process all filters after where completed.
       :filter clause-val}

      :optional
      {:type  :optional
       :where (parse-where db {:where clause-val} supplied-vars)}

      :union
      (if (= 2 (count clause-val))
        {:type  :union
         :where (mapv #(parse-where db {:where %} supplied-vars) clause-val)}
        (throw (ex-info (str "Invalid where clause, 'union' clause must have exactly two solutions. "
                             "Each solution must be its own 'where' clause wrapped in a vector")
                        {:status 400 :error :db/invalid-query})))

      :bind
      (if (map? clause-val)
        (let [bind-map (symbolize-var-keys clause-val)
              ;; TODO - parse binding fns here and arrange into parsed bind-map
              [aggregates scalars] (parse-binding bind-map)]
          {:type       :bind
           :aggregates aggregates
           :scalars    scalars})
        (throw (ex-info (str "Invalid where clause, 'bind' must be a map with binding vars as keys "
                             "and binding scalars, or aggregates, as values.")
                        {:status 400 :error :db/invalid-query})))

      :minus                                                ;; negation - SPARQL 1.1, not yet supported
      (throw (ex-info (str "Invalid where clause, Fluree does not yet support the 'minus' operation.")
                      {:status 400 :error :db/invalid-query}))

      ;; else
      (throw (ex-info (str "Invalid where clause, unsupported where clause operation: " clause-type)
                      {:status 400 :error :db/invalid-query})))))


(defn parse-binding-tuple
  "Parses a two-tuple variable binding where clause"
  [binding-var binding-val]
  (let [var (q-var->symbol binding-var)
        fn? (query-fn? binding-val)]
    (if fn?
      {:type     :binding
       :variable var
       :fn       binding-val}
      {:type     :binding
       :variable var
       :value    binding-val})))

(defn- value-type-map
  "For both 's' and 'o', returns a map with respective value
  that indicates the value's type and if needed other information.


  'o' values have special handling before calling this function as they can
  also have 'tag' values or query-functions."
  [value]
  (cond
    (util/pred-ident? value)
    {:ident value}

    (q-var->symbol value)
    {:variable (q-var->symbol value)}

    (nil? value)
    nil

    :else
    {:value value}))

;; The docs say the default depth is 100
;; here: https://developers.flur.ee/docs/concepts/analytical-queries/inner-joins-in-fluree/#recursion
(def ^:const default-recursion-depth 100)

(defn recursion-predicate
  "A predicate that ends in a '+', or a '+' with some integer afterwards is a recursion
  predicate. e.g.: person/follows+3

  Returns a two-tuple of predicate followed by # of times to recur.

  If not a recursion predicate, returns nil."
  [predicate]
  (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" predicate)]
    [pred (if recur-n (util/str->int recur-n) default-recursion-depth)]))

(defn pred-id-strict
  "Returns predicate ID for a given predicate, else will throw with an invalid
  predicate error."
  [db predicate]
  (or (dbproto/-p-prop db :id predicate)
      (throw (ex-info (str "Invalid predicate: " predicate)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-where-tuple
  "Parses where clause tuples (not maps)"
  [supplied-vars db s p o]
  (let [fulltext? (str/starts-with? p "fullText:")
        rdf-type? (or (= "rdf:type" p)
                      (= "a" p))
        _id?      (= "_id" p)
        [recur-pred recur-n] (recursion-predicate p)
        s*        (value-type-map s)
        p*        (cond
                    fulltext? #?(:clj  (full-text/parse-domain p)
                                 :cljs (throw (ex-info "Full text queries not supported in JavaScript currently."
                                                       {:status 400 :error :db/invalid-query})))
                    rdf-type? :rdf/type
                    _id? :_id
                    recur-pred (cond->> recur-pred
                                        db (pred-id-strict db))
                    :else (cond->> p
                                   db (pred-id-strict db)))
        p-idx?    (when p* (dbproto/-p-prop db :idx? p*))   ;; is the predicate indexed?
        p-tag?    (when p* (= :tag (dbproto/-p-prop db :type p)))
        o*        (cond
                    p-tag?
                    {:tag o}

                    (query-fn? o)
                    (let [parsed-filter-map (parse-filter-fn o supplied-vars)]
                      {:variable (:variable parsed-filter-map)
                       :filter   parsed-filter-map})

                    rdf-type?
                    (if (= "_block" o)
                      (value-type-map "_tx")                ;; _block gets aliased to _tx
                      (value-type-map o))

                    :else
                    (value-type-map o))
        idx       (cond
                    fulltext?
                    :full-text

                    (or _id? rdf-type?)
                    :spot

                    (and s* (not (:variable s*)))
                    :spot

                    (and p-idx? (:value o*))
                    :post

                    p
                    (do (when (:value o*)
                          (log/info (str "Searching for a property value on unindexed predicate: " p
                                         ". Consider making property indexed for improved performance "
                                         "and lower fuel consumption.")))
                        :psot)

                    o
                    :opst

                    :else
                    (throw (ex-info (str "Unable to determine query type for where statement: "
                                         [s p o] ".")
                                    {:status 400 :error :db/invalid-query})))]
    {:type   (cond
               fulltext? :full-text
               rdf-type? :rdf/type
               _id? :_id
               :else :tuple)
     :idx    idx
     :s      s*
     :p      p*
     :o      o*
     :recur  recur-n                                        ;; will only show up if recursion specified.
     :p-tag? p-tag?
     :p-idx? p-idx?}))

(defn parse-remote-tuple
  "When a specific DB is used (not default) for a where statement.
  This is in the form of a 4-tuple where clause."
  [supplied-vars db s p o]
  {:db   db
   :type :remote-tuple
   :s    s
   :p    p
   :o    o}
  ;; TODO - once we support multiple sources, below will attempt to resolve predicates into pids
  ;; TODO - for now, we just let them all through.
  #_(-> (parse-where-tuple supplied-vars nil s p o)
        (assoc :db db
               :type :remote-tuple
               :s s :p p :o o)))

(defn parse-where
  "Parses where clause"
  [db {:keys [where] :as _query-map'} supplied-vars]
  (when-not (sequential? where)
    (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                    {:status 400 :error :db/invalid-query})))
  (loop [[where-smt & r] where
         filters        []
         hoisted-bind   {}                                  ;; bindings whose values are scalars are hoisted to the top level.
         supplied-vars* supplied-vars
         where*         []]
    (if where-smt
      (cond
        (map? where-smt)
        (let [{:keys [type] :as where-map} (parse-where-map db where* where-smt supplied-vars*)]
          (case type
            :bind
            (let [{:keys [aggregates scalars]} where-map]
              (recur r
                     filters
                     (merge hoisted-bind scalars)
                     (merge supplied-vars* aggregates scalars)
                     (if (not-empty aggregates)             ;; if all scalar bindings, no need to add extra where statement
                       (conj where* {:type :bind, :aggregates aggregates})
                       where*)))

            :filter
            (recur r (conj filters (:filter where-map)) hoisted-bind supplied-vars* where*)

            ;; else
            (recur r filters hoisted-bind supplied-vars* (conj where* where-map))))

        (sequential? where-smt)
        (let [tuple-count (count where-smt)
              where-smt*  (case tuple-count
                            3 (apply parse-where-tuple supplied-vars* db where-smt)
                            4 (if (= "$fdb" (first where-smt)) ;; $fdb refers to default/main db, parse as 3-tuple
                                (apply parse-where-tuple supplied-vars* db (rest where-smt))
                                (apply parse-remote-tuple supplied-vars* where-smt))
                            2 (apply parse-binding-tuple where-smt)
                            ;; else
                            (if (sequential? (first where-smt))
                              (throw (ex-info (str "Invalid where clause, it should contain 2, 3 or 4 tuples. "
                                                   "It appears you have an extra nested vector here: " where-smt)
                                              {:status 400 :error :db/invalid-query}))
                              (throw (ex-info (str "Invalid where clause, it should contain 2, 3 or 4 tuples but instead found: " where-smt)
                                              {:status 400 :error :db/invalid-query}))))]
          (recur r
                 filters
                 hoisted-bind
                 supplied-vars*
                 (conj where* where-smt*)))

        :else
        (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                        {:status 400 :error :db/invalid-query})))
      (let [where+filters (if (seq filters)
                            (reduce (fn [where' filter]
                                      (-> {:where where'}
                                          ;; add-filter allows calling on final parsed query, need to add/remove :where keys when inside :where parsing
                                          (add-filter filter supplied-vars*)
                                          :where))
                                    where* filters)
                            where*)]
        (if (not-empty hoisted-bind)
          (into [{:type :bind, :scalars hoisted-bind}] where+filters)
          where+filters)))))


(defn extract-vars
  "Returns query map without vars, to allow more effective caching of parsing."
  [query-map]
  (dissoc query-map :vars))

(defn parse-order-by
  [order-by-clause]
  (let [throw!   (fn [msg] (throw (ex-info (or msg
                                               (str "Invalid orderBy clause: " order-by-clause))
                                           {:status 400 :error :db/invalid-query})))
        [pred order] (cond (vector? order-by-clause)
                           [(second order-by-clause) (first order-by-clause)]

                           (string? order-by-clause)
                           [order-by-clause :asc]

                           :else
                           (throw! nil))
        order*   (case order
                   (:asc :desc) order
                   "ASC" :asc
                   "DESC" :desc
                   ;; else
                   (throw! nil))
        pred-var (q-var->symbol pred)]
    (if pred-var
      {:type     :variable
       :order    order*
       :variable pred-var}
      {:type      :predicate
       :order     order*
       :predicate pred})))


(defn add-order-by
  "Parses order-by and returns a map with more details
  Map contains keys:
   :type      - contains :variable or :predicate for type
   :order     - :asc or :desc
   :predicate - predicate name, if :predicate type
   :variable  - variable name, if :variable type"
  [{:keys [where] :as parsed-query} order-by]
  (let [{:keys [variable] :as parsed-order-by} (parse-order-by order-by)]
    (when (and variable (not (variable-in-where? variable where)))
      (throw (ex-info (str "Order by specifies a variable, " variable
                           " that is used in a where statement.")
                      {:status 400 :error :db/invalid-query})))
    (assoc parsed-query :order-by parsed-order-by)))


(defn add-group-by
  [{:keys [where] :as parsed-query} group-by]
  (let [group-by* (->> (if (sequential? group-by) group-by [group-by])
                       (mapv q-var->symbol))]
    (when-not (every? symbol? group-by*)
      (throw (ex-info (str "Group by must only include variable(s), provided: " group-by)
                      {:status 400 :error :db/invalid-query})))
    (when-not (every? #(variable-in-where? % where) group-by*)
      (throw (ex-info (str "Group by includes variable(s) not specified in the where clause: " group-by)
                      {:status 400 :error :db/invalid-query})))
    (assoc parsed-query :group-by group-by*)))


(defn get-limit
  "Extracts limit, if available, and verifies it is a positive integer.
  Uses Integer/max as default if not present."
  [{:keys [limit opts] :as _query-map'}]
  (let [limit* (or limit
                   (:limit opts)
                   util/max-integer)]
    (when-not (pos-int? limit*)
      (throw (ex-info (str "Invalid query limit specified: " limit*)
                      {:status 400 :error :db/invalid-query})))
    limit*))

(defn get-offset
  "Extracts offset, if specified, and verifies it is a positive integer.
  Uses 0 as default if not present."
  [{:keys [offset opts] :as _query-map'}]
  (let [offset* (or offset
                    (:offset opts)
                    0)]
    (when-not (>= offset* 0)
      (throw (ex-info (str "Invalid query offset specified: " offset*)
                      {:status 400 :error :db/invalid-query})))
    offset*))

(defn get-max-fuel
  "Extracts max-fuel from query if specified, or uses Integer/max a default."
  [{:keys [fuel max-fuel] :as query-map'}]
  (when max-fuel
    (log/info "Deprecated max-fuel used in query: " query-map'))
  (let [max-fuel (cond
                   (number? max-fuel)
                   max-fuel

                   (number? fuel)
                   fuel

                   :else util/max-integer)]
    (when-not (> max-fuel 0)
      (throw (ex-info (str "Invalid query fuel specified: " max-fuel)
                      {:status 400 :error :db/invalid-query})))
    max-fuel))


(defn expand-var-rel-binding
  "Expands a relational bindings vars definition where it was not supplied
  as a vector of maps, but instead a map with one or more vectors as vals.
  e.g.
  {?x [1 2 3 4]}
  {?x [1 2 3 4]
   ?y ['a' 'b' 'c' 'd']}
  {?x [1 2 3 4]
   ?y 'some-constant-var'}

  Returns a vector of full vars maps."
  [supplied-vars]
  (let [ks (keys supplied-vars)]
    (->> (vals supplied-vars)
         (mapv #(if (sequential? %)                         ;; scalar values get turned into infite lazy seqs of value
                  %
                  (repeat %)))
         (apply interleave)
         (partition (count ks))
         (mapv #(zipmap ks %)))))


(defn coerce-vars
  "Turns all var keys into symbols.
  If multiple vars (relational bindings) then will
  return a vector of vars maps."
  [supplied-vars]
  (when supplied-vars
    (if (sequential? supplied-vars)
      (mapv symbolize-var-keys supplied-vars)
      (let [supplied-vars* (symbolize-var-keys supplied-vars)
            rel-binding?   (some sequential? (vals supplied-vars*))]
        (if rel-binding?
          (expand-var-rel-binding supplied-vars*)
          supplied-vars*)))))

(defn basic-query?
  "Returns true if the query is the legacy 'basic query' type.
  e.g.:
   {select [*], from: '_user'}
   {select [*], from: ['_user/username' 'userid']}
   {select [*], from: '_user', where: '_user/username = userid'}"
  [{:keys [where] :as _query-map}]
  (not (sequential? where)))


;; TODO - only capture :select, :where, :limit - need to get others
(defn parse*
  [db {:keys [opts prettyPrint filter orderBy groupBy] :as query-map'} supplied-vars]
  (let [rel-binding?      (sequential? supplied-vars)
        supplied-var-keys (if rel-binding?
                            (-> supplied-vars first keys set)
                            (-> supplied-vars keys set))
        orderBy*          (or orderBy (:orderBy opts))
        groupBy*          (or groupBy (:groupBy opts))
        parsed            (cond-> {:strategy      :legacy
                                   :rel-binding?  rel-binding?
                                   :where         (parse-where db query-map' supplied-var-keys)
                                   :opts          opts
                                   :limit         (get-limit query-map') ;; limit can be a primary key, or within :opts
                                   :offset        (get-offset query-map') ;; offset can be a primary key, or within :opts
                                   :fuel          (get-max-fuel query-map')
                                   :supplied-vars supplied-var-keys
                                   :pretty-print  (if (boolean? prettyPrint) ;; prettyPrint can be a primary key, or within :opts
                                                    prettyPrint
                                                    (:prettyPrint opts))}
                                  filter (add-filter filter supplied-var-keys) ;; note, filter maps can/should also be inside :where clause
                                  orderBy* (add-order-by orderBy*)
                                  groupBy* (add-group-by groupBy*)
                                  true (add-select-spec query-map'))]
    (or (re-parse-as-simple-subj-crawl parsed)
        parsed)))

(defn parse
  [db query-map]
  (let [query-map*   (if (basic-query? query-map)
                       (basic-to-analytical-transpiler query-map)
                       query-map)
        {:keys [vars opts]} query-map*
        vars*        (coerce-vars vars)
        opts*        (when opts (util/keywordize-keys opts))
        parsed-query (parse* db
                             (-> query-map*
                                 (assoc :opts opts*)
                                 (dissoc :vars))
                             vars*)]
    (assoc parsed-query :vars vars*)))
