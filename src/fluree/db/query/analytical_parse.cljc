(ns fluree.db.query.analytical-parse
  (:require [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.query.subject-crawl.legacy :refer [basic-to-analytical-transpiler]]
            [fluree.db.query.subject-crawl.reparse :refer [re-parse-as-simple-subj-crawl]]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.json-ld.select :as json-ld-select]
            [fluree.db.flake :as flake]
            [fluree.db.query.parse.aggregate :refer [parse-aggregate safe-read-fn built-in-aggregates]]
            [clojure.set :as set]
            [fluree.db.query.union :as union]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(declare parse-where parse-where-tuple where-meta-reverse)

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
  (when (or (and (string? x)
                 (= \? (first x)))
            (and (or (symbol? x) (keyword? x))
                 (= \? (first (name x)))))
    (symbol x)))


(defn variable-in-where?
  "Returns true if provided variable exists as a variable
  somewhere within the where clause."
  [variable where]
  (log/debug "variable-in-where? variable:" variable "where:" where)
  (some (fn [{:keys [s o type] :as where-smt}]
          (or (= (:variable o) variable)
              (= (:variable s) variable)
              (case type
                :optional (variable-in-where? variable (:where where-smt))
                :binding (= (:variable where-smt) variable)
                :union (or (variable-in-where? variable (first (:where where-smt)))
                           (variable-in-where? variable (second (:where where-smt))))
                nil)))
        where))


;; TODO - need to add back in 'having' to new json-ld parsing - retain old code here temporarily for reference
;(defn parse-having-code
;  "Returns two-tuple of [params updated-code]
;  where params are the function parameters and updated-code is a revised version of
;  code-parsed where all functions within the code are mapped to actual executable functions."
;  [code-parsed code-string]
;  (let [[form-f & form-r] code-parsed
;        form-f' (or (get built-in-aggregates form-f)
;                    (get filter/filter-fns-with-ns (str form-f)))
;        vars    (into #{} (filter symbol? form-r))]
;    (loop [[form-next & form-rest] form-r
;           vars* vars
;           acc   []]
;      (if form-next
;        (let [[params item] (if (list? form-next)
;                              (parse-having-code form-next code-string)
;                              [nil (cond
;                                     (symbol? form-next) (if-not (str/starts-with? (str form-next) "?")
;                                                           (throw (ex-info (str "Invalid variable name '" form-next
;                                                                                "' in having function: " code-string
;                                                                                ". All vars must start with '?'.")
;                                                                           {:status 400 :error :db/invalid-query}))
;                                                           form-next)
;                                     (string? form-next) form-next
;                                     (boolean? form-next) form-next
;                                     (number? form-next) form-next
;                                     :else (throw (ex-info (str "Invalid having function: " code-string
;                                                                ". Only scalar types allowed besides functions: " form-next ".")
;                                                           {:status 400 :error :db/invalid-query})))])
;              vars** (if params
;                       (into vars* params)
;                       vars*)]
;          (recur form-rest vars** (conj acc item)))
;        [(vec vars*) (cons form-f' acc)]))))
;
;
;(defn parse-having
;  [having]
;  (when-not (aggregate? having)
;    (throw (ex-info (str "Invalid 'having' statement aggregate: " having)
;                    {:status 400 :error :db/invalid-query})))
;  (let [code (safe-read-fn having)
;        [params code*] (parse-having-code code having)]
;    {:variable nil                                          ;; not used for 'having' fn execution
;     :params   params
;     :fn-str   (str "(fn " params " " code)
;     :function (filter/make-executable params code*)}))


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
  (log/debug "add-filter-where where:" where "- filter-fn-map:" filter-fn-map)
  (loop [[{:keys [o type] :as where-smt} & r] where
         found-var? false
         where*     []]
    (log/debug "add-filter-where loop where-smt:" where-smt "- found-var?:" found-var?
               "- where*:" where*)
    (if where-smt
      (cond
        (= :optional type)
        (do
          (log/debug "add-filter-where loop handling optional")
          (recur (concat (:where where-smt) r) found-var? (conj where* where-smt)))

        (= variable (:variable o))
        (do
          (log/debug "add-filter-where loop found match")
          (recur r true (conj where* (assoc where-smt :o {:variable variable
                                                          :filter   filter-fn-map}))))
        :else
        (do
          (log/debug "add-filter-where loop default case")
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
  [params all-vars]
  (let [non-assigned-vars (remove #(contains? all-vars %) params)]
    (case (count non-assigned-vars)
      1 (first non-assigned-vars)
      0 (throw (ex-info (str "Query filter function has no variable assigned to it, all parameters "
                             "exist in the 'vars' map. Filter function params: " params ". "
                             "Vars assigned in query: " all-vars ".")
                        {:status 400
                         :error  :db/invalid-query}))
      ;; else
      (throw (ex-info (str "Vars used in a filter function are not included in the 'vars' map "
                           "or as a binding. Should only be missing one var, but missing: " (vec non-assigned-vars) ".")
                      {:status 400
                       :error  :db/invalid-query})))))


(defn parse-filter-fn
  "Evals, and returns query function."
  [filter-fn all-vars]
  (let [filter-code (safe-read-fn filter-fn)
        fn-vars     (or (not-empty (get-vars filter-code))
                        (throw (ex-info (str "Filter function must contain a valid variable. Provided: " key)
                                        {:status 400 :error :db/invalid-query})))
        params      (vec fn-vars)
        o-var       (get-object-var params all-vars)
        [fun _] (filter/extract-filter-fn filter-code fn-vars)]
    {:variable o-var
     :params   params
     :fn-str   (str "(fn " params " " fun)
     :function (filter/make-executable params fun)}))


(defn add-filter
  [{:keys [where] :as parsed-query} filter all-vars]
  (if-not (sequential? filter)
    (throw (ex-info (str "Filter clause must be a vector/array, provided: " filter)
                    {:status 400 :error :db/invalid-query}))
    (loop [[filter-fn & r] filter
           parsed-query* parsed-query]
      (if filter-fn
        (let [parsed (parse-filter-fn filter-fn all-vars)]
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
  [db parsed-where map-clause supplied-vars context]
  (when (not= 1 (count map-clause))
    (throw (ex-info (str "Where clause maps can only have one key/val, provided: " map-clause)
                    {:status 400 :error :db/invalid-query})))
  (let [[clause-type clause-val] (first map-clause)]
    (case clause-type
      :filter
      {:type   :filter                                      ;; will process all filters after where completed.
       :filter clause-val}

      :optional
      (if (vector? (first clause-val))
        (if (= 1 (count clause-val))
          ;; single clause, just wrapped in a vector unnecessarily - still support but unwrap
          (-> (apply parse-where-tuple supplied-vars supplied-vars context db (first clause-val))
              (assoc :optional? true))
          ;; multiple optional statements, treat like a sub-query
          {:type  :optional
           :where (parse-where db {:where clause-val} supplied-vars context)})
        ;; single optional statement, treat like a 3-tuple
        (-> (apply parse-where-tuple supplied-vars supplied-vars context db clause-val)
            (assoc :optional? true)))

      :union
      (if (= 2 (count clause-val))
        {:type  :union
         :where (mapv #(parse-where db {:where %} supplied-vars context) clause-val)}
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
  [context value subject?]
  (cond
    (util/pred-ident? value)
    {:ident value}

    (q-var->symbol value)
    {:variable (q-var->symbol value)}

    (nil? value)
    nil

    context
    {:value (if (int? value)
              value
              (json-ld/expand-iri value context))}

    :else
    (if (and subject? (not (int? value)))
      (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                           "Provided: " value ".")
                      {:status 400 :error :db/invalid-query}))
      {:value value})))

;; The docs say the default depth is 100
;; here: https://developers.flur.ee/docs/concepts/analytical-queries/inner-joins-in-fluree/#recursion
(def ^:const default-recursion-depth 100)

(defn recursion-predicate
  "A predicate that ends in a '+', or a '+' with some integer afterwards is a recursion
  predicate. e.g.: person/follows+3

  Returns a two-tuple of predicate followed by # of times to recur.

  If not a recursion predicate, returns nil."
  [predicate context]
  (cond
    (string? predicate)
    (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" predicate)]
      [(json-ld/expand pred context) (if recur-n (util/str->int recur-n) default-recursion-depth)])

    (keyword? predicate)
    (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" (name predicate))]
      [(json-ld/expand (keyword (namespace predicate) pred) context)
       (if recur-n (util/str->int recur-n) default-recursion-depth)])))

(defn pred-id-strict
  "Returns predicate ID for a given predicate, else will throw with an invalid
  predicate error."
  [db predicate]
  (or (dbproto/-p-prop db :id predicate)
      (throw (ex-info (str "Invalid predicate: " predicate)
                      {:status 400 :error :db/invalid-query}))))

(def rdf:type? #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "a" :a "rdf:type" :rdf/type "@type"})


(defn parse-where-tuple
  "Parses where clause tuples (not maps)"
  [supplied-vars _ context db s p o]
  (let [s*        (value-type-map context s true)
        p*        (cond
                    (rdf:type? p)
                    {:value const/$rdf:type}

                    (= "@id" p)
                    {:value const/$iri}

                    (q-var->symbol p)
                    {:variable (q-var->symbol p)}

                    (recursion-predicate p context)
                    (let [[p-iri recur-n] (recursion-predicate p context)]
                      {:value (pred-id-strict db p-iri)
                       :recur (or recur-n util/max-integer)}) ;; default recursion depth

                    (and (string? p)
                         (str/starts-with? p "fullText:"))
                    {:full-text (->> (json-ld/expand-iri (subs p 9) context)
                                     (pred-id-strict db))}

                    :else
                    {:value (->> (json-ld/expand-iri p context)
                                 (pred-id-strict db))})
        rdf-type? (= const/$rdf:type (:value p*))
        iri?      (= const/$iri (:value p*))
        o*        (cond
                    (query-fn? o)
                    (let [parsed-filter-map (parse-filter-fn o supplied-vars)]
                      {:variable (:variable parsed-filter-map)
                       :filter   parsed-filter-map})

                    rdf-type?
                    (let [id (->> (json-ld/expand-iri o context)
                                  (dbproto/-p-prop db :id))]
                      (or (value-type-map context id false)
                          (throw (ex-info (str "Undefined RDF type specified: " (json-ld/expand-iri o context))
                                          {:status 400 :error :db/invalid-query}))))

                    :else
                    (value-type-map context o false))]
    {:type (cond
             rdf-type? :class
             iri? :iri
             (:full-text p) :full-text
             :else :tuple)
     :s    s*
     :p    p*
     :o    o*}))

(defn parse-remote-tuple
  "When a specific DB is used (not default) for a where statement.
  This is in the form of a 4-tuple where clause."
  [supplied-vars all-vars context db s p o]
  {:db   db
   :type :remote-tuple
   :s    s
   :p    p
   :o    o}
  ;; TODO - once we support multiple sources, below will attempt to resolve predicates into pids
  ;; TODO - for now, we just let them all through.
  #_(-> (parse-where-tuple supplied-vars all-vars context nil s p o)
        (assoc :db db
               :type :remote-tuple
               :s s :p p :o o)))

(defn parse-where
  "Parses where clause"
  [db {:keys [where] :as _query-map} supplied-vars context]
  (when-not (sequential? where)
    (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                    {:status 400 :error :db/invalid-query})))
  (loop [[where-smt & r] where
         filters      []
         hoisted-bind {}                                    ;; bindings whose values are scalars are hoisted to the top level.
         all-vars     supplied-vars
         where*       []]
    (if where-smt
      (cond
        (map? where-smt)
        (let [{:keys [type] :as where-map} (parse-where-map db where* where-smt all-vars context)]
          (case type
            :bind
            (let [{:keys [aggregates scalars]} where-map]
              (recur r
                     filters
                     (merge hoisted-bind scalars)
                     (set (concat all-vars (keys aggregates) (keys scalars)))
                     (if (not-empty aggregates)             ;; if all scalar bindings, no need to add extra where statement
                       (conj where* {:type :bind, :aggregates aggregates})
                       where*)))

            :filter
            (recur r (conj filters (:filter where-map)) hoisted-bind all-vars where*)

            ;; else
            (recur r filters hoisted-bind all-vars (conj where* where-map))))

        (sequential? where-smt)
        (let [tuple-count (count where-smt)
              where-smt*  (case tuple-count
                            3 (apply parse-where-tuple supplied-vars all-vars context db where-smt)
                            4 (if (= "$fdb" (first where-smt)) ;; $fdb refers to default/main db, parse as 3-tuple
                                (apply parse-where-tuple supplied-vars all-vars context db (rest where-smt))
                                (apply parse-remote-tuple supplied-vars all-vars context where-smt))
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
                 all-vars
                 (conj where* where-smt*)))

        :else
        (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                        {:status 400 :error :db/invalid-query})))
      (let [where+filters (if (seq filters)
                            (reduce (fn [where' filter]
                                      (-> {:where where'}
                                          ;; add-filter allows calling on final parsed query, need to add/remove :where keys when inside :where parsing
                                          (add-filter filter all-vars)
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
  (let [throw!           (fn [msg] (throw (ex-info (or msg
                                                       (str "Invalid orderBy clause: " order-by-clause))
                                                   {:status 400 :error :db/invalid-query})))
        order-by-clauses (if (vector? order-by-clause)
                           order-by-clause
                           [order-by-clause])
        clause-maps      (mapv (fn [order-by-clause]
                                 (let [[pred order] (cond
                                                      (string? order-by-clause)
                                                      [order-by-clause :asc]

                                                      (list? order-by-clause)
                                                      (if (and (= 2 (count order-by-clause))
                                                               (#{'desc "desc"} (first order-by-clause)))
                                                        [(second order-by-clause) :desc]
                                                        (throw "Invalid orderBy, if trying to order in descending order try: (desc ?myvar)"))

                                                      (symbol? order-by-clause)
                                                      [order-by-clause :asc]

                                                      :else
                                                      (throw! nil))
                                       variable (q-var->symbol pred)]
                                   (if variable
                                     {:type     :variable
                                      :order    order
                                      :variable variable}
                                     {:type      :predicate
                                      :order     order
                                      :predicate pred})))
                               order-by-clauses)]
    {:input  order-by-clause
     :parsed clause-maps}))


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
                           " that is not used in a where statement.")
                      {:status 400 :error :db/invalid-query})))
    (assoc parsed-query :order-by parsed-order-by)))


(defn add-group-by
  "Adds group-by clause.
  If no order-by is present, uses group-by statement to create corresponding
  order-by. If order-by is present, uses it for ordering - but if inconsistent
  with group-by the desired results may not be achieved.
  *note - we could implement more logic to check if group-by is inconsistent with
  order-by"
  [{:keys [where order-by] :as parsed-query} group-by]
  (let [group-symbols (->> (if (sequential? group-by) group-by [group-by])
                           (mapv q-var->symbol))]
    (when-not (every? symbol? group-symbols)
      (throw (ex-info (str "Group by must only include variable(s), provided: " group-by)
                      {:status 400 :error :db/invalid-query})))
    (when-not (every? #(variable-in-where? % where) group-symbols)
      (throw (ex-info (str "Group by includes variable(s) not specified in the where clause: " group-by)
                      {:status 400 :error :db/invalid-query})))
    (cond-> (assoc parsed-query :group-by {:input  group-by
                                           :parsed (mapv (fn [sym] {:variable sym}) group-symbols)})
            (not order-by) (add-order-by group-symbols))))


(defn get-limit
  "Extracts limit, if available, and verifies it is a positive integer.
  Uses Integer/max as default if not present."
  [{:keys [limit opts] :as _query-map}]
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
  [{:keys [offset opts] :as _query-map}]
  (let [offset* (or offset
                    (:offset opts)
                    0)]
    (when-not (and (int? offset*) (>= offset* 0))
      (throw (ex-info (str "Invalid query offset specified: " offset*)
                      {:status 400 :error :db/invalid-query})))
    offset*))

(defn get-depth
  "Extracts depth setting from query, if specified. If not returns
  default depth of 0"
  [{:keys [depth opts] :as _query-map}]
  (let [depth* (or depth
                   (:depth opts)
                   0)]
    (when-not (and (int? depth*) (>= depth* 0))
      (throw (ex-info (str "Invalid query depth specified: " depth*)
                      {:status 400 :error :db/invalid-query})))
    depth*))


(defn get-max-fuel
  "Extracts max-fuel from query if specified, or uses Integer/max a default."
  [{:keys [fuel max-fuel] :as query-map}]
  (when max-fuel
    (log/info "Deprecated max-fuel used in query: " query-map))
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
            rel-binding?   (and (some sequential? (vals supplied-vars*))
                                (not (every? #(util/pred-ident? %) (vals supplied-vars*))))]
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

(defn keywordize-keys
  [m]
  (reduce-kv (fn [acc k v]
               (if (string? k)
                 (assoc acc (keyword k) v)
                 (assoc acc k v)))
             {} m))

(defn consolidate-ident-vars
  "When where statements use supplied vars that are identies in s or o position
  they need to get resolved before executing the query.
  Here we consolidate them from all where statements into a single :ident-vars
  key on the parsed query map."
  [{:keys [where] :as parsed-query}]
  (let [all-ident-vars (->> where
                            (mapcat :ident-vars)
                            (into #{})
                            not-empty)]
    (assoc parsed-query :ident-vars all-ident-vars)))

(defn update-positions
  [{:keys [variable] :as tuple-item} in-vars]
  (if variable
    (let [in-n (util/index-of in-vars variable)]
      (cond-> tuple-item
              in-n (assoc :in-n in-n)))
    tuple-item))

(defn update-position+type
  "Like update-position, but also flags data type for things that need it (e.g. grouping, select statement)"
  [{:keys [variable] :as tuple-item} in-vars all-vars]
  (cond-> (update-positions tuple-item in-vars)

          ;; we know item is an IRI
          (#{:s :p} (get all-vars variable))
          (assoc :iri? true)

          ;; we know item is an object variable and will therefore be a two-tuple of [value datatype]
          (= :o (get all-vars variable))
          (assoc :o-var? true)))

(defn gen-x-form
  "Returns x-form function that take flakes as an input and returns out
  only the needed variables from the flake based on the query's processing needs."
  [out-vars {s-variable :variable} {p-variable :variable} {o-variable :variable}]
  (let [s-var? (util/index-of out-vars s-variable)
        p-var? (util/index-of out-vars p-variable)
        o-var? (util/index-of out-vars o-variable)]
    (cond
      (and s-var? p-var? o-var?)
      (map (fn [f] [(flake/s f) (flake/p f) [(flake/o f) (flake/dt f)]]))

      (and s-var? o-var?)
      (map (fn [f] [(flake/s f) [(flake/o f) (flake/dt f)]]))

      (and s-var? p-var?)
      (map (fn [f] [(flake/s f) (flake/p f)]))

      (and p-var? o-var?)
      (map (fn [f] [(flake/p f) [(flake/o f) (flake/dt f)]]))

      s-var?
      (map (fn [f] [(flake/s f)]))

      o-var?
      (map (fn [f] [[(flake/o f) (flake/dt f)]]))

      p-var?
      (map (fn [f] [(flake/p f)])))))

(defn build-vec-extraction-fn
  [extraction-positions]
  (fn [input-item]
    (mapv (partial nth input-item)
          extraction-positions)))

(defn gen-passthrough-fn
  "Transforms input variables into required passthrough variables.
  Optimized for several arity.

  Passthrough variables should retain output order across where statements."
  [out-vars {:keys [others] :as _vars} in-vars]
  (let [passthrough-vars (filter (set others) out-vars)     ;; only keep output vars in 'others'
        passthrough-pos  (keep #(when-let [pass-pos (util/index-of in-vars %)]
                                  pass-pos) passthrough-vars)]
    (build-vec-extraction-fn passthrough-pos)))


(defn get-idx
  [{s-variable :variable, s-supplied? :supplied?, s-in? :in-n}
   {p-value :value, p-variable :variable}
   {o-variable :variable, o-supplied? :supplied?, o-in? :in-n}]
  (let [have-s? (if s-variable
                  (or s-supplied? s-in?)
                  true)
        have-p? (boolean p-value)                           ;; todo - add variable support for 'p'
        have-o? (if o-variable
                  (or o-supplied? o-in?)
                  true)]
    (cond
      have-s? :spot
      (and have-p? have-o?) :post
      have-p? :psot
      have-o? :opst
      ;; if have none, this is an [?s ?p ?o] query for everything
      :else :spot)))

(defn rearrange-out-vars
  "Puts pass-through vars (passed through from prior where statements)
  next to each other, and vars generated in the current where clause
  that need to carry through next to each other - so we can efficiently
  concat the two when passing on to next step.

  Ensures the generated vars (which come from flakes as a result of searches)
  are in order of s p o so the flake-x-form function retains proper ordering."
  [out-vars-s out-flake-pos passthrough-vars]
  (let [generated-vars-s (set/difference out-vars-s (into #{} passthrough-vars))
        out-flake-pos*   (filter generated-vars-s out-flake-pos)]
    (into [] (concat out-flake-pos* passthrough-vars))))

(defn get-passthrough-vars
  "Variables in the parsing that are not used in the next where statement join, but
  are needed in a future where statement, or 'select' variable output.

  Passthrough vars are vars in 'out-vars' that are out output of the flake
  results from the search."
  [out-vars-set out-flake-vars]
  (into [] (set/difference out-vars-set out-flake-vars)))


(defn order-in-vars
  "The needed vars will be ordered by the prior-vars flake-out followed by
  the prior-vars others as applicable to retain ordering across where statements."
  [in-vars-s {:keys [flake-out others] :as _prior-vars}]
  (let [in-vars-flake (filter in-vars-s flake-out)
        in-vars-other (filter in-vars-s others)]
    (into [] (concat in-vars-flake in-vars-other))))

(defn get-in-vars
  "In vars are all variables needed to be output minus flake-out
  vars that are query result output in this step, plus flake-in vars that
  are needed to perform that query."
  [out-vars {:keys [flake-in flake-out] :as vars} prior-vars]
  ;; all vars are the output vars + any var needed as input into the query search
  (let [in-vars-s (-> (set out-vars)
                      (set/difference (set flake-out))
                      (into flake-in))]
    (order-in-vars in-vars-s prior-vars)))


(defn order-out-vars
  "We use the final 'select' statement output variables as the foundation of what is needed
  as cascaded through prior where statements.

  The out vars will consist of query result flake output and possible other vars passed through
  from prior where clauses.

  This makes the flake output vars needed come first (in 's p o' order) followed by any vars
  required that are passed through from prior statments. This allows the x-forms of both the
  query result flakes and the prior results passed through able to 'concat' to produce the final
  properly ordered output.

  There could be an order-by or group-by var not included in the select statement, so
  those must get added into the results here - note group-by without order-by will create an order-by"
  [select-out-vars {:keys [vars] :as _where-clause} {:keys [parsed] :as _order-by}]
  (when-let [illegal-var (some #(when-not (contains? (:all vars) %) %) select-out-vars)]
    (throw (ex-info (str "Variable " illegal-var " used in select statement but does not exist in the query.")
                    {:status 400 :error :db/invalid-query})))
  (let [{:keys [flake-out others nils]} vars
        order-by   (->> (map :variable parsed)
                        (remove nil?))
        out-vars-s (into (set select-out-vars) order-by)
        flake-out  (filter out-vars-s flake-out)            ;; only keep flake-out vars needed in final output
        others-out (filter out-vars-s others)]              ;; only keep other vars needed in final output
    (into [] (concat flake-out others-out nils))))

(defn where-clause-reverse-tuple
  "When doing a final pass of the where clause in reverse, we calculate the variable output
  needed for each clause (starting with the :select output) and pass it into the next
  statement.

  Functions are created to take flake output from the step and output just the flake vars needed
  while also ensuring any non-flake output that must be passed through this step is taken."
  [{:keys [s p o vars prior-vars] :as where-clause} out-vars]
  (let [in-vars        (get-in-vars out-vars vars prior-vars)
        s*             (update-positions s in-vars)
        p*             (update-positions p in-vars)
        o*             (update-positions o in-vars)
        flake-x-form   (gen-x-form out-vars s* p* o*)
        passthrough-fn (gen-passthrough-fn out-vars vars in-vars)
        nils-fn        (when (:nils vars)
                         (union/gen-nils-fn out-vars vars))]
    (-> where-clause
        (assoc :in-vars in-vars
               :out-vars out-vars
               :s s*
               :o o*
               :idx (get-idx s* p o*)
               :flake-x-form flake-x-form
               :passthrough-fn passthrough-fn
               :nils-fn nils-fn)
        ;; note, we could also dissoc :vars as they are no longer needed, but retain for now to help debug query parsing
        (dissoc :prior-vars))))


(defn where-clause-reverse-union
  "For union statements, need to process each of the two union where clauses.
  Unions acts as an 'or', and can include different variables or the same.

  If unions use different variables, the variables one statement uses output
  as 'nil' in the other."
  [{:keys [where] :as union-clause} out-vars]
  (let [[union1 union2] where
        union1* (where-meta-reverse union1 out-vars)
        union2* (where-meta-reverse union2 out-vars)]
    (assoc union-clause :where [union1* union2*]
                        ;; union1 and union2 should have the identical vars, so just pick one for latest :in-vars
                        :in-vars (-> union1* first :in-vars)
                        :out-vars out-vars)))

(defn where-meta-reverse
  "Goes through where statements once more but in reverse.
   Now that all variables for each statement are known, we
   need to output only the needed vars for each statement and
   order those output vars in as efficient manner as possible.

   The ordering allows the output tuples of each statement to be a:
   (concat <flake-vars-needing-output> <vars-from-prior-statements-needing-output>)
   where you can think of each of these two sequences as an x-form of their inputs.
   In the case of the first statement an x-form of the flake output from the index-range
   call, in the second statement an x-form of the prior step's output."
  [where last-out-vars]
  (loop [[{:keys [type] :as clause} & r] (reverse where)
         out-vars last-out-vars
         acc      []]
    (if clause
      (let [clause* (case type
                      (:class :tuple :iri) (where-clause-reverse-tuple clause out-vars)
                      :optional (throw (ex-info "OPTIONAL - TODO" {}))
                      :union (where-clause-reverse-union clause out-vars))]
        (recur r (:in-vars clause*) (conj acc clause*)))
      (into [] (reverse acc)))))


(defn update-select
  "Updates select statement variables with final where clause positions of items.

  If group-by is used, grouping can re-order output so utilize out-vars from that
  as opposed to the last where statement."
  [{:keys [spec] :as select} where {group-out-vars :out-vars, grouped-vars :grouped-vars, :as _group-by}]
  (let [last-where (last where)
        out-vars   (or group-out-vars
                       (:out-vars last-where))
        {:keys [all]} (:vars last-where)                    ;; the last where statement has an aggregation of all variables
        spec*      (cond->> (mapv #(update-position+type % out-vars all) spec)
                            grouped-vars (mapv #(if (grouped-vars (:variable %))
                                                  (assoc % :grouped? true)
                                                  %)))]
    (assoc select :spec spec*)))

(defn update-delete
  "Updates a delete statement variables with final where clause positions of items."
  [{:keys [s p o] :as delete} where {group-out-vars :out-vars, :as _group-by}]
  (let [last-where (last where)
        out-vars   (or group-out-vars
                       (:out-vars last-where))
        {:keys [all]} (:vars last-where)]                   ;; the last where statement has an aggregation of all variables
    (assoc delete :s (update-position+type s out-vars all)
                  :p (update-position+type p out-vars all)
                  :o (update-position+type o out-vars all))))


(defn build-order-fn
  "Returns final ordering function that take a single arg, the where results,
  and outputs the sorted where results."
  [{:keys [parsed] :as _order-by}]
  (let [compare-fns (mapv (fn [{:keys [in-n order o-var?]}]
                            (case order
                              :asc (if o-var?
                                     (fn [x y]
                                       (let [[x-val x-dt] (nth x in-n)
                                             [y-val y-dt] (nth y in-n)]
                                         (let [dt-cmp (compare x-dt y-dt)]
                                           (if (zero? dt-cmp)
                                             (compare x-val y-val)
                                             dt-cmp))))
                                     (fn [x y]
                                       (compare (nth x in-n) (nth y in-n))))
                              :desc (if o-var?
                                      (fn [x y]
                                        (let [[x-val x-dt] (nth x in-n)
                                              [y-val y-dt] (nth y in-n)]
                                          (let [dt-cmp (compare y-dt x-dt)]
                                            (if (zero? dt-cmp)
                                              (compare y-val x-val)
                                              dt-cmp))))
                                      (fn [x y]
                                        (compare (nth y in-n) (nth x in-n))))))
                          parsed)]
    (if (= 1 (count compare-fns))
      (first compare-fns)
      ;; if more than one variable being ordered, need to compose comparators together
      (fn [x y]
        (loop [[compare-fn & r] compare-fns]
          (if compare-fn
            (let [res (compare-fn x y)]
              (if (zero? res)
                (recur r)
                res))
            ;; no more compare functions, items are equal and return zero
            0))))))

(defn update-order-by
  "Updates order-by, if applicable, with final where clause positions of items."
  [{:keys [parsed] :as order-by} group-by where]
  (when order-by
    (let [{:keys [out-vars vars] :as _last-where} (last where)
          parsed*    (mapv #(update-position+type % out-vars (:all vars)) parsed)
          order-by*  (assoc order-by :parsed parsed*)
          comparator (build-order-fn order-by*)]
      (assoc order-by* :comparator comparator))))

(defn lazy-group-by
  "Returns lazily parsed results from group-by.
  Even though the query results must be fully realized through sorting,
  a pre-requisite of grouping, the grouping itself can be lazy which will
  help with large result sets that have a 'limit'."
  [grouping-fn grouped-vals-fn results]
  (lazy-seq
    (when-let [results* (seq results)]
      (let [fst  (first results*)
            fv   (grouping-fn fst)
            fres (grouped-vals-fn fst)
            [next-chunk rest-results] (loop [rest-results (rest results*)
                                             acc          [fres]]
                                        (let [result (first rest-results)]
                                          (if result
                                            (if (= fv (grouping-fn result))
                                              (recur (next rest-results) (conj acc (grouped-vals-fn result)))
                                              [(conj fv acc) rest-results])
                                            [(conj fv acc) nil])))]
        (cons next-chunk (lazy-group-by grouping-fn grouped-vals-fn (lazy-seq rest-results)))))))


(defn update-group-by
  "Updates group-by, if applicable, with final where clause positions of items."
  [{:keys [parsed] :as group-by} where]
  (when group-by
    (let [{:keys [out-vars all] :as _last-where} (last where)
          parsed*               (mapv #(update-position+type % out-vars all) parsed)
          group-by*             (assoc group-by :parsed parsed*)
          grouped-positions     (mapv :in-n parsed*)        ;; returns 'n' positions of values used for grouping
          partition-fn          (build-vec-extraction-fn grouped-positions) ;; returns fn containing only grouping vals, used like a 'partition-by' fn
          grouped-val-positions (filterv                    ;; returns 'n' positions of values that are being grouped
                                  (complement (set grouped-positions))
                                  (range (count out-vars)))
          grouped-vals-fn       (build-vec-extraction-fn grouped-val-positions) ;; returns fn containing only values being grouped (excludes grouping vals)
          ;; grouping fn takes sorted results, and partitions results by group-by vars returning only the values being grouped.
          ;; we don't yet merge all results together as that work is unnecessary if using an offset, or limit
          grouping-fn           (fn [results]
                                  (lazy-group-by partition-fn grouped-vals-fn results))
          ;; group-finish-fn takes final results and merges results together
          grouped-out-vars      (into (mapv :variable parsed) (map #(nth out-vars %) grouped-val-positions))]
      (assoc group-by* :out-vars grouped-out-vars           ;; grouping can change output variable ordering, as all grouped vars come first then groupings appended to end
                       :grouped-vars (into #{} (map #(nth out-vars %) grouped-val-positions)) ;; these are the variable names in the output that are grouped
                       :grouping-fn grouping-fn))))


(defn get-clause-vars
  [new-flake-vars {:keys [others all] :as _prior-vars}]
  (let [[s-var p-var o-var] new-flake-vars
        new-flakes-set (set (remove nil? new-flake-vars))
        flake-in*      (filter #(contains? all %) new-flake-vars) ;; any pre-existing var used in the flake
        others-set     (set/difference (into #{} (keys all)) new-flakes-set)]
    {:flake-in  flake-in*
     :flake-out new-flake-vars
     :all       (cond-> all
                        s-var (assoc s-var :s)
                        p-var (assoc p-var :p)
                        o-var (assoc o-var :o))
     ;; takes others-set (all vars not output by query result flakes)
     ;; and retains the prior 'others' ordering, appending any new vars to end.
     :others    (loop [[old-other & r] others
                       remaining-others others-set
                       new-others       []]
                  (if old-other
                    ;; and old other might end up being flake
                    (let [new-other* (if (others-set old-other)
                                       (conj new-others old-other) ;; and old-other might now be flake output, so make sure in the set
                                       new-others)]
                      (recur r (disj remaining-others old-other) new-other*))
                    ;; once we retain the previous other ordering, add in any remaining others that might be new
                    (into new-others remaining-others)))}))

(defn add-where-meta-tuple
  [{:keys [s p o] :as where-smt} prior-vars supplied-vars]
  (let [s-var       (:variable s)
        p-var       (:variable p)
        o-var       (:variable o)
        s-supplied? (supplied-vars s-var)
        p-supplied? (supplied-vars p-var)
        o-supplied? (supplied-vars o-var)
        s-out?      (and s-var (not s-supplied?))
        p-out?      (and p-var (not p-supplied?))
        o-out?      (and o-var (not o-supplied?))
        flake-vars  [(when s-out? s-var) (when p-out? p-var) (when o-out? o-var)] ;; each var needed coming out of flake in [s p o] position
        vars        (get-clause-vars flake-vars prior-vars)
        where-smt*  (cond-> (assoc where-smt
                              :vars vars
                              :prior-vars prior-vars)
                            s-supplied? (assoc-in [:s :supplied?] true)
                            p-supplied? (assoc-in [:p :supplied?] true)
                            o-supplied? (assoc-in [:o :supplied?] true))]
    ;; return signature of all statements is vector of where statements
    where-smt*))

(defn add-nested-where
  "Optional and Union query statements have nested tuple where clauses where each
  statement must be parsed, then only the last 'vars' from the last statement
   needs to be passed back to the standard query processing.

   This function does that interim processing.

   Returns two-tuple of where statement further processed, and the last vars from the last where."
  [where prior-vars supplied-vars]
  (let [where* (loop [[where-smt & r] where
                      prior-vars prior-vars
                      acc        []]
                 (if where-smt
                   (let [where-item* (add-where-meta-tuple where-smt prior-vars supplied-vars)]
                     (recur r (:vars where-item*) (conj acc where-item*)))
                   acc))
        vars   (-> where* last :vars)]
    [where* vars]))

(defn add-where-meta-optional
  "Handles optional clause additional parsing."
  [{:keys [where] :as optional-where-clause} prior-vars supplied-vars]
  (throw (ex-info (str "Multi-statement optional clauses not yet supported!")
                  {:status 400 :error :db/invalid-query}))
  ;; TODO! - parsing here should be working OK but need to implement logic
  (let [[where* vars] (add-nested-where where prior-vars supplied-vars)]
    (assoc optional-where-clause :where where*
                                 :prior-vars prior-vars
                                 :vars vars)))


(defn add-where-meta-union
  "Handles union clause additional parsing."
  [{:keys [where] :as union-where-clause} prior-vars supplied-vars]
  (let [[union1 union2] where
        [union1* _] (add-nested-where union1 prior-vars supplied-vars)
        [union2* _] (add-nested-where union2 prior-vars supplied-vars)
        [union1** union2**] (union/merge-vars union1* union2*)]
    (assoc union-where-clause :where [union1** union2**]
                              :prior-vars prior-vars
                              :vars (-> union2** last :vars))))


(defn add-where-meta
  "Adds input vars and output vars to each where statement."
  [{:keys [out-vars where select delete supplied-vars order-by group-by op-type] :as parsed-query}]
  ;; note: Currently 'p' is always fixed, but that is an imposed limitation - it should be able to be a variable.
  (loop [[where-smt & r] where
         i          0
         prior-vars {:flake-in  []                          ;; variables query will need to execute
                     :flake-out []                          ;; variables query result flakes will output
                     :all       {}                          ;; cascading set of all variables used in statements through current one
                     :others    []}                         ;; this is all vars minus the flake vars
         where*     []]
    (if where-smt
      (let [where-smt* (case (:type where-smt)
                         (:class :tuple :iri) (add-where-meta-tuple where-smt prior-vars supplied-vars)
                         :optional (add-where-meta-optional where-smt prior-vars supplied-vars)
                         :union (add-where-meta-union where-smt prior-vars supplied-vars))]
        (recur r (inc i) (:vars where-smt*) (conj where* where-smt*)))
      (let [last-clause     (last where*)
            select-out-vars (if (= :union (:type last-clause))
                              (union/order-out-vars out-vars last-clause order-by)
                              (order-out-vars out-vars last-clause order-by))
            where*          (where-meta-reverse where* select-out-vars)
            order-by*       (update-order-by order-by group-by where*)
            group-by*       (update-group-by group-by where*)]
        (cond-> (assoc parsed-query :where where*
                                    :order-by order-by*
                                    :group-by group-by*)
                (= :select op-type) (assoc :select (update-select select where* group-by*))
                (= :delete op-type) (assoc :delete (update-delete delete where* group-by*)))))))

(defn parse-delete
  "Parses delete statement syntax... e.g. from a statement like:
  {:delete ['?s '?p '?o]
   :where  [['?s :schema/age 34]
            ['?s '?p '?o]]}"
  [{:keys [context] :as parsed-query} {:keys [delete] :as _query-map'} db]
  (let [[s p o] delete
        val-map (fn [v iri?]
                  (if-let [var (q-var->symbol v)]
                    {:variable var}
                    {:value (if iri?
                              (json-ld/expand-iri v context)
                              v)}))
        s*      (val-map s true)
        p*      (let [p* (val-map p true)]
                  (if-let [p-value (:value p*)]
                    {:value (pred-id-strict db p-value)}
                    p*))
        o*      (val-map o false)]
    (assoc parsed-query :out-vars (keep :variable [s* p* o*])
                        :delete {:s s*
                                 :p p*
                                 :o o*})))


;; TODO - only capture :select, :where, :limit - need to get others
(defn parse*
  [db {:keys [opts prettyPrint filter context depth
              orderBy order-by groupBy group-by] :as query-map} supplied-vars]
  (log/trace "parse* query-map:" query-map)
  (let [op-type           (cond
                            (some #{:select :selectOne :selectReduced :selectDistince} (keys query-map))
                            :select

                            (contains? query-map :delete)
                            :delete

                            :else
                            (throw (ex-info "Invalid query type, not a select or delete type."
                                            {:status 400 :error :db/invalid-query})))
        rel-binding?      (sequential? supplied-vars)
        supplied-var-keys (if rel-binding?
                            (-> supplied-vars first keys set)
                            (-> supplied-vars keys set))
        opts*             (keywordize-keys opts)
        json-ld-db?       (= :json-ld (dbproto/-db-type db))
        context*          (when json-ld-db?
                            (if (:js? opts*)
                              (json-ld/parse-context
                                (get-in db [:schema :context-str]) (or context (get query-map "@context")))
                              (json-ld/parse-context
                                (get-in db [:schema :context]) (or context (get query-map "@context")))))
        order-by*         (or orderBy order-by (:orderBy opts))
        group-by*         (or groupBy group-by (:groupBy opts))
        parsed            (cond-> {:op-type       op-type
                                   :strategy      :legacy
                                   :context       context*
                                   :rel-binding?  rel-binding?
                                   :where         (parse-where db query-map supplied-var-keys context*)
                                   :opts          (if (not (nil? (:parseJSON opts*)))
                                                    (-> opts*
                                                        (assoc :parse-json? (:parseJSON opts*))
                                                        (dissoc :parseJSON))
                                                    opts*)
                                   :limit         (get-limit query-map) ;; limit can be a primary key, or within :opts
                                   :offset        (get-offset query-map) ;; offset can be a primary key, or within :opts
                                   :depth         (get-depth query-map) ;; for query crawling, default depth to crawl
                                   :fuel          (get-max-fuel query-map)
                                   :supplied-vars supplied-var-keys
                                   :pretty-print  (if (boolean? prettyPrint) ;; prettyPrint can be a primary key, or within :opts
                                                    prettyPrint
                                                    (:prettyPrint opts))
                                   :compact-fn    (json-ld/compact-fn context*)}
                                  filter (add-filter filter supplied-var-keys) ;; note, filter maps can/should also be inside :where clause
                                  order-by* (add-order-by order-by*)
                                  group-by* (add-group-by group-by*)
                                  true (consolidate-ident-vars) ;; add top-level :ident-vars consolidating all where clause's :ident-vars
                                  (= :select op-type) (json-ld-select/parse query-map db)
                                  (= :delete op-type) (parse-delete query-map db)
                                  true (add-where-meta))]
    (or (re-parse-as-simple-subj-crawl parsed)
        parsed)))

(defn parse
  [db query-map]
  (let [query-map*   (if (basic-query? query-map)
                       (basic-to-analytical-transpiler db query-map)
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
