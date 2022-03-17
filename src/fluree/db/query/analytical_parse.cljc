(ns fluree.db.query.analytical-parse
  (:require [clojure.string :as str]))

#?(:clj (set! *warn-on-reflection* true))

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


(defn parse-aggregate
  [x]
  (let [list-agg   (#?(:clj read-string :cljs cljs.reader/read-string) x)
        as?        (= 'as (first list-agg))
        as         (if as? (-> (str "?" (last list-agg)) symbol) (->> list-agg (str "?") symbol))
        func-list  (if as? (let [func-list (second list-agg)]
                             (if (coll? func-list) func-list
                                                   (throw (ex-info (str "Invalid aggregate selection. As can only be used in conjunction with other functions. Provided: " x)
                                                                   {:status 400 :error :db/invalid-query})))) list-agg)
        list-count (count func-list)
        [fun arg var] (cond (= 3 list-count) [(first func-list) (second func-list) (last func-list)]
                            (and (= 2 list-count) (= 'sample (first func-list)))
                            (throw (ex-info (str "The sample aggregate function takes two arguments: n and a variable, provided: " x)
                                            {:status 400 :error :db/invalid-query}))
                            (= 2 list-count) [(first func-list) nil (last func-list)]
                            :else (throw (ex-info (str "Invalid aggregate selection, provided: " x)
                                                  {:status 400 :error :db/invalid-query})))
        agg-fn     (if-let [agg-fn (built-in-aggregates fun)]
                     (if arg (fn [coll] (agg-fn arg coll)) agg-fn)
                     (throw (ex-info (str "Invalid aggregate selection function, provided: " x)
                                     {:status 400 :error :db/invalid-query})))
        [agg-fn variable] (let [distinct? (and (coll? var) (= (first var) 'distinct))
                                variable  (if distinct? (second var) var)
                                agg-fn    (if distinct? (fn [coll] (-> coll distinct agg-fn))
                                                        agg-fn)]
                            [agg-fn variable])]
    {:variable variable
     :as       as
     :code     agg-fn}))

(defn aggregate?
  [x]
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))

(defn q-var->symbol
  "Returns a query variable as a symbol, else nil if not a query variable."
  [x]
  (when (or (keyword? x)
            (and (string? x)
                 (= \? (first x))))
    (symbol x)))

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


(defn parse-select
  [select-smt]
  (let [_ (or (every? #(or (string? %) (map? %)) select-smt)
              (throw (ex-info (str "Invalid select statement. Every selection must be a string or map. Provided: " select-smt) {:status 400 :error :db/invalid-query})))]
    (map (fn [select]
           (let [var-symbol (q-var->symbol select)]
             (cond var-symbol {:variable var-symbol}
                   (aggregate? select) (parse-aggregate select)
                   (map? select) (parse-map select)
                   ;(get interim-vars var-symbol) {:value (get interim-vars var-symbol)}
                   :else (throw (ex-info (str "Invalid select in statement, provided: " select)
                                         {:status 400 :error :db/invalid-query}))))) select-smt)))


(defn get-ad-hoc-select-spec
  [{:keys [selectOne select selectDistinct selectReduced]} opts]
  (let [select-smt    (or selectOne select selectDistinct selectReduced)
        inVector?     (vector? select-smt)
        select-smt    (if inVector? select-smt [select-smt])
        parsed-select (parse-select select-smt)
        aggregates    (filter #(contains? % :code) parsed-select)
        expandMap?    (some #(contains? % :selection) parsed-select)
        aggregates    (if (empty? aggregates) nil aggregates)
        orderBy       (when-let [orderBy (:orderBy opts)]
                        (if (or (string? orderBy) (and (vector? orderBy) (#{"DESC" "ASC"} (first orderBy))))
                          (if (vector? orderBy) orderBy ["ASC" orderBy])
                          (throw (ex-info (str "Invalid orderBy clause, must be variable or two-tuple formatted ['ASC' or 'DESC', var]. Provided: " orderBy)
                                          {:status 400
                                           :error  :db/invalid-query}))))]
    {:select          parsed-select
     :aggregates      aggregates
     :expandMaps?     expandMap?
     :orderBy         orderBy
     :groupBy         (:groupBy opts)
     :limit           (or (:limit opts) 100)
     :offset          (or (:offset opts) 0)
     :selectOne?      (boolean selectOne)
     :selectDistinct? (boolean (or selectDistinct selectReduced))
     :inVector?       inVector?
     :prettyPrint     (or (:prettyPrint opts) false)}))


(defn symbolize-var-keys
  "Turns keys of var maps into symbols"
  [q-map]
  (let [keys (map symbol (keys q-map))
        vals (vals q-map)]
    (zipmap keys vals)))

(defn parse-where-map
  "Where item is a map, which always only contain a single key/val in them."
  [clause]
  (let [[clause-type clause-val] (first clause)]
    {:clause clause-val
     :type   clause-type}
    #_(case clause-type
        :optional                                           ;; left join
        (assoc clause* :todo nil)

        :union
        (if (= 2 (count clause-val))
          :TODO                                             ;; TODO - parse contents of each as new where item
          (throw (ex-info (str "Invalid where clause, 'union' clause must have exactly two solutions. Instead, "
                               (count clause-val) " were specified.")
                          {:status 400 :error :db/invalid-query})))

        :bind
        (if (map? clause-val)
          :TODO                                             ;; TODO - need to verify binding vars used in values exist in previous part of the query
          (throw (ex-info (str "Invalid where clause, 'bind' must be a map with binding vars as keys "
                               "and binding scalars, or aggregates, as values.")
                          {:status 400 :error :db/invalid-query})))

        :filter
        (if (sequential? clause-val)
          :TODO
          (throw (ex-info (str "Invalid where clause, 'filter' value must be a vector with one or more "
                               "filter functions.")
                          {:status 400 :error :db/invalid-query})))

        :minus                                              ;; negation - SPARQL 1.1, not yet supported
        (throw (ex-info (str "Invalid where clause, Fluree does not yet support the 'minus' operation.")
                        {:status 400 :error :db/invalid-query}))

        ;; else
        (throw (ex-info (str "Invalid where clause, unsupported where clause operation: " clause-type)
                        {:status 400 :error :db/invalid-query}))))

  )

(defn parse-where-tuple
  "Parses where clause tuples (not maps)"
  [clause]
  (let [tuple-count (count clause)]
    (case tuple-count
      3
      (let [[_ p _] clause
            fulltext? (str/starts-with? p "fullText:")
            rdf-type? (or (= "rdf:type" p)
                          (= "a" p))]
        (cond
          fulltext? {:clause clause
                     :type   :fulltext}
          rdf-type? {:clause clause
                     :type   :rdf-type}
          :else {:clause clause
                 :type   :tuple}))


      4
      {:clause clause
       :type   :external}

      2
      {:clause clause
       :type   :binding}

      ;else
      (if (sequential? (first clause))
        (throw (ex-info (str "Invalid where clause, it should contain 2, 3 or 4 tuples. "
                             "It appears you have an extra nested vector here: " clause)
                        {:status 400 :error :db/invalid-query}))
        (throw (ex-info (str "Invalid where clause, it should contain 2, 3 or 4 tuples but instead found: " clause)
                        {:status 400 :error :db/invalid-query}))))))


(defn parse-where
  "Parses where clause"
  [{:keys [where] :as _query-map} opts]
  ;; top-level error catching
  (if (sequential? where)
    (mapv #(cond
             (map? %)
             ;; TODO - any maps resort to traditional strategy
             (do
               (parse-where-map %))

             (sequential? %)
             (parse-where-tuple %)

             :else
             (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                             {:status 400 :error :db/invalid-query})))
          where)
    (throw (ex-info (str "Invalid where clause, must be a vector of tuples and/or maps: " where)
                    {:status 400 :error :db/invalid-query}))))



(defn subject-crawl?
  "Returns true if, when given parsed query, the select statement is a
  subject crawl - meaning there is nothing else in the :select except a
  graph crawl on a list of subjects"
  [{:keys [select] :as _parsed-query}]
  (and (:expandMap? select)
       (not (:inVector? select))))


(defn simple-where?
  [parsed-query]
  :TODO!!!!!

  )

(defn simple-subject-crawl?
  "Returns true if query contains a single subject crawl.
  e.g.
  {:select {?subjects ['*']
   :where [...]}"
  [parsed-query]
  (and (subject-crawl? parsed-query)
       (simple-where? parsed-query)))

(defn extract-vars
  "Returns query map without vars, to allow more effective caching of parsing."
  [query-map]
  (dissoc query-map :vars))

(defn parse*
  [query-map' opts]
  (let [parsed {:select   (get-ad-hoc-select-spec query-map' opts)
                :strategy :legacy
                :where    (parse-where query-map' opts)}
        ssc?   (simple-subject-crawl? parsed)]
    (cond-> parsed
            ssc? (assoc :strategy :simple-subject-crawl))))

(defn parse
  [query-map opts]
  (try
    (let [{:keys [vars]} query-map
          query-map'   (extract-vars query-map)
          parsed-query (parse* query-map' opts)]

      (assoc parsed-query :vars (symbolize-var-keys vars)))
    (catch Exception e (println "PARSE ERROR: " query-map)
                       (println "EXCEPTION: " (pr-str e))
                       nil)))