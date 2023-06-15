(ns fluree.db.query.fql
  (:require
    [fluree.db.dbproto :as dbproto]
    [fluree.db.util.log :as log]
    [clojure.string :as str]
    [fluree.db.util.core :as util :refer [try* catch*]]
    [fluree.db.query.analytical :as analytical]
    #?(:clj  [clojure.core.async :refer [go <!] :as async]
       :cljs [cljs.core.async :refer [go <!] :as async])
    [fluree.db.util.async :refer [<? go-try]]
    [fluree.db.query.analytical-parse :as q-parse]
    [fluree.db.query.subject-crawl.core :refer [simple-subject-crawl]])
  (:refer-clojure :exclude [vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare query)

(defn vswap!
  "This silly fn exists to work around a bug in go macros where they sometimes clobber
  type hints and issue reflection warnings. The vswap! macro uses interop so those forms
  get macroexpanded into the go block. You'll then see reflection warnings for reset
  deref. By letting the macro expand into this fn instead, it avoids the go bug.
  I've filed a JIRA issue here: https://clojure.atlassian.net/browse/ASYNC-240
  NB: I couldn't figure out how to get a var-arg version working so this only supports
  0-3 args. I didn't see any usages in here that need more than 2, but note well and
  feel free to add additional arities if needed (but maybe see if that linked bug has
  been fixed first in which case delete this thing with a vengeance and remove the
  refer-clojure exclude in the ns form).
  - WSM 2021-08-26"
  ([vol f]
   (clojure.core/vswap! vol f))
  ([vol f arg1]
   (clojure.core/vswap! vol f arg1))
  ([vol f arg1 arg2]
   (clojure.core/vswap! vol f arg1 arg2))
  ([vol f arg1 arg2 arg3]
   (clojure.core/vswap! vol f arg1 arg2 arg3)))

(defn fuel-flake-transducer
  "Can sit in a flake pipeline and accumulate a count of 'fuel-per' for every flake pulled
  or item touched. 'fuel-per' defaults to 1 fuel per item.

  Inputs are:
  - fuel - volatile! that holds fuel counter
  - max-fuel - throw exception if @fuel ever exceeds this number

  To get final count, just deref fuel volatile when when where is complete."
  ([fuel max-fuel] (fuel-flake-transducer fuel max-fuel 1))
  ([fuel max-fuel fuel-per]
   (fn [xf]
     (fn
       ([] (xf))                                            ;; transducer start
       ([result] (xf result))                               ;; transducer stop
       ([result flake]
        (vswap! fuel + fuel-per)
        (when (and max-fuel (> @fuel max-fuel))
          (throw (ex-info (str "Maximum query cost of " max-fuel " exceeded.")
                          {:status 400 :error :db/exceeded-cost})))
        (xf result flake))))))


(defn compare-fn
  [a b]
  (if (string? a)
    (let [res (compare (str/upper-case a) (str/upper-case b))]
      (if (= res 0)
        (* -1 (compare a b))
        res))
    (compare a b)))


(defn get-pretty-print-keys
  [select]
  (let [vars  (map (fn [select]
                     (cond (:as select)
                           (-> select :as str (subs 1))

                           (:code select)
                           (-> select :code str)

                           (:variable select)
                           (-> select :variable str (subs 1)))) select)
        freqs (frequencies vars)]
    (if (every? #(= 1 %) (vals freqs))
      vars
      (loop [[var & r] vars
             all-vars []]
        (cond (not var)
              all-vars

              ((set all-vars) var)
              (recur r (conj all-vars (str var "$" (count all-vars))))

              :else
              (recur r (conj all-vars var)))))))


(defn get-header-idx
  [headers select]
  (cond (:as select)
        (util/index-of headers (:as select))

        (:code select)
        (util/index-of (map str headers) (-> select :code str))

        (:variable select)
        (util/index-of headers (:variable select))))


(defn- build-expand-map
  "Builds list of two-tuples: ([tuple-index query-map] ...)
  for :select tuple positions that define a graph crawling query map.

  Used by 'expand-map' and 'replace-expand-map' functions for executing
  the query map and inserting the query map results into the final response
  respectively.

  i.e. if the initial query was {:select [?x {?person ['*']} ?y] .... }, then in the
  three-tuple :select clause is [?x ?person ?y], where ?person must be expanded with additional query results.

  Given this example, this function would output:
  ([1 ['*']]) - which means position 1 in the select clause tuple (0-indexed) needs to be expanded with a
  query: {:select ['*'] :from ?person}, for each instance of ?person returned from the query."
  [select pretty-print-keys]
  (keep-indexed (fn [idx select-item]
                  (when-let [query-map (:selection select-item)]
                    ;; if pretty print is used, the result is a map,
                    ;; and in the index should be the respective pretty-print key, else just the numerical index
                    (let [tuple-index (if pretty-print-keys
                                        (nth pretty-print-keys idx)
                                        idx)]
                      [tuple-index query-map])))
                select))


(defn- expand-map
  "Updates a two-tuple as defined by 'build-expand-map` function by executing the query-map query for
  the tuple-result using supplied db and options. Up
  [tuple-index query-map] -> [tuple-index query-map-result]

  Returns async channel with the transformed two-tuple, or a query exception if one occurs."
  [db query-opts tuple-result [tuple-index query-map]]
  ;; ignore any nil values in tuple-result at idx position (i.e. can happen with optional/left outer joins)
  (when-let [_id (get tuple-result tuple-index)]
    (async/go
      [tuple-index (<? (query db {:selectOne query-map
                                  :from      _id
                                  :opts      query-opts}))])))

(defn- replace-expand-maps
  "Follow-on step for 'expand-map' function above, replaces the final query map
  results into the tuple position specified. Designed to be used in a reducing function.

  tuple-result is a single tuple result, like [42 12345 'usa']
  expand-map-tuple is a two-tuple of index position to replace in the tuple result
  along with the value to replace it with, i.e. [1 {12345 {:firstName 'Jane', :lastName 'Doe'}}]
  After replacing position/index 1 in the initial tuple result in this example, the final output
  will be the modified tuple result of:
  [42 {12345 {:firstName 'Jane', :lastName 'Doe'}} 'usa']"
  [tuple-result expand-map-tuple]
  (when (util/exception? expand-map-tuple)
    (throw expand-map-tuple))
  (let [[tuple-index query-map-result] expand-map-tuple]
    (assoc tuple-result tuple-index query-map-result)))


(defn pipeline-expandmaps-result
  "For each tuple in the results that requires a query map expanded, fetches the
  results in parallel with `parallelism` supplied.

  Inputs are:
  - select - select specification map
  - pp-keys - if prettyPrint was done on the query, the results will be a map instead of a tuple. This lists the map keys
  - single-result? - if the query's :select was not wrapped in a vector, we return a single result instead of a tuple
  - db - the db to execute the query-map expansion with
  - opts - opts to use for the query-map expansion query
  - parallelism - how many queries to run in parallel
  - tuples-res - final response tuples that need one or more query expansions on them

  i.e. if a simple one-tuple result set were columns [?person], where ?person is just
  the subject id of persons... then the tuples would look like
  [[1234567] [1234566] [1234565] ...]

  The select clause might be {?person [person/fullName, person/age, {person/children [*]}]}

  This will produce the results of each of the select clauses based on the source tuples."
  [select pp-keys single-result? db fuel max-fuel opts parallelism tuples-res]
  (go-try
    (let [expandMaps (build-expand-map select pp-keys)
          queue-ch   (async/chan)
          res-ch     (async/chan)
          stop!      (fn [] (async/close! queue-ch) (async/close! res-ch))
          opts*      (-> (dissoc opts :limit :offset :orderBy :groupBy)
                         (assoc :fuel (volatile! 0)))
          af         (fn [tuple-res port]
                       (async/go
                         (try*
                           (let [tuple-res' (if single-result? [tuple-res] tuple-res)
                                 query-fuel (volatile! 0)]
                             (->> expandMaps
                                  (keep #(expand-map db (assoc opts* :fuel fuel) tuple-res' %)) ;; returns async channels, executes expandmap query
                                  (async/merge)
                                  (async/into [])
                                  (async/<!)                ;; all expandmaps with final results now in single vector
                                  (reduce replace-expand-maps tuple-res') ;; update original tuple with expandmaps result(s)
                                  (#(if single-result? [(first %) @query-fuel] [% @query-fuel])) ;; return two-tuple with second element being fuel consumed
                                  (async/put! port)))
                           (async/close! port)
                           (catch* e (async/put! port e) (async/close! port)))))]

      (async/onto-chan! queue-ch tuples-res)
      (async/pipeline-async parallelism res-ch af queue-ch)

      (loop [acc []]
        (let [next-res (async/<! res-ch)]
          (cond
            (nil? next-res)
            acc

            (util/exception? next-res)
            (do
              (stop!)
              next-res)

            :else
            (let [total-fuel (vswap! fuel + (second next-res))]
              (if (> total-fuel max-fuel)
                (do (stop!)
                    (ex-info (str "Query exceeded max fuel while processing: " max-fuel
                                  ". If you have permission, you can set the max fuel for a query with: 'opts': {'fuel' 10000000}")
                             {:error :db/insufficient-fuel :status 400}))
                (recur (conj acc (first next-res)))))))))))


(defn select-fn
  "Builds function that returns tuple result based on the :select portion of the original query
  when provided the list of tuples that result from the :where portion of the original query."
  [headers vars select]
  (let [{:keys [as variable value]} select
        select-val   (or as variable)
        idx          (get-header-idx headers select)
        tuple-select (cond
                       value (constantly value)
                       idx (fn [tuple] (nth tuple idx))
                       (get vars select-val) (constantly (get vars select-val)))]
    tuple-select))


(defn- select-tuples-fn
  "Returns a single function, that when applied against a full result tuple from
  the query's :where clause, preps the :select clause response with just the values
  in the specified order.

  The :where result tuples will contain a column/tuple index for every variable
  that appears in the where clause, but the :select clause specifies which of those
  variables to return in the result - which is often a subset.

  Here, the 'headers' will contain the where clause variables and what column/index
  they are in, and the 'select' will specify the select variables desired, and order."
  [headers vars select]
  (->> select
       (map (partial select-fn headers vars))
       (apply juxt)))


(defn order-result-tuples
  "Sorts result tuples when orderBy is specified.
   Order By can be:
   - Single variable, ?favNums
   - Two-tuple,  [ASC, ?favNums]
   - Three-tuple, [ASC, ?favNums, 'NOCASE'] - ignore case when sorting strings

  Operation should happen before tuples get filtered, as the orderBy variable might
  not be present in the :select clause.

  2 fuel per tuple ordered + 2 additional fuel for 'NOCASE'."
  ;; TODO - check/throw max fuel
  [fuel max-fuel headers orderBy tuples]
  (let [[order var option] orderBy
        comparator  (if (= "DESC" order) (fn [a b] (compare b a)) compare)
        compare-idx (util/index-of headers (symbol var))
        no-case?    (and (string? option) (= "NOCASE" (str/upper-case option)))
        keyfn       (if no-case?
                      #(str/upper-case (nth % compare-idx))
                      #(nth % compare-idx))]
    (if compare-idx
      (let [fuel-total (vswap! fuel + (* (if no-case? 4 2) (count tuples)))]
        (when (> fuel-total max-fuel)
          (throw (ex-info (str "Maximum query cost of " max-fuel " exceeded.")
                          {:status 400 :error :db/exceeded-cost})))
        (sort-by keyfn comparator tuples))
      tuples)))

(defn- process-ad-hoc-group
  ([db fuel max-fuel res select-spec opts]
   (process-ad-hoc-group db fuel max-fuel res select-spec nil opts))
  ([db fuel max-fuel {:keys [vars] :as res} {:keys [aggregates orderBy offset groupBy select limit expandMaps? selectDistinct? inVector? prettyPrint] :as select-spec} group-limit opts]
   (go-try (if (and aggregates (= 1 (count select))) ;; only aggregate
             (let [res  (second (analytical/calculate-aggregate res (first aggregates)))
                   res' (if prettyPrint
                          {(-> select first :as str (subs 1)) res}
                          res)]
               (if inVector? [res'] res'))

             (let [{:keys [headers tuples]} (if aggregates (analytical/add-aggregate-cols res aggregates) res)
                   offset'        (when (and offset (not groupBy)) ;; groupBy results cannot be offset (not sure why! was there)
                                    offset)
                   single-result? (and (not prettyPrint) (not inVector?))
                   pp-keys        (when prettyPrint (get-pretty-print-keys select))
                   xf             (apply comp
                                         (cond-> [(map (select-tuples-fn headers vars select))] ;; a function that formats a :where result tuple to specified :select clause
                                                 single-result? (conj (map first))
                                                 selectDistinct? (conj (fuel-flake-transducer fuel max-fuel 5)) ;; distinct charges 5 per item touched
                                                 selectDistinct? (conj (distinct))
                                                 offset' (conj (drop offset'))
                                                 group-limit (conj (take group-limit))
                                                 prettyPrint (conj (map #(zipmap (get-pretty-print-keys select) %)))))
                   result         (cond->> tuples
                                           orderBy (order-result-tuples fuel max-fuel headers orderBy)
                                           true (into [] xf))]
               (if expandMaps?
                 (<? (pipeline-expandmaps-result select pp-keys single-result? db fuel max-fuel opts 8 result))
                 result))))))

(defn ad-hoc-group-by
  [{:keys [headers vars tuples] :as res} groupBy]
  (log/info "Result passed to group-by:" res)
  (let [[inVector? groupBy] (cond (vector? groupBy) [true (map symbol groupBy)]
                                  (string? groupBy) [false [(symbol groupBy)]]
                                  :else (throw (ex-info
                                                 (str "Invalid groupBy clause, must be a string or vector. Provided: " groupBy)
                                                 {:status 400 :error :db/invalid-query})))
        group-idxs (map (fn [group-var]
                          (if-let [group-idx (util/index-of headers group-var)]
                            {::idx group-idx}
                            (if-let [group-val (get vars group-var)]
                              {::value group-val}
                              (throw (ex-info
                                       (str "Invalid groupBy clause - are all groupBy vars declared in the where clause. Provided: " groupBy)
                                       {:status 400 :error :db/invalid-query})))))
                        groupBy)]
    (reduce
      (fn [res tuple]
        (let [k  (map (fn [val-spec]
                        (if-let [idx (::idx val-spec)]
                          (nth tuple idx)
                          (::value val-spec)))
                      group-idxs)
              k' (if inVector? (into [] k) (first k))
              v  tuple]
          (assoc res k' (conj (get res k' []) v))))
      {} tuples)))

(defn- build-order-fn
  [orderBy groupBy]
  (let [[sortDirection sortCriteria] (if orderBy orderBy ["ASC" groupBy])]
    (cond
      (= sortCriteria groupBy)
      (if (= sortDirection "DESC")
        (fn [x y] (* -1 (compare-fn x y)))
        compare-fn)

      (and (coll? groupBy) (string? sortCriteria))
      (let [orderByIdx (util/index-of groupBy sortCriteria)]
        (if (= "DESC" sortDirection)
          (fn [x y] (* -1 (compare-fn (nth x orderByIdx) (nth y orderByIdx))))
          (fn [x y] (compare-fn (nth x orderByIdx) (nth y orderByIdx)))))

      :else nil)))

(defn filter-having
  "groupBy statements can optionally have a 'having' statement which filters
  items within the group."
  [{:keys [params function] :as having} headers group-map]
  (let [idxs     (analytical/get-tuple-indexes params headers)
        filtered (loop [[[k tuples] & r] group-map
                        acc {}]
                   (if k
                     (let [argument (flatten (analytical/transform-tuples-to-idxs idxs tuples))
                           res      (try*
                                      (function argument)
                                      (catch* e
                                              (log/error e (str "Error procesing fn: " (:fn-str having)
                                                                " with argument: " argument))
                                              (throw (ex-info (str "Error executing having function: " (:fn-str having)
                                                                   " with error message: " (ex-message e))
                                                              {:status 400 :error :db/invalid-query}))))]
                       (if res
                         (recur r (assoc acc k tuples))
                         (recur r acc)))
                     acc))]
    filtered))

(defn process-ad-hoc-res
  [db fuel max-fuel
   {:keys [headers vars] :as res}
   {:keys [groupBy orderBy limit selectOne? selectDistinct? inVector? offset having] :as select-spec}
   opts]
  (go-try (if groupBy
            (let [order-fn  (build-order-fn orderBy groupBy)
                  group-map (cond->> (ad-hoc-group-by res groupBy)
                                     order-fn (into (sorted-map-by order-fn))
                                     having (filter-having having headers)
                                     offset (drop offset)
                                     limit (take limit)
                                     selectOne? (take 1))]
              (loop [[[k tuples] & r] group-map
                     acc {}]
                (if k
                  (let [group-as-res   {:headers headers :vars vars :tuples tuples}
                        order-group-by (when (and orderBy (nil? order-fn))
                                         ;; if orderBy is not nil but order-fn is
                                         ;; that means we need to sort w/in each group
                                         orderBy)
                        v              (<? (process-ad-hoc-group
                                            db fuel max-fuel group-as-res
                                            (assoc select-spec :orderBy order-group-by
                                                               :offset 0 :limit nil)
                                            (assoc opts :offset 0 :limit nil)))]
                    (recur r (assoc acc k v)))
                  acc)))
            ; no group by
            (let [limit (if selectOne? 1 limit)
                  res   (<? (process-ad-hoc-group db fuel max-fuel res select-spec limit opts))]
              (cond (not (coll? res)) (if inVector? [res] res)
                    selectOne? (first res)
                    :else res)))))

(defn- process-ad-hoc-query
  [{:keys [db parsed-query fuel max-fuel] :as opts}]
  (log/debug "process-ad-hoc-query opts:" opts)
  (go-try
    (let [where-result (<? (analytical/q opts))
          _ (log/debug "process-ad-hoc-query where-result:" where-result)
          select-spec  (:select parsed-query)]
      (<? (process-ad-hoc-res db fuel max-fuel where-result select-spec opts)))))

(defn relationship-binding
  [{:keys [vars] :as opts}]
  (async/go-loop [[next-vars & rest-vars] vars
                  acc []]
    (if next-vars
      (let [opts' (assoc opts :vars next-vars)
            res   (<? (process-ad-hoc-query opts'))]
        (recur rest-vars (into acc res)))
      acc)))

(defn- ad-hoc-query
  "Legacy ad-hoc query processor"
  [db {:keys [rel-binding? vars] :as parsed-query} query-map]
  (log/debug "Running ad hoc query:" query-map)
  (let [{:keys [selectOne limit offset component orderBy groupBy prettyPrint opts]} query-map
        opts' (-> opts (assoc :parse-json? (:parseJSON opts)) (dissoc :parseJSON))
        opts' (cond-> (merge {:limit   limit :offset (or offset 0) :component component
                              :orderBy orderBy :groupBy groupBy :prettyPrint prettyPrint}
                             opts')
                      selectOne (assoc :limit 1)
                      true (assoc :max-fuel (:max-fuel opts)
                                  :fuel (or (:fuel opts)    ;; :fuel volatile! can be provided upstream
                                            (when (or (:max-fuel opts) (:meta opts))
                                              (volatile! 0)))
                                  :parsed-query parsed-query
                                  :query-map query-map
                                  :db db
                                  :vars vars))]
    (if rel-binding?
      (relationship-binding opts')
      (process-ad-hoc-query opts'))))

(defn cache-query
  "Returns already cached query from cache if available, else
  executes and stores query into cache."
  [{:keys [network ledger-id block auth conn] :as db} {:keys [opts] :as query-map}]
  ;; TODO - if a cache value exists, should max-fuel still be checked and throw if not enough?
  (let [oc        (:object-cache conn)
        query*    (update query-map :opts dissoc :fuel :max-fuel)
        cache-key [:query network ledger-id block auth query*]]
    ;; object cache takes (a) key and (b) fn to retrieve value if null
    (oc cache-key
        (fn [_]
          (let [pc (async/promise-chan)]
            (async/go
              (let [res (async/<! (query db (assoc-in query-map [:opts :cache] false)))]
                (async/put! pc res)))
            pc)))))

(defn cache?
  "Returns true if query was requested to run from the cache."
  [{:keys [opts] :as _query-map}]
  #?(:clj (:cache opts) :cljs false))

(defn query
  "Returns core async channel with results or exception"
  [db query-map]
  (log/debug "Running query:" query-map)
  (if (cache? query-map)
    (cache-query db query-map)
    (let [parsed-query (q-parse/parse db query-map)
          db*          (assoc db :ctx-cache (volatile! {}))] ;; allow caching of some functions when available
      (if (= :simple-subject-crawl (:strategy parsed-query))
        (simple-subject-crawl db* parsed-query)
        (ad-hoc-query db* parsed-query query-map)))))
