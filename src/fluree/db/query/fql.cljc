(ns fluree.db.query.fql
  (:require [fluree.db.query.fql-parser :refer [parse-db ns-lookup-pred-spec p->pred-config parse-where]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [clojure.set :as set]
            [fluree.db.query.analytical :as analytical]
            [fluree.db.query.schema :as schema]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try into? merge-into?]])
  #?(:clj (:import (fluree.db.flake Flake)))
  #?(:cljs (:require-macros [clojure.core])))

(declare flakes->res query)

(defn fuel-flake-transducer
  "Can sit in a flake pipeline and accumulate a count of 1 for every flake pulled.

  Inputs are:
  - fuel - volatile! that holds fuel counter
  - max-fuel - throw exception if @fuel ever exceeds this number

  To get final count, just deref fuel volatile when when where is complete."
  [fuel max-fuel]
  (fn [xf]
    (fn
      ([] (xf))                                             ;; transducer start
      ([result] (xf result))                                ;; transducer stop
      ([result flake]
       (vswap! fuel inc)
       (when (and max-fuel (> @fuel max-fuel))
         (throw (ex-info (str "Maximum query cost of " max-fuel " exceeded.")
                         {:status 400 :error :db/exceeded-cost})))
       (xf result flake)))))


(defn fuel-flakes-transducer
  "Can sit in a flake group pipeline and accumulate a count of 1 for every flake pulled.

  Supply with a volatile!

  To get final count, just deref volatile when when where is complete."
  [fuel max-fuel]
  (fn [xf]
    (fn
      ([] (xf))                                             ;; transducer start
      ([result] (xf result))                                ;; transducer stop
      ([result flakes]
       (vswap! fuel + (count flakes))
       (xf result flakes)))))


(defn add-fuel
  "Adds a n amount of fuel and will throw if max fuel exceeded."
  [fuel n max-fuel]
  (vswap! fuel + n)
  (when (and max-fuel (> @fuel max-fuel))
    (throw (ex-info (str "Maximum query cost of " max-fuel " exceeded.")
                    {:status 400 :error :db/exceeded-cost}))))


(defn wildcard-pred-spec
  "Just uses query cache to avoid constant lookups."
  [db cache p compact?]
  (or (get-in @cache [p compact?])
      (let [p-map (p->pred-config db p compact?)]
        (vswap! cache assoc-in [p compact?] p-map)
        p-map)))

(defn compare-fn
  [a b]
  (if (string? a)
    (let [res (compare (str/upper-case a) (str/upper-case b))]
      (if (= res 0)
        (* -1 (compare a b))
        res))
    (compare a b)))

(defn sort-offset-and-limit-res
  "We only need to do this if there is an orderBy, otherwise limit and offset
  were performed in index-range."
  [sortPred sortOrder offset limit res]

  (if (vector? res)
    (cond->> res
             sortPred (sort-by #(get % sortPred) compare-fn)
             (= "DESC" sortOrder) (reverse)
             offset (drop offset)
             limit (take limit)) res))

(defn- add-pred
  ([db cache fuel max-fuel acc pred-spec ^Flake flake componentFollow? recur?]
   (add-pred db cache fuel max-fuel acc pred-spec ^Flake flake componentFollow? recur? {}))
  ([db cache fuel max-fuel acc pred-spec ^Flake flake componentFollow? recur? offset-map]
   (go-try
     (let [pred-spec  (if (and (:wildcard? pred-spec) (nil? (:as pred-spec)))
                        ;; nested 'refs' can be wildcard, but also have a pred-spec... so only get a default wildcard spec if we have no other spec
                        (wildcard-pred-spec db cache (.-p flake) (:compact? pred-spec))
                        pred-spec)
           pred-spec' (if (contains? pred-spec :componentFollow?)
                        pred-spec
                        (assoc pred-spec :componentFollow? componentFollow?))
           ;; TODO - I think we can eliminate the check below for fallbacks and ensure we always have an 'as' in every spec
           k          (or (:as pred-spec') (:name pred-spec') (:p pred-spec')) ;; use :as, then full pred name, then just p-id as backup
           {:keys [multi? ref? limit orderBy offset p]} pred-spec'
           [k-val offset-map] (cond
                                (and multi?
                                     offset
                                     (not= 0 offset)
                                     (not= 0 (get offset-map p)))
                                [nil
                                 (if (get offset-map p)
                                   (update offset-map p dec)
                                   (assoc offset-map p (dec offset)))]
                                (and multi?
                                     (not orderBy)
                                     (>= (count (get acc k)) limit))
                                [nil offset-map]

                                ;; have a sub-selection
                                (and (not recur?)
                                     (or (:select pred-spec') (:wildcard? pred-spec')))
                                (let [nested-select-spec (select-keys pred-spec' [:wildcard? :compact? :select])]
                                  [(<? (cond->> (<? (query-range/index-range db :spot = [(.-o flake)]))
                                                fuel (sequence (fuel-flake-transducer fuel max-fuel))
                                                true ((fn [n] (flakes->res db cache fuel max-fuel nested-select-spec n)))))
                                   offset-map])

                                ;; resolve tag
                                (:tag? pred-spec')
                                [(or (get @cache [(.-o flake) (:name pred-spec')])
                                     (let [res (<? (dbproto/-tag db (.-o flake) (:name pred-spec')))]
                                       (vswap! cache assoc [(.-o flake) (:name pred-spec')] res)
                                       res)) offset-map]

                                ; is a component, get children
                                (and componentFollow? (:component? pred-spec'))
                                (let [children (<? (query-range/index-range db :spot = [(.-o flake)] {:limit (:limit pred-spec')}))]
                                  (if (empty? children)
                                    [{"_id" (.-o flake)} offset-map] ;; no permission (empty results), so just return _id
                                    [(<? (cond->> children
                                                  fuel (sequence (fuel-flake-transducer fuel max-fuel))
                                                  true ((fn [n] (flakes->res db cache fuel max-fuel {:wildcard? true} n)))))
                                     offset-map]))

                                ;; if a ref, put out an {:_id ...}
                                ref?
                                [{"_id" (.-o flake)} offset-map]

                                ;; else just output value
                                :else
                                [(.-o flake) offset-map])]
       (cond
         (and (not (nil? k-val)) multi?)
         [(assoc acc k (conj (get acc k []) k-val)) offset-map]

         (not (nil? k-val))
         [(assoc acc k k-val) offset-map]

         :else
         [acc offset-map])))))


(defn full-select-spec
  "Resolves a full predicate select spec in case there are
  any namespace lookups (:ns-lookup) in the map that
  need to be resolved for this given subject."
  [db cache base-pred-spec subject-id]
  (let [coll-id (flake/sid->cid subject-id)]
    (or (get @cache [coll-id base-pred-spec])
        (let [lookup-specs (ns-lookup-pred-spec db coll-id (get-in base-pred-spec [:select :ns-lookup]))
              updated-spec (update base-pred-spec :select (fn [sel] (-> sel
                                                                        (assoc :pred-id (merge lookup-specs (:pred-id sel)))
                                                                        (dissoc :ns-lookup))))]
          (vswap! cache assoc [coll-id base-pred-spec] updated-spec)
          updated-spec))))


(defn- has-ns-lookups?
  "Returns true if the predicate spec has a sub-selection that requires a namespace lookup."
  [select-spec]
  (get-in select-spec [:select :ns-lookup]))


(defn- s
  [^Flake f]
  (.-s f))

(defn- o
  [^Flake f]
  (.-o f))


(defn resolve-reverse-refs
  "Resolves all reverse references into a result map."
  [db cache fuel max-fuel subject-id reverse-refs-specs]
  (go-try
    (loop [[n & r] reverse-refs-specs                       ;; loop through reverse refs
           acc nil]
      (if-not n
        acc
        (let [[pred-id pred-spec] n
              {:keys [offset limit as name p]} pred-spec
              sub-ids    (->> (<? (query-range/index-range db :opst = [subject-id pred-id]))
                              (map s)
                              (not-empty))
              _          (when (and sub-ids fuel) (add-fuel fuel (count sub-ids) max-fuel))
              sub-result (loop [[sid & r'] sub-ids
                                n    0
                                acc' []]
                           (cond
                             (or (not sid) (and limit (>= n limit)))
                             acc'

                             (and offset (< n offset))
                             (recur r' (inc n) acc')

                             :else
                             (let [sub-flakes    (<? (query-range/index-range db :spot = [sid]))
                                   sub-pred-spec (select-keys pred-spec [:wildcard? :compact? :select :limit])
                                   acc'*         (if (empty? sub-flakes)
                                                   acc'
                                                   (do
                                                     (when fuel (add-fuel fuel (count sub-flakes) max-fuel))
                                                     (conj acc' (<? (flakes->res db cache fuel max-fuel sub-pred-spec sub-flakes)))))]
                               (recur r' (inc n) acc'*))))]
          (recur r (assoc acc (or as name p) sub-result)))))))


(defn component-follow?
  [pred-spec select-spec]
  (cond (contains? pred-spec :componentFollow?)
        (:componentFollow? pred-spec)

        (not (nil? (:componentFollow? select-spec)))
        (:componentFollow? select-spec)

        (or (:component? pred-spec) (:wildcard? select-spec))
        true))


(defn select-spec->reverse-pred-specs
  [select-spec]
  (reduce (fn [acc spec]
            (let [key-spec (key spec)
                  val-spec (if (nil? (:componentFollow? (val spec)))
                             (assoc (val spec) :componentFollow? (:componentFollow? select-spec))
                             (val spec))]
              (assoc acc key-spec val-spec)))
          {} (get-in select-spec [:select :reverse])))


(defn- conjv
  "Like conj, but if collection is nil creates a new vector instead of list.
  Not built to handle variable arity values"
  [coll x]
  (if (nil? coll)
    (vector x)
    (conj coll x)))


(defn- recur-select-spec
  "For recursion, takes current select-spec and nests the recur predicate as a child, updating
  recur-depth and recur-seen values. Uses flake as the recursion flake being operated on."
  [select-spec ^Flake flake]
  (let [recur-subject (.-o flake)
        recur-pred    (.-p flake)
        {:keys [recur-seen recur-depth]} select-spec]
    (-> select-spec
        (assoc-in [:select :pred-id recur-pred] select-spec) ;; move current pred-spec to child in :select key for next recursion round
        (assoc-in [:select :pred-id recur-pred :recur-depth] (inc recur-depth))
        (assoc-in [:select :pred-id recur-pred :recur-seen] (conj recur-seen recur-subject))
        ;; only need inherited keys
        (select-keys [:select :componentFollow? :compact?]))))



;; TODO - reverse refs
(defn flake->recur
  ([db ^Flake flake select-spec acc fuel max-fuel cache]
   (go-try
     (let [recur-subject (.-o flake)                        ;; ref, so recur subject is the object of the incoming flake
           {:keys [multi? as recur recur-seen recur-depth limit]} select-spec ;; recur contains # with requested recursion depth
           seen?         (contains? recur-seen recur-subject) ;; subject has been seen before, stop recursion
           max-depth?    (> recur-depth recur)              ;; reached max depth
           sub-flakes    (cond->> (<? (query-range/index-range db :spot = [recur-subject]))
                                  fuel (sequence (fuel-flake-transducer fuel max-fuel)))
           stop?         (or seen? max-depth? (empty? sub-flakes))
           add-result    (if multi?
                           (fn [results as new-result]
                             (update results as conjv new-result))
                           (fn [results as new-result]
                             (assoc results as new-result)))]
       (if stop?
         acc
         (let [select-spec* (recur-select-spec select-spec flake)

               res          (<? (flakes->res db cache fuel max-fuel select-spec* sub-flakes))]
           (add-result acc as res)))))))


(defn flakes->res
  "Takes a sequence of flakes of the same subject and
  composes them into a map result based on the 'select' spec
  provided. Optionally, also follows components or recurs."
  [db cache fuel max-fuel base-select-spec flakes]
  (go-try
    (when (not-empty flakes)
      (let [top-level-subject (try*
                                (s (first flakes))
                                (catch* e
                                        (log/error e)
                                        (throw e)))
            select-spec       (if (has-ns-lookups? base-select-spec)
                                (full-select-spec db cache base-select-spec top-level-subject)
                                base-select-spec)
            base-acc          (if (or (:wildcard? select-spec) (:id? select-spec))
                                {"_id" top-level-subject}
                                {})
            acc+refs          (if (get-in select-spec [:select :reverse])
                                (->> (select-spec->reverse-pred-specs select-spec)
                                     (resolve-reverse-refs db cache fuel max-fuel (s (first flakes)))
                                     (<?)
                                     (merge base-acc))
                                base-acc)
            result            (loop [flakes     flakes
                                     acc        acc+refs
                                     offset-map {}]
                                (if (empty? flakes)
                                  acc
                                  (let [f                (first flakes)
                                        pred-spec        (get-in select-spec [:select :pred-id (.-p f)])
                                        componentFollow? (component-follow? pred-spec select-spec)
                                        [acc flakes' offset-map'] (cond
                                                                    (:recur pred-spec)
                                                                    [(<? (flake->recur db f pred-spec acc fuel max-fuel cache))
                                                                     (rest flakes) offset-map]

                                                                    pred-spec
                                                                    (let [[acc offset-map] (<? (add-pred db cache fuel max-fuel acc pred-spec f componentFollow? false offset-map))]
                                                                      [acc (rest flakes) offset-map])

                                                                    (:wildcard? select-spec)
                                                                    [(first (<? (add-pred db cache fuel max-fuel acc
                                                                                          select-spec f componentFollow? false)))
                                                                     (rest flakes)
                                                                     offset-map]

                                                                    (and (empty? (:select select-spec)) (:id? select-spec))
                                                                    [{"_id" (.-s f)} (rest flakes) offset-map]

                                                                    :else
                                                                    [acc (rest flakes) offset-map])
                                        acc              (assoc acc :_id (.-s f))]
                                    (recur flakes' acc offset-map'))))
            sort-preds        (reduce (fn [acc spec]
                                        (if (or (and (:multi? spec) (:orderBy spec))
                                                (and (:reverse? spec) (:orderBy spec)))
                                          (conj acc [(:as spec) (-> spec :orderBy :order) (-> spec :orderBy :predicate) (:limit spec)])
                                          acc)) [] (concat (-> select-spec :select :pred-id vals)
                                                           (-> select-spec :select :reverse vals)))
            res               (reduce (fn [acc [selectPred sortOrder sortPred limit]]
                                        (->> (get acc selectPred)
                                             (sort-offset-and-limit-res sortPred sortOrder 0 limit)
                                             (assoc acc selectPred)))
                                      result sort-preds)]
        res))))




;(defn flakes->res-xf
;  "Transducer for filling out a result from a sequence of
;  flakes all from the same subject."
;  [db cache fuel max-fuel select-spec]
;  (fn [xf]
;    (fn
;      ([] (xf))                                             ;; transducer start
;      ([result] (xf result))                                ;; transducer stop
;      ([result flakes]
;       (if-let [res (flakes->res db cache fuel max-fuel select-spec flakes)]
;         (xf result res)
;         ;; if no response, just return result which will include nothing in the result set
;         result)))))


;; TODO - use pipeline-async to do selects in parallel
(defn flake-select
  "Runs a select statement based on a sequence of flakes."
  ([db cache fuel max-fuel select-spec flakes] (flake-select db cache fuel max-fuel select-spec flakes nil nil))
  ([db cache fuel max-fuel select-spec flakes limit] (flake-select db cache fuel max-fuel select-spec flakes limit nil))
  ([db cache fuel max-fuel select-spec flakes limit offset]
   ;; for a flake select, we convert all the initial predicates
   ;; to their predicate id to allow for a quick lookup
   (go-try
     (let [xf            (comp (cond-> (partition-by s)
                                       fuel (comp (fuel-flakes-transducer fuel max-fuel))
                                       offset (comp (drop offset))
                                       limit (comp (take limit)))
                               (halt-when (fn [x] (and max-fuel (>= @fuel max-fuel)))))
           flakes-by-sub (sequence xf flakes)]
       ;; parallel processing - issues: (a) Exhaustion of Fuel will not stop work,
       ;;                               (b) Volatile is used for fuel, could have RACE conditions on multi-threaded
       ;;                                   platforms (Java) and under-report fuel. Assuming fuel is considered a
       ;;                                   'close estimate', should be OK.
       ;; TODO - can limit effect of (a) by processing just four or so simultaneously via a pipeline
       (->> flakes-by-sub
            (map #(flakes->res db cache fuel max-fuel select-spec %))
            (merge-into? [])
            (<?))

       ;; sequential processing - will be slower for larger queries, but negligible for small queries. Fuel will always be accurate.
       #_(loop [[sub-flakes & r] flakes-by-sub
                acc []]
           (if-not sub-flakes
             acc
             (let [res (<? (flakes->res db cache fuel max-fuel select-spec sub-flakes))]
               (recur r (conj acc res)))))))))

(defn subject-select
  "Like flake select, but takes a collection of subject ids which we
  then find collections of flakes for."
  ([db cache fuel max-fuel select-spec subjects] (subject-select db cache fuel max-fuel select-spec subjects nil nil))
  ([db cache fuel max-fuel select-spec subjects limit] (subject-select db cache fuel max-fuel select-spec subjects limit nil))
  ([db cache fuel max-fuel select-spec subjects limit offset]
   (go-try
     (loop [[s & r] subjects
            n   0
            acc []]
       (cond
         (or (nil? s) (and limit (> n limit)))
         acc

         (and offset (< n offset))
         (recur r (inc n) acc)

         :else
         (recur r (inc n)
                (conj acc (->> (<? (query-range/index-range db :spot = [s] {:limit limit}))
                               ((fn [n] (flakes->res db cache fuel max-fuel select-spec n)))
                               (<?)))))))))

(defn valid-where-predicate?
  [db p]
  (or (dbproto/-p-prop db :idx? p)
      (dbproto/-p-prop db :ref? p)
      (= :tag (dbproto/-p-prop db :type p))))

;; TODO - this needs to be made somewhat lazy, can pull entire DB easily
(defn- where-filter
  "Takes a where clause and returns subjects that match."
  ([db where-clause]
   (where-filter db where-clause nil))
  ([db where-clause default-collection]
   (go-try
     (let [[op* statements] (parse-where db where-clause default-collection)]
       (when (not-empty statements)
         (loop [[smt & r] statements
                acc #{}]
           (if-not smt
             acc
             (let [[p op match] smt
                   _    (when (not (valid-where-predicate? db p))
                          (throw (ex-info (str "Non-indexed predicates are not valid in where clause statements. Provided: " (dbproto/-p-prop db :name p))
                                          {:status 400
                                           :error  :db/invalid-query})))
                   subs (->> (condp identical? op           ;; TODO - apply .-s transducer to index-range once support is there
                               not= (concat (<? (query-range/index-range db :post > [p match] <= [p]))
                                            (<? (query-range/index-range db :post >= [p] < [p match])))
                               = (<? (query-range/index-range db :post = [p match]))
                               > (<? (query-range/index-range db :post > [p match] <= [p]))
                               >= (<? (query-range/index-range db :post >= [p match] <= [p]))
                               < (<? (query-range/index-range db :post >= [p] < [p match]))
                               <= (<? (query-range/index-range db :post >= [p] <= [p match])) ())
                             (map s))
                   acc* (case op*
                          :or (into acc subs)
                          :and (set/intersection acc (into #{} subs)))]
               (if (and (= :and op*) (empty? acc*))
                 acc*
                 (recur r acc*))))))))))

(defn order-offset-and-limit-results
  "Order By can be:
    - Single variable, ?favNums
    - Two-tuple,  [ASC, ?favNums]"
  [orderBy {:keys [headers tuples] :as res} offset limit]
  (let [[order var] orderBy
        indexOfFind (or (util/index-of headers (symbol var)) -1)
        tuples      (if (<= 0 indexOfFind)
                      (cond->> (sort-by #(nth % indexOfFind) compare-fn tuples)
                               (= "DESC" order) reverse
                               offset (drop offset)
                               limit (take limit)) tuples)]
    {:headers headers :tuples tuples}))

(defn parse-map [x valid-var]
  (let [_             (when-not (= 1 (count (keys x)))
                        (throw (ex-info (str "Invalid aggregate selection, provided: " x)
                                        {:status 400 :error :db/invalid-query})))
        var-as-symbol (-> x keys first symbol)
        _             (when-not (valid-var var-as-symbol)
                        (throw (ex-info (str "Invalid select variable in aggregate select, provided: " x)
                                        {:status 400 :error :db/invalid-query})))]
    {:variable  var-as-symbol
     :selection (-> x vals first)}))

(defn parse-select
  [vars interim-vars select-smt]
  (let [_        (or (every? #(or (string? %) (map? %)) select-smt)
                     (throw (ex-info (str "Invalid select statement. Every selection must be a string or map. Provided: " select-smt) {:status 400 :error :db/invalid-query})))
        vars     (set vars)
        all-vars (set (concat vars (keys interim-vars)))]
    (map (fn [select]
           (let [var-symbol (if (map? select) nil (-> select symbol))]
             (cond (vars var-symbol) {:variable var-symbol}
                   (analytical/aggregate? select) (analytical/parse-aggregate select vars)
                   (map? select) (parse-map select all-vars)
                   (get interim-vars var-symbol) {:value (get interim-vars var-symbol)}
                   :else (throw (ex-info (str "Invalid select in statement, provided: " select)
                                         {:status 400 :error :db/invalid-query}))))) select-smt)))

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

(defn format-tuple
  [functionArray tuple]
  (async/go-loop [[function & r] functionArray
                  tuples-res []]
    (if function (let [result (<? (function tuple))]
                   (recur r (conj tuples-res result)))
                 tuples-res)))

(defn get-header-idx
  [headers select]
  (cond (:as select)
        (util/index-of headers (:as select))

        (:code select)
        (util/index-of (map str headers) (-> select :code str))

        (:variable select)
        (util/index-of headers (:variable select))))

(defn format-filter-tuples
  [db tuples {:keys [prettyPrint select inVector? expandMaps?] :as select-spec} headers vars opts]
  (go-try (let [pp            (when prettyPrint
                                (get-pretty-print-keys select))
                functionArray (if expandMaps? (map (fn [select]
                                                     (let [select-val (or (:as select) (:variable select))
                                                           idx        (when select-val (util/index-of headers select-val))
                                                           select-fn  (cond idx (fn [tuple] (nth tuple idx))
                                                                            (:value select) (fn [tuple] (:value select))
                                                                            (get vars select-val) (fn [tuple] (get vars select-val)))]
                                                       (if (:selection select)
                                                         (fn [tuple]
                                                           (go-try (when (select-fn tuple)
                                                                     (or (<? (query db {:selectOne (:selection select)
                                                                                        :from      (select-fn tuple)
                                                                                        :opts      opts}))
                                                                         {:_id (select-fn tuple)}))))
                                                         (fn [tuple]
                                                           (go-try (select-fn tuple)))))) select)
                                              (map (fn [select]
                                                     (if-let [val (:value select)]
                                                       (fn [x] val)
                                                       (let [idx (get-header-idx headers select)]
                                                         (fn [tuple]
                                                           (nth tuple idx))))) select))]
            (if expandMaps? (<? (async/go-loop [[tuple & r] tuples
                                                res []]
                                  (if tuple (let [tuple-res  (<? (format-tuple functionArray tuple))
                                                  tuple-res' (cond pp (zipmap pp tuple-res)
                                                                   inVector? tuple-res
                                                                   :else (first tuple-res))]
                                              (recur r (conj res tuple-res'))) res)))
                            (map (fn [tuple]
                                   (let [tuple-res (map #(% tuple) functionArray)]
                                     (cond pp (zipmap pp tuple-res)
                                           inVector? tuple-res
                                           :else (first tuple-res)))) tuples)))))

(defn- process-ad-hoc-group
  ([db res select-spec opts]
   (process-ad-hoc-group db res select-spec nil opts))
  ([db {:keys [vars] :as res} {:keys [aggregates orderBy offset groupBy select limit selectDistinct? inVector? prettyPrint] :as select-spec} group-limit opts]
   (go-try (if
             (and aggregates (= 1 (count select)))          ;; only aggregate
             (let [res  (second (analytical/calculate-aggregate res (first aggregates)))
                   res' (if prettyPrint
                          {(-> select first :as str (subs 1)) res}
                          res)]
               (if inVector? [res'] res'))

             (let [res+agg (if aggregates (analytical/add-aggregate-cols res aggregates) res)
                   offset  (if groupBy 0 offset)
                   {:keys [headers tuples]} (if orderBy
                                              (order-offset-and-limit-results orderBy res+agg offset group-limit)
                                              res+agg)
                   res     (<? (format-filter-tuples db tuples select-spec headers vars (dissoc opts :limit :offset :orderBy :groupBy)))]
               ;; TODO - drop unused columns, and calculate distinct before resolving all vals
               (if selectDistinct?
                 (->> (into #{} res) (into []))
                 res))))))


(defn ad-hoc-group-by
  [{:keys [headers tuples] :as res} groupBy]
  (let [[inVector? groupBy] (cond (vector? groupBy) [true (map symbol groupBy)]
                                  (string? groupBy) [false [(symbol groupBy)]]
                                  :else (throw (ex-info
                                                 (str "Invalid groupBy clause, must be a string or vector. Provided: " groupBy)
                                                 {:status 400 :error :db/invalid-query})))
        group-idxs (map #(util/index-of headers %) groupBy)
        _          (when (some nil? group-idxs)
                     (throw (ex-info
                              (str "Invalid groupBy clause - are all groupBy vars declared in the where clause. Provided: " groupBy)
                              {:status 400 :error :db/invalid-query})))]
    (reduce
      (fn [res tuple]
        (let [k  (map #(nth tuple %) group-idxs)
              k' (if inVector? (into [] k) (first k))
              v  tuple]
          (assoc res k' (conj (get res k' []) v))))
      {} tuples)))

(defn- build-order-fn
  [orderBy groupBy]
  (let [[sortDirection sortCriteria]  (if orderBy orderBy ["ASC" groupBy])]
    (cond
      (= sortCriteria groupBy)
      (if (= sortDirection "DESC")
        (fn [x y] (* -1 (compare-fn x y)))
        compare-fn)

      (and (coll? groupBy) (string? sortCriteria))
      (let [orderByIdx     (util/index-of groupBy sortCriteria)]
        (if (= "DESC" sortDirection)
          (fn [x y] (* -1 (compare-fn (nth x orderByIdx) (nth y orderByIdx))))
          (fn [x y] (compare-fn (nth x orderByIdx) (nth y orderByIdx)))))

      :else nil)))

(defn process-ad-hoc-res
  [db
   {:keys [headers vars] :as res}
   {:keys [groupBy orderBy limit selectOne? selectDistinct? inVector? offset] :as select-spec}
   opts]
  (go-try (if groupBy
            (let [order-fn  (build-order-fn orderBy groupBy)
                  group-map (cond->> (ad-hoc-group-by res groupBy)
                                     order-fn (into (sorted-map-by order-fn)))]
              (if selectOne?
                (let [k (first (keys group-map))
                      v (<? (process-ad-hoc-group db {:headers headers
                                                      :vars    vars
                                                      :tuples  (first (vals group-map))} select-spec limit opts))]
                  {k v})
                ; loop through map of groups
                (loop [[group-key & rest-keys] (keys group-map)
                       [group & rest-groups] (vals group-map)
                       limit      (if (= 0 limit) nil limit) ; limit of 0 is ALL
                       offset     (or offset 0)
                       acc        {}]
                  (let [group-count (count group)
                        group-as-res {:headers headers :vars vars :tuples group}]
                    (cond
                      ;? process all groups
                      (nil? group) acc

                      ;? exceeded limit
                      (and limit (< limit 1)) acc

                      ;? last item in this group is BEFORE offset - skip
                      (>= offset group-count)
                      (recur rest-keys
                             rest-groups
                             limit
                             (if selectDistinct? (- offset 1) (- offset group-count))
                             acc)

                      :else
                      ; 1) set orderBy to nil so order-offset-and-limit-results is not executed
                      ; 2) set offset to 0 so a drop is not performed
                      ; 3) then call process-ad-hoc-group
                      (-> (cond->> (<? (process-ad-hoc-group
                                         db
                                         group-as-res
                                         (assoc select-spec :orderBy nil :offset 0 :limit 0)
                                         (assoc opts :offset 0 :limit 0)))
                                   (< 0 offset) (drop offset)
                                   (and limit (< 0 limit)) (take limit))
                          (as-> res' (recur rest-keys
                                            rest-groups
                                            (when-not (nil? limit) (- limit (count res')))
                                            (cond
                                              (<= offset 0) 0
                                              selectDistinct? (- offset 1)
                                              :else (- offset (- group-count (count res'))))
                                            (if (or (nil? res') (empty? res')) acc (assoc acc group-key res'))) )))))))
            ; no group by
            (let [limit (if selectOne? 1 limit)
                  res   (<? (process-ad-hoc-group db res select-spec limit opts))]
              (cond (not (coll? res)) (if inVector? [res] res)
                    selectOne? (first res)
                    selectDistinct? (distinct res)
                    :else res)))))


(defn get-ad-hoc-select-spec
  [headers vars {:keys [selectOne select selectDistinct selectReduced]} opts]
  (let [select-smt    (or selectOne select selectDistinct selectReduced)
        inVector?     (vector? select-smt)
        select-smt    (if inVector? select-smt [select-smt])
        parsed-select (parse-select headers vars select-smt)
        aggregates    (filter #(contains? % :code) parsed-select)
        expandMap?    (some #(contains? % :selection) parsed-select)
        aggregates    (if (empty? aggregates) nil aggregates)
        orderBy       (when-let [orderBy (:orderBy opts)]
                        (if (or (string? orderBy) (and (vector? orderBy) (#{"DESC" "ASC"} (first orderBy))))
                          (if (vector? orderBy) orderBy ["ASC" orderBy])
                          (throw (ex-info (str "Invalid orderBy clause, must by variable or two-tuple formatted ['ASC' or 'DESC', var]. Provided: " orderBy)
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

(defn construct-triples
  [{:keys [construct] :as query-map} {:keys [headers tuples] :as where-result}]
  (let [[fn1 fn2 fn3] (map (fn [construct-item]
                             (if-let [index-of (util/index-of headers (symbol construct-item))]
                               (fn [row] (nth row index-of))
                               (fn [row] construct-item))) construct)]
    (map (fn [res]
           [(fn1 res) (fn2 res) (fn3 res)])

         tuples)))

(defn- ad-hoc-query
  [db fuel max-fuel query-map opts]
  (go-try
    (let [where-result (<? (analytical/q query-map fuel max-fuel db opts))]
      (cond (util/exception? where-result)
            where-result

            ;(:construct query-map)
            ;(construct-triples query-map where-result)

            :else
            (let [select-spec (get-ad-hoc-select-spec (:headers where-result) (:vars where-result)
                                                      query-map opts)]
              (<? (process-ad-hoc-res db where-result select-spec opts)))))))

(defn query
  "Returns core async channel with results"
  [db query-map]
  (let [{:keys [select selectOne selectDistinct where from limit offset component orderBy groupBy prettyPrint opts]} query-map
        opts' (cond-> (merge {:limit   limit :offset (or offset 0) :component component
                              :orderBy orderBy :groupBy groupBy :prettyPrint prettyPrint}
                             opts)
                      selectOne (assoc :limit 1))]
    (if #?(:clj (:cache opts') :cljs false)
      ;; handle caching - TODO - if a cache value exists, should max-fuel still be checked and throw if not enough?
      (let [oc (get-in db [:conn :object-cache])]
        ;; object cache takes (a) key and (b) fn to retrieve value if null
        (oc [:query (:block db) (dissoc query-map :opts) (dissoc opts' :fuel :max-fuel) (:auth db)]
            (fn [_]
              (let [pc (async/promise-chan)]
                (async/go
                  (let [res (async/<! (query db (assoc-in query-map [:opts :cache] false)))]
                    (async/put! pc res)))
                pc))))
      (let [max-fuel (:max-fuel opts')
            fuel     (or (:fuel opts)                       ;; :fuel volatile! can be provided upstream
                         (when (or max-fuel (:meta opts))
                           (volatile! 0)))]
        (if (sequential? where)
          ;; ad-hoc query
          (ad-hoc-query db fuel max-fuel query-map opts')
          ;; all other queries
          (go-try
            (let [select-smt   (or select selectOne selectDistinct
                                   (throw (ex-info "Query missing :select or :selectOne." {:status 400 :error :db/invalid-query})))
                  {:keys [orderBy limit component offset]} opts'
                  select-spec  (parse-db db select-smt opts')
                  select-spec' (if (not (nil? component))
                                 (assoc select-spec :componentFollow? component)
                                 select-spec)
                  cache        (volatile! {})
                  [sortPred sortOrder] (if orderBy (cond (vector? orderBy) [(second orderBy) (first orderBy)]
                                                         (string? orderBy) [orderBy "ASC"]
                                                         :else [nil nil])
                                                   [nil nil])
                  result       (cond
                                 (string? where)
                                 (let [default-collection (when (string? from) from)
                                       subjects           (<? (where-filter db where default-collection))]
                                   (<? (subject-select db cache fuel max-fuel select-spec'
                                                       subjects (if orderBy nil limit) (if orderBy nil offset))))

                                 ;; predicate-based query
                                 (and (string? from) (str/includes? #?(:clj from :cljs (str from)) "/"))
                                 (let [xf       (cond-> (map s)
                                                        fuel (comp (fuel-flake-transducer fuel max-fuel))
                                                        true (comp (distinct)))
                                       opts     (if orderBy {} {:limit limit :offset offset})
                                       subjects (->> (<? (query-range/index-range db :psot = [from] opts))
                                                     (sequence xf))]
                                   (<? (subject-select db cache fuel max-fuel select-spec' subjects limit)))


                                 ;; collection-based query -> _block or _tx
                                 (and (string? from) (#{"_block" "_tx"} from))
                                 (let [opts   (if orderBy {} {:limit limit :offset offset})
                                       flakes (<? (query-range/_block-or_tx-collection db opts))]
                                   (<? (flake-select db cache fuel max-fuel select-spec' flakes)))

                                 ;; collection-based query
                                 (string? from)
                                 (let [opts              (if orderBy {} {:limit limit :offset offset})
                                       collection-flakes (<? (query-range/collection db from opts))]
                                   (<? (flake-select db cache fuel max-fuel select-spec' collection-flakes)))

                                 ;; single subject _id provided
                                 (util/subj-ident? from)
                                 (let [subjects (some-> (<? (dbproto/-subid db from false))
                                                        (vector))
                                       res      (<? (subject-select db cache fuel max-fuel select-spec' subjects limit offset))]
                                   (when fuel (vswap! fuel inc)) ;; charge 1 for the lookup
                                   res)

                                 ;; multiple subject ids provided
                                 (and (sequential? from) (every? util/subj-ident? from))
                                 (let [subjects (loop [[n & r] from
                                                       acc []]
                                                  (if-not n
                                                    acc
                                                    (let [s    (if (int? n)
                                                                 n
                                                                 (do (when fuel (vswap! fuel inc))
                                                                     (<? (dbproto/-subid db n false))))
                                                          acc* (if s
                                                                 (conj acc s)
                                                                 acc)]
                                                      (recur r acc*))))
                                       subjects (into [] subjects)]
                                   (<? (subject-select db cache fuel max-fuel select-spec' subjects (if orderBy nil limit) (if orderBy nil offset))))

                                 :else
                                 (ex-info (str "Invalid 'from' in query:" (pr-str query-map))
                                          {:status 400 :error :db/invalid-query}))
                  res          (if sortPred
                                 (sort-offset-and-limit-res sortPred sortOrder offset limit result)
                                 result)]
              (if (and selectOne (coll? res) (not (util/exception? res)))
                (first res)
                res))))))))
