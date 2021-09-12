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
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try into? merge-into?]]
            [fluree.db.constants :as const]
            [fluree.db.util.iri :as iri-util])
  (:refer-clojure :exclude [vswap!])
  #?(:clj (:import (fluree.db.flake Flake)))
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare flakes->res query basic-query)

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
  [db cache p context compact?]
  (or (get-in @cache [p compact?])
      (let [p-map (p->pred-config db p context compact?)]
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

(defn rdf-type->str
  "When rendering rdf:type properties, this returns the appropriate string based on the query's context."
  [db cache context ^Flake flake]
  (let [type-sid (.-o flake)]
    (if-let [iri (get-in db [:schema :pred type-sid :iri])]
      (let [compacted (iri-util/compact iri context)]
        (vswap! cache assoc type-sid compacted)
        compacted)
      {:_id type-sid})))

(defn- add-pred
  "Adds a predicate to a select spec graph crawl. flakes input is a list of flakes
  all with the same subject and predicate values."
  ([db cache fuel max-fuel acc pred-spec flakes componentFollow? recur?]
   (add-pred db cache fuel max-fuel acc pred-spec flakes componentFollow? recur? {}))
  ([db cache fuel max-fuel acc {:keys [context] :as pred-spec} flakes componentFollow? recur? offset-map]
   (go-try
     (let [compact?   (:compact? pred-spec)                 ;retain original value
           pred-spec  (if (and (:wildcard? pred-spec) (nil? (:as pred-spec)))
                        ;; nested 'refs' can be wildcard, but also have a pred-spec... so only get a default wildcard spec if we have no other spec
                        (wildcard-pred-spec db cache (-> flakes first :p) context compact?)
                        pred-spec)
           pred-spec' (cond-> pred-spec
                              (not (contains? pred-spec :componentFollow?)) (assoc :componentFollow? componentFollow?)
                              (not (contains? pred-spec :compact?)) (assoc :compact? compact?))
           {:keys [as multi? ref? limit orderBy offset p]} pred-spec'
           [k-val offset-map] (cond
                                (and multi?
                                     offset
                                     (not= 0 offset)
                                     (not= 0 (get offset-map p)))
                                [nil
                                 (if (get offset-map p)
                                   (update offset-map p dec)
                                   (assoc offset-map p (dec offset)))]

                                ;; check if have hit limit of predicate spec
                                (and multi?
                                     (not orderBy)
                                     limit
                                     (>= (count (get acc as)) limit))
                                [nil offset-map]

                                ;; have a sub-selection
                                (and (not recur?)
                                     (or (:select pred-spec') (:wildcard? pred-spec')))
                                (let [nested-select-spec (select-keys pred-spec' [:wildcard? :compact? :select])]
                                  [(loop [[^Flake flake & r] flakes
                                          acc []]
                                     (if flake
                                       (let [sub-sel (<? (query-range/index-range db :spot = [(.-o flake)]))]
                                         (when fuel (vswap! fuel + (count sub-sel)))
                                         (recur r (conj acc (<? (flakes->res db cache fuel max-fuel nested-select-spec sub-sel)))))
                                       acc))
                                   offset-map])

                                ;; resolve tag
                                (:tag? pred-spec')
                                [(loop [[^Flake flake & r] flakes
                                        acc []]
                                   (if flake
                                     (let [res (or (get @cache [(.-o flake) (:name pred-spec')])
                                                   (let [res (<? (dbproto/-tag db (.-o flake) (:name pred-spec')))]
                                                     (vswap! cache assoc [(.-o flake) (:name pred-spec')] res)
                                                     res))]
                                       (recur r (if res (conj acc res) acc)))
                                     acc))
                                 offset-map]

                                ; is a component, get children
                                (and componentFollow? (:component? pred-spec'))
                                [(loop [[^Flake flake & r] flakes
                                        acc []]
                                   (if flake
                                     (let [children (<? (query-range/index-range db :spot = [(.-o flake)] {:limit (:limit pred-spec')}))
                                           acc*     (if (empty? children)
                                                      (conj acc {:_id (.-o flake)})
                                                      (conj acc (<? (flakes->res db cache fuel max-fuel {:wildcard? true :compact? compact?} children))))]
                                       (when fuel (vswap! fuel + (count children)))
                                       (recur r acc*))
                                     acc))
                                 offset-map]

                                ;; if a ref, put out an {:_id ...}, if @type expand into IRI
                                ref?
                                (if (= const/$rdf:type (:p pred-spec'))
                                  [(mapv (partial rdf-type->str db cache context) flakes)
                                   offset-map]
                                  [(mapv #(hash-map :_id (.-o ^Flake %)) flakes) offset-map])

                                ;; @id value, so might need to shorten based on context
                                (= const/$iri p)
                                [(mapv #(iri-util/compact (.-o ^Flake %) context) flakes) offset-map]


                                ;; else just output value
                                :else
                                [(mapv #(.-o ^Flake %) flakes) offset-map])]
       (cond
         (empty? k-val) [acc offset-map]
         multi? [(assoc acc as k-val) offset-map]
         :else [(assoc acc as (first k-val)) offset-map])))))


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
              sub-ids (->> (<? (query-range/index-range db :opst = [subject-id pred-id]))
                           (map s)
                           (not-empty))
              _ (when (and sub-ids fuel) (add-fuel fuel (count sub-ids) max-fuel))
              sub-result (loop [[sid & r'] sub-ids
                                n 0
                                acc' []]
                           (cond
                             (or (not sid) (and limit (>= n limit)))
                             acc'

                             (and offset (< n offset))
                             (recur r' (inc n) acc')

                             :else
                             (let [sub-flakes (<? (query-range/index-range db :spot = [sid]))
                                   sub-pred-spec (select-keys pred-spec [:wildcard? :compact? :select :limit])
                                   acc'* (if (empty? sub-flakes)
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
        recur-pred (.-p flake)
        {:keys [recur-seen recur-depth]} select-spec]
    (-> select-spec
        (assoc-in [:select :pred-id recur-pred] select-spec) ;; move current pred-spec to child in :select key for next recursion round
        (assoc-in [:select :pred-id recur-pred :recur-depth] (inc recur-depth))
        (assoc-in [:select :pred-id recur-pred :recur-seen] (conj recur-seen recur-subject))
        ;; only need inherited keys
        (select-keys [:select :componentFollow? :compact?]))))



(defn flake->recur
  "Performs recursion on a select spec graph crawl when specified. flakes input is list
  of flakes all with the same subject and predicate values."
  [db flakes select-spec results fuel max-fuel cache]
  (go-try
    (let [{:keys [multi? as recur-seen recur-depth limit]} select-spec ;; recur contains # with requested recursion depth
          max-depth? (> recur-depth (:recur select-spec))]
      (if max-depth?
        results
        (loop [[^Flake flake & r] flakes
               i   0
               acc []]
          (if (or (not flake) (and limit (< i limit)))
            (cond (empty? acc) results
                  multi? (assoc results as acc)
                  :else (assoc results as (first acc)))
            (let [recur-subject (.-o flake)                 ;; ref, so recur subject is the object of the incoming flake
                  seen?         (contains? recur-seen recur-subject) ;; subject has been seen before, stop recursion

                  sub-flakes    (cond->> (<? (query-range/index-range db :spot = [recur-subject]))
                                         fuel (sequence (fuel-flake-transducer fuel max-fuel)))
                  skip?         (or seen? (empty? sub-flakes))
                  select-spec*  (recur-select-spec select-spec flake)]
              (if skip?
                (recur r (inc i) acc)
                (recur r (inc i) (conj acc (<? (flakes->res db cache fuel max-fuel select-spec* sub-flakes))))))))))))


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
                                {:_id top-level-subject}
                                {})
            acc+refs          (if (get-in select-spec [:select :reverse])
                                (->> (select-spec->reverse-pred-specs select-spec)
                                     (resolve-reverse-refs db cache fuel max-fuel (s (first flakes)))
                                     (<?)
                                     (merge base-acc))
                                base-acc)
            result            (loop [p-flakes   (partition-by :p flakes)
                                     acc        acc+refs
                                     offset-map {}]
                                (if (empty? p-flakes)
                                  acc
                                  (let [flakes           (first p-flakes)
                                        pred-spec        (get-in select-spec [:select :pred-id (-> flakes first :p)])
                                        componentFollow? (component-follow? pred-spec select-spec)
                                        [acc flakes' offset-map'] (cond
                                                                    (:recur pred-spec)
                                                                    [(<? (flake->recur db flakes pred-spec acc fuel max-fuel cache))
                                                                     (rest p-flakes) offset-map]

                                                                    pred-spec
                                                                    (let [[acc offset-map] (<? (add-pred db cache fuel max-fuel acc pred-spec flakes componentFollow? false offset-map))]
                                                                      [acc (rest p-flakes) offset-map])

                                                                    (:wildcard? select-spec)
                                                                    [(first (<? (add-pred db cache fuel max-fuel acc select-spec flakes componentFollow? false)))
                                                                     (rest p-flakes)
                                                                     offset-map]

                                                                    (and (empty? (:select select-spec)) (:id? select-spec))
                                                                    [{:_id (-> flakes first :s)} (rest p-flakes) offset-map]

                                                                    :else
                                                                    [acc (rest p-flakes) offset-map])
                                        acc*             (assoc acc :_id (-> flakes first :s))]
                                    (recur flakes' acc* offset-map'))))
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


;; TODO - use pipeline-async to do selects in parallel
(defn flake-select
  "Runs a select statement based on a sequence of flakes."
  ([db cache fuel max-fuel select-spec flakes] (flake-select db cache fuel max-fuel select-spec flakes nil nil))
  ([db cache fuel max-fuel select-spec flakes limit] (flake-select db cache fuel max-fuel select-spec flakes limit nil))
  ([db cache fuel max-fuel select-spec flakes limit offset]
   ;; for a flake select, we convert all the initial predicates
   ;; to their predicate id to allow for a quick lookup
   (go-try
     (let [xf (comp (cond-> (partition-by s)
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
            (<?))))))


(defn subject-select
  "Like flake select, but takes a collection of subject ids which we
  then find collections of flakes for."
  ([db cache fuel max-fuel select-spec subjects] (subject-select db cache fuel max-fuel select-spec subjects nil nil))
  ([db cache fuel max-fuel select-spec subjects limit] (subject-select db cache fuel max-fuel select-spec subjects limit nil))
  ([db cache fuel max-fuel select-spec subjects limit offset]
   (go-try
     (loop [[s & r] subjects
            n 0
            acc []]
       (cond
         (or (nil? s) (and limit (> n limit)))
         acc

         (and offset (< n offset))
         (recur r (inc n) acc)

         :else
         (let [flakes (<? (query-range/index-range db :spot = [s]))]
           (when fuel (vswap! fuel + (count flakes)))
           (recur r (inc n) (conj acc (<? (flakes->res db cache fuel max-fuel select-spec flakes))))))))))


(defn valid-where-predicate?
  [db p]
  (or (dbproto/-p-prop db :idx? p)
      (dbproto/-p-prop db :ref? p)
      (= :tag (dbproto/-p-prop db :type p))))

;; TODO - this needs to be made somewhat lazy, can pull entire DB easily
(defn- where-filter
  "Takes a where clause and returns subjects that match."
  [db where-clause default-collection]
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
                  subs (->> (condp identical? op            ;; TODO - apply .-s transducer to index-range once support is there
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
                (recur r acc*)))))))))


(defn parse-map
  [x valid-var]
  (let [_ (when-not (= 1 (count (keys x)))
            (throw (ex-info (str "Invalid aggregate selection, provided: " x)
                            {:status 400 :error :db/invalid-query})))
        var-as-symbol (-> x keys first symbol)
        _ (when-not (valid-var var-as-symbol)
            (throw (ex-info (str "Invalid select variable in aggregate select, provided: " x)
                            {:status 400 :error :db/invalid-query})))]
    {:variable  var-as-symbol
     :selection (-> x vals first)}))

(defn parse-select
  [vars interim-vars select-smt]
  (let [_ (or (every? #(or (string? %) (map? %)) select-smt)
              (throw (ex-info (str "Invalid select statement. Every selection must be a string or map. Provided: " select-smt) {:status 400 :error :db/invalid-query})))
        vars (set vars)
        all-vars (set (concat vars (keys interim-vars)))]
    (map (fn [select]
           (let [var-symbol (if (map? select) nil (-> select symbol))]
             (cond (vars var-symbol) {:variable var-symbol}
                   (analytical/aggregate? select) (analytical/parse-aggregate select vars)
                   (map? select) (parse-map select all-vars)
                   (get interim-vars var-symbol) {:value (get interim-vars var-symbol)}
                   :else (throw (ex-info (str "Invalid select in statement, provided: " select)
                                         {:status 400 :error :db/invalid-query})))))
         select-smt)))

(defn get-pretty-print-keys
  [select]
  (let [vars (map (fn [select]
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
  [db query-opts context fuel max-fuel tuple-result [tuple-index query-map]]
  ;; ignore any nil values in tuple-result at idx position (i.e. can happen with optional/left outer joins)
  (when-let [_id (get tuple-result tuple-index)]
    (async/go
      [tuple-index (<? (basic-query db fuel max-fuel {:selectOne query-map :from _id :context context} query-opts))])))

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
  [select context pp-keys single-result? db fuel max-fuel opts parallelism tuples-res]
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
                                  (keep #(expand-map db opts* context fuel max-fuel tuple-res' %)) ;; returns async channels, executes expandmap query
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
        select-val (or as variable)
        idx (get-header-idx headers select)
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
        comparator (if (= "DESC" order) (fn [a b] (compare b a)) compare)
        compare-idx (util/index-of headers (symbol var))
        no-case? (and (string? option) (= "NOCASE" (str/upper-case option)))
        keyfn (if no-case?
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
  ([db fuel max-fuel {:keys [vars] :as res} {:keys [aggregates orderBy offset groupBy select expandMaps? selectDistinct? inVector? prettyPrint context] :as select-spec} group-limit opts]
   (go-try (if
             (and aggregates (= 1 (count select)))          ;; only aggregate
             (let [res  (second (analytical/calculate-aggregate res (first aggregates)))
                   res' (if prettyPrint
                          {(-> select first :as str (subs 1)) res}
                          res)]
               (if inVector? [res'] res'))

             (let [{:keys [headers tuples]} (if aggregates (analytical/add-aggregate-cols res aggregates) res)
                   offset' (when (and offset (not groupBy)) ;; groupBy results cannot be offset (not sure why! was there)
                             offset)
                   single-result? (and (not prettyPrint) (not inVector?))
                   pp-keys (when prettyPrint (get-pretty-print-keys select))
                   xf (apply comp
                             (cond-> [(map (select-tuples-fn headers vars select))] ;; a function that formats a :where result tuple to specified :select clause
                                     single-result? (conj (map first))
                                     selectDistinct? (conj (fuel-flake-transducer fuel max-fuel 5)) ;; distinct charges 5 per item touched
                                     selectDistinct? (conj (distinct))
                                     offset' (conj (drop offset'))
                                     group-limit (conj (take group-limit))
                                     prettyPrint (conj (map #(zipmap (get-pretty-print-keys select) %)))))
                   result (cond->> tuples
                                   orderBy (order-result-tuples fuel max-fuel headers orderBy)
                                   true (into [] xf))]
               (if expandMaps?
                 (<? (pipeline-expandmaps-result select context pp-keys single-result? db fuel max-fuel opts 8 result))
                 result))))))


(defn ad-hoc-group-by
  [{:keys [headers tuples] :as res} groupBy]
  (let [[inVector? groupBy] (cond (vector? groupBy) [true (map symbol groupBy)]
                                  (string? groupBy) [false [(symbol groupBy)]]
                                  :else (throw (ex-info
                                                 (str "Invalid groupBy clause, must be a string or vector. Provided: " groupBy)
                                                 {:status 400 :error :db/invalid-query})))
        group-idxs (map #(util/index-of headers %) groupBy)
        _ (when (some nil? group-idxs)
            (throw (ex-info
                     (str "Invalid groupBy clause - are all groupBy vars declared in the where clause. Provided: " groupBy)
                     {:status 400 :error :db/invalid-query})))]
    (reduce
      (fn [res tuple]
        (let [k (map #(nth tuple %) group-idxs)
              k' (if inVector? (into [] k) (first k))
              v tuple]
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

(defn process-ad-hoc-res
  [db fuel max-fuel
   {:keys [headers vars] :as res}
   {:keys [groupBy orderBy limit selectOne? selectDistinct? inVector? offset] :as select-spec}
   opts]
  (go-try (if groupBy
            (let [order-fn (build-order-fn orderBy groupBy)
                  group-map (cond->> (ad-hoc-group-by res groupBy)
                                     order-fn (into (sorted-map-by order-fn)))]
              (if selectOne?
                (let [k (first (keys group-map))
                      v (<? (process-ad-hoc-group db fuel max-fuel
                                                  {:headers headers
                                                   :vars    vars
                                                   :tuples  (first (vals group-map))} select-spec limit opts))]
                  {k v})
                ; loop through map of groups
                (loop [[group-key & rest-keys] (keys group-map)
                       [group & rest-groups] (vals group-map)
                       limit (if (= 0 limit) nil limit)     ; limit of 0 is ALL
                       offset (or offset 0)
                       acc {}]
                  (let [group-count (count group)
                        group-as-res {:headers headers :vars vars :tuples group}]
                    (cond
                      ;? processed all groups
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
                      ; 4) May return a singleton aggregate
                      (let [res (<? (process-ad-hoc-group
                                      db fuel max-fuel
                                      group-as-res
                                      (assoc select-spec :orderBy nil :offset 0 :limit nil)
                                      (assoc opts :offset 0 :limit nil)))]
                        (if (and (coll? res) (seq res))
                          ; non-empty collection
                          (-> (cond->> res
                                       (< 0 offset) (drop offset)
                                       (and limit (< 0 limit)) (take limit))
                              (as-> res' (recur rest-keys
                                                rest-groups
                                                (when-not (nil? limit) (- limit (count res')))
                                                (cond
                                                  (<= offset 0) 0
                                                  selectDistinct? (- offset 1)
                                                  :else (- offset (- group-count (count res'))))
                                                (if (or (nil? res') (empty? res')) acc (assoc acc group-key res')))))
                          ; empty collection (?) or singleton aggregate
                          (recur rest-keys
                                 rest-groups
                                 (when-not (nil? limit) (- limit 1))
                                 (if (<= offset 0) 0 (- offset 1))
                                 (assoc acc group-key res)))))))))
            ; no group by
            (let [limit (if selectOne? 1 limit)
                  res (<? (process-ad-hoc-group db fuel max-fuel res select-spec limit opts))]
              (cond (not (coll? res)) (if inVector? [res] res)
                    selectOne? (first res)
                    :else res)))))


(defn get-ad-hoc-select-spec
  [headers vars {:keys [selectOne select selectDistinct selectReduced context]} opts]
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
                          (throw (ex-info (str "Invalid orderBy clause, must be variable or two-tuple formatted ['ASC' or 'DESC', var]. Provided: " orderBy)
                                          {:status 400
                                           :error  :db/invalid-query}))))]
    {:select          parsed-select
     :context         context
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
              (<? (process-ad-hoc-res db fuel max-fuel where-result select-spec opts)))))))


(defn- basic-query-multi-subject
  "Returns list of resolvable subject ids when provided a list of identities"
  [db identities]
  (go-try
    (loop [[ident & r] identities
           acc []]
      (if-not ident
        acc
        (let [s (<? (dbproto/-subid db ident false))]
          (recur r (if s (conj acc s) acc)))))))


(defn- basic-query-type
  "Returns a two-tuple of the type of basic query to be executed, and secondly the
  subject id or ids to be searched for using that particular query type"
  [db from where fuel]
  (go-try
    (cond
      ;; legacy string where clause - should be the first thing we deprecate and use ad-hoc instead
      (string? where)
      (let [default-collection (when (string? from) from)
            subjects           (<? (where-filter db where default-collection))]
        (when fuel (vswap! fuel + (count subjects)))
        [:multi-subject subjects])

      ;; string could be a predicate, collection, or @id subject
      (string? from)
      (if (#{"_block" "_tx"} from)
        [:tx-collection nil]
        (if-let [cid (dbproto/-c-prop db :id from)]
          [:collection cid]
          (if-let [pid (dbproto/-p-prop db :id from)]
            [:predicate pid]
            ;; assume this is an @id iri, lookup
            (let [sid (<? (dbproto/-subid db from false))]
              (when fuel (vswap! fuel + (count from)))
              [:subject sid]))))

      ;; single subject ident
      (util/subj-ident? from)
      (let [sid (<? (dbproto/-subid db from false))]
        (when fuel (vswap! fuel inc))
        [:subject sid])

      ;; multi-subject
      (and (sequential? from) (every? util/subj-ident? from))
      (let [subjects (<? (basic-query-multi-subject db from))]
        (when fuel (vswap! fuel + (count from)))
        [:multi-subject subjects])

      :else
      (ex-info (str "Invalid 'from' or 'where' in query:" (or from where))
               {:status 400 :error :db/invalid-query}))))


(defn- basic-query-orderBy
  "Handles ordering basic query results."
  [orderBy offset limit results]
  (let [[sortPred sortOrder] (cond (vector? orderBy) [(second orderBy) (first orderBy)]
                                   (string? orderBy) [orderBy "ASC"])]
    (sort-offset-and-limit-res sortPred sortOrder offset limit results)))


(defn basic-query
  "Executes a basic query (not ad-hoc/advanced). Eventually we will probably deprecate basic query.
  A basic query has a 'from' key in the query, whereas an ad-hoc query has a 'where' key in the query
  with a vector value."
  [db fuel max-fuel query-map opts]
  (go-try
    (let [{:keys [select selectOne selectDistinct where from context]} query-map
          select-smt   (or select selectOne selectDistinct
                           (throw (ex-info "Query missing select, selectOne or selectDistinct." {:status 400 :error :db/invalid-query})))
          {:keys [orderBy limit component offset]} opts
          select-spec  (parse-db db select-smt context opts)
          select-spec' (if (not (nil? component))
                         (assoc select-spec :componentFollow? component)
                         select-spec)
          cache        (volatile! {})
          [q-type _ids] (<? (basic-query-type db from where fuel))
          opts*        (if orderBy
                         (dissoc opts :limit :offset)
                         opts)
          q-result     (case q-type
                         :collection (let [flakes (<? (query-range/collection db from opts*))]
                                       (<? (flake-select db cache fuel max-fuel select-spec' flakes)))
                         :tx-collection (let [flakes (<? (query-range/_block-or_tx-collection db opts*))]
                                          (<? (flake-select db cache fuel max-fuel select-spec' flakes)))
                         :predicate (let [xf       (cond-> (map s)
                                                           fuel (comp (fuel-flake-transducer fuel max-fuel))
                                                           true (comp (distinct)))
                                          subjects (->> (<? (query-range/index-range db :psot = [from] opts*))
                                                        (sequence xf))]
                                      (<? (subject-select db cache fuel max-fuel select-spec' subjects limit)))
                         :subject (when _ids
                                    (<? (subject-select db cache fuel max-fuel select-spec' [_ids] limit offset)))
                         :multi-subject (<? (subject-select db cache fuel max-fuel select-spec' _ids (:limit opts*) (:offset opts*))))
          take-one?    (and selectOne (coll? q-result) (not (util/exception? q-result)))]
      (cond->> q-result
               orderBy (basic-query-orderBy orderBy offset limit)
               take-one? first))))


(defn query
  "Returns core async channel with results or exception"
  [db query-map]
  (log/debug "Running query:" (pr-str query-map))
  (let [{:keys [select selectOne selectDistinct where from limit offset
                component orderBy groupBy prettyPrint context opts]} query-map
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
      (let [max-fuel   (:max-fuel opts')
            fuel       (or (:fuel opts)                     ;; :fuel volatile! can be provided upstream
                           (when (or max-fuel (:meta opts))
                             (volatile! 0)))
            query-map* (assoc query-map :context (iri-util/query-context context db))]
        (if (sequential? where)
          ;; ad-hoc query
          (ad-hoc-query db fuel max-fuel query-map* opts')
          ;; all other queries
          (basic-query db fuel max-fuel query-map* opts'))))))
