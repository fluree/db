(ns fluree.db.query.fql-resp
  (:require [fluree.db.query.fql-parser :refer [ns-lookup-pred-spec p->pred-config]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [fluree.db.query.range :as query-range]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try merge-into?]])
  (:refer-clojure :exclude [vswap!])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(declare flakes->res)

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

(defn- has-ns-lookups?
  "Returns true if the predicate spec has a sub-selection that requires a namespace lookup."
  [select-spec]
  (get-in select-spec [:select :ns-lookup]))


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


(defn select-spec->reverse-pred-specs
  [select-spec]
  (reduce (fn [acc spec]
            (let [key-spec (key spec)
                  val-spec (if (nil? (:componentFollow? (val spec)))
                             (assoc (val spec) :componentFollow? (:componentFollow? select-spec))
                             (val spec))]
              (assoc acc key-spec val-spec)))
          {} (get-in select-spec [:select :reverse])))


(defn add-fuel
  "Adds a n amount of fuel and will throw if max fuel exceeded."
  [fuel n max-fuel]
  (vswap! fuel + n)
  (when (and max-fuel (> @fuel max-fuel))
    (throw (ex-info (str "Maximum query cost of " max-fuel " exceeded.")
                    {:status 400 :error :db/exceeded-cost}))))

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
                              (map flake/s)
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

(defn- recur-select-spec
  "For recursion, takes current select-spec and nests the recur predicate as a child, updating
  recur-depth and recur-seen values. Uses flake as the recursion flake being operated on."
  [select-spec flake]
  (let [recur-subject (flake/o flake)
        recur-pred    (flake/p flake)
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
        (loop [[flake & r] flakes
               i   0
               acc []]
          (if (or (not flake) (and limit (< i limit)))
            (cond (empty? acc) results
                  multi? (assoc results as acc)
                  :else (assoc results as (first acc)))
            (let [recur-subject (flake/o flake)                 ;; ref, so recur subject is the object of the incoming flake
                  seen?         (contains? recur-seen recur-subject) ;; subject has been seen before, stop recursion

                  sub-flakes    (cond->> (<? (query-range/index-range db :spot = [recur-subject]))
                                         fuel (sequence (fuel-flake-transducer fuel max-fuel)))
                  skip?         (or seen? (empty? sub-flakes))
                  select-spec*  (recur-select-spec select-spec flake)]
              (if skip?
                (recur r (inc i) acc)
                (recur r (inc i) (conj acc (<? (flakes->res db cache fuel max-fuel select-spec* sub-flakes))))))))))))


(defn wildcard-pred-spec
  "Just uses query cache to avoid constant lookups."
  [db cache p compact?]
  (or (get-in @cache [p compact?])
      (let [p-map (p->pred-config db p compact?)]
        (vswap! cache assoc-in [p compact?] p-map)
        p-map)))


(defn- add-pred
  "Adds a predicate to a select spec graph crawl. flakes input is a list of flakes
  all with the same subject and predicate values."
  ([db cache fuel max-fuel acc pred-spec flakes componentFollow? recur?]
   (add-pred db cache fuel max-fuel acc pred-spec flakes componentFollow? recur? {}))
  ([db cache fuel max-fuel acc pred-spec flakes componentFollow? recur? offset-map]
   (go-try
     (let [compact?   (:compact? pred-spec)                 ;retain original value
           pred-spec  (if (and (:wildcard? pred-spec) (nil? (:as pred-spec)))
                        ;; nested 'refs' can be wildcard, but also have a pred-spec... so only get a default wildcard spec if we have no other spec
                        (wildcard-pred-spec db cache (-> flakes first :p) (:compact? pred-spec))
                        pred-spec)
           pred-spec' (cond-> pred-spec
                              (not (contains? pred-spec :componentFollow?)) (assoc :componentFollow? componentFollow?)
                              (not (contains? pred-spec :compact?)) (assoc :compact? compact?))
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

                                ;; check if have hit limit of predicate spec
                                (and multi?
                                     (not orderBy)
                                     limit
                                     (>= (count (get acc k)) limit))
                                [nil offset-map]

                                ;; have a sub-selection
                                (and (not recur?)
                                     (or (:select pred-spec') (:wildcard? pred-spec')))
                                (let [nested-select-spec (select-keys pred-spec' [:wildcard? :compact? :select])]
                                  [(loop [[flake & r] flakes
                                          acc []]
                                     (if flake
                                       (let [sub-sel (<? (query-range/index-range db :spot = [(flake/o flake)]))
                                             res     (when (seq sub-sel)
                                                       (<? (flakes->res db cache fuel max-fuel nested-select-spec sub-sel)))]
                                         (when fuel (vswap! fuel + (count sub-sel)))
                                         (recur r (if (seq res)
                                                    (conj acc res)
                                                    acc)))
                                       acc))
                                   offset-map])

                                ;; resolve tag
                                (:tag? pred-spec')
                                [(loop [[flake & r] flakes
                                        acc []]
                                   (if flake
                                     (let [res (or (get @cache [(flake/o flake) (:name pred-spec')])
                                                   (let [res (<? (dbproto/-tag db (flake/o flake) (:name pred-spec')))]
                                                     (vswap! cache assoc [(flake/o flake) (:name pred-spec')] res)
                                                     res))]
                                       (recur r (if res (conj acc res) acc)))
                                     acc))
                                 offset-map]

                                ; is a component, get children
                                (and componentFollow? (:component? pred-spec'))
                                [(loop [[flake & r] flakes
                                        acc []]
                                   (if flake
                                     (let [children (<? (query-range/index-range db :spot = [(flake/o flake)] {:limit (:limit pred-spec')}))
                                           acc*     (if (empty? children)
                                                      (conj acc {:_id (flake/o flake)})
                                                      (conj acc (<? (flakes->res db cache fuel max-fuel {:wildcard? true :compact? compact?} children))))]
                                       (when fuel (vswap! fuel + (count children)))
                                       (recur r acc*))
                                     acc))
                                 offset-map]

                                ;; if a ref, put out an {:_id ...}
                                ref?
                                (if (true? (-> db :permissions :root?))
                                  [(mapv #(hash-map :_id (flake/o %)) flakes) offset-map]
                                  (loop [[f & r] flakes
                                         acc []]
                                    (if f
                                      (if (seq (<? (query-range/index-range db :spot = [(flake/o f)])))
                                        (recur r (conj acc {:_id (flake/o f)}))
                                        (recur r acc))
                                      [acc offset-map])))

                                ;; else just output value
                                :else
                                [(mapv #(flake/o %) flakes) offset-map])]
       (cond
         (empty? k-val) [acc offset-map]
         multi? [(assoc acc k k-val) offset-map]
         :else [(assoc acc k (first k-val)) offset-map])))))


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


(defn flakes->res
  "Takes a sequence of flakes of the same subject and
  composes them into a map result based on the 'select' spec
  provided. Optionally, also follows components or recurs."
  [db cache fuel max-fuel base-select-spec flakes]
  (go-try
    (when (not-empty flakes)
      (let [top-level-subject (try*
                                (flake/s (first flakes))
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
                                     (resolve-reverse-refs db cache fuel max-fuel (flake/s (first flakes)))
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
