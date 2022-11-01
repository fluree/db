(ns fluree.db.query.analytical
  (:require [clojure.set :as set]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.flake :as flake]
            [fluree.db.query.analytical-wikidata :as wikidata]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.query.union :as union]
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true]
            #?(:cljs [cljs.reader])
            [fluree.db.dbproto :as dbproto]
            [fluree.db.query.analytical-parse :as parse])
  #?(:clj (:import (java.io Closeable))))

#?(:clj (set! *warn-on-reflection* true))

(defn variable? [form]
  (when (and (or (string? form) (keyword? form) (symbol? form)) (= (first (name form)) \?))
    (symbol form)))

(defn escaped-string?
  [form]
  (and (string? form)
       (= (first (name form)) \")
       (= (last (name form)) \")))

(defn safe-read-string
  [string]
  (try
    (#?(:clj read-string :cljs cljs.reader/read-string) string)
    (catch #?(:clj Exception :cljs :default) e string)))

(defn get-vars
  [filter-code]
  (some #(or (variable? %) (when (coll? %) (get-vars %))) filter-code))

(defn clause->rel
  "Given any interm-vars, such as {?article 351843720901583}
  and an fdb clause, such as  [\"?article\", \"articles/leadInstitutionOrg\", \"?org\"],


  Returns a map with the following keys:

  - search - a vector that will be passed to query-range/search, i.e. [ nil \"articles/leadInstitutionOrg\" nil ]
  - rel - a map with any variables (that are not present in interm-vars) and their idx, i.e. {?org 2}
  - opts - search opts, currently recur, if the predicate is recurred, and object-fn, if there is an object function.
 "
  [db interm-vars clause]
  (reduce-kv (fn [acc idx key]
               (println "key:" (pr-str key))
               (let [key-as-var   (variable? key)
                     static-value (get interm-vars key-as-var)]
                 (when (and (= idx 1) (not key-as-var) (not= "_id" key)
                            (not (dbproto/-p-prop db :name (re-find #"[_a-zA-Z0-9/]*" key))))
                   (throw (ex-info (str "Invalid predicate provided: " key)
                                   {:status 400
                                    :error  :db/invalid-query})))
                 (cond static-value
                       (update acc :search #(conj % static-value))

                       key-as-var
                       (-> acc
                           (update :search #(conj % nil))
                           (assoc-in [:rel key-as-var] idx))

                       (and (= idx 2) (parse/query-fn? key))
                       (let [filter-code (#?(:clj read-string :cljs cljs.reader/read-string) (subs key 1))
                             var         (or (get-vars filter-code)
                                             (throw (ex-info (str "Filter function must contain a valid variable. Provided: " key)
                                                             {:status 400 :error :db/invalid-query})))
                             [fun _] (filter/extract-filter-fn filter-code #{var})
                             filter-fn   (filter/get-internal-filter-fn var fun)]
                         (-> acc
                             (update :search #(conj % nil))
                             (assoc-in [:opts :object-fn] filter-fn)
                             (assoc-in [:rel var] idx)))

                       (and (= idx 1) (re-find #"\+" key))
                       (let [[pred recur-amt] (str/split key #"\+")
                             recur-amt (if recur-amt
                                         (or (safe-read-string recur-amt) 100)
                                         100)]
                         (-> acc
                             (update :search #(conj % pred))
                             (assoc-in [:opts :recur] recur-amt)))

                       (escaped-string? key)
                       (update acc :search #(conj % (safe-read-string key)))

                       :else
                       (update acc :search #(conj % key))))) {:search [] :rel {} :opts {}} clause))

(defn transform-tuples-to-idxs
  "Returns an updated list of tuples that only contains tuple indexes from idx.
  e.g.:
  idx is a list of indexes, eg. [2 0 4]
  Thus, the return will be 3-tuples of nth 2, 0, 4 respectively."
  [idxs tuples]
  (map (fn [tuple] (map #(nth tuple %) idxs)) tuples))

(defn clause->keys
  [clause]
  (reduce (fn [acc var]
            (if-let [var (variable? var)]
              (conj acc var) acc))
          [] clause))

(defn intersecting-keys-tuples-clause
  [tuples clause]
  (let [rel-keys    (-> tuples :headers set)
        clause-keys (clause->keys clause)]
    (reduce (fn [acc key]
              (if (rel-keys key)
                (conj acc key) acc)) [] clause-keys)))

(defn intersecting-keys-tuples
  [a-tuples b-tuples]
  (let [a-keys (-> a-tuples :headers set)
        b-keys (-> b-tuples :headers)]
    (reduce (fn [acc key]
              (if (a-keys key)
                (conj acc key) acc)) [] b-keys)))


(defn get-tuple-indexes
  "Returns index positions of vars within headers.
  e.g. if vars were:
  ['?e '?name]
  and headers were:
  ['?email '?name '?x '?e]
  The return value would be: [3 1]"
  [vars headers]
  (reduce (fn [acc var-smt]
            (if-let [var (or (variable? var-smt)
                             (:variable var-smt))]
              (conj acc (util/index-of headers var))
              (throw
                (ex-info (str var-smt " cannot be retrieved from the results. "
                              "Check that it is declared in your where clause.")
                         {:status 400 :error :db/invalid-query}))))
          [] vars))


(defn select-from-tuples
  [vars tuples]
  (let [idxs (get-tuple-indexes vars (:headers tuples))]
    (transform-tuples-to-idxs idxs (:tuples tuples))))

(defn add-fuel
  [add-amount fuel max-fuel]
  (if (and max-fuel (> add-amount max-fuel))
    (throw (ex-info (str "Maximum query fuel exceeded: " max-fuel)
                    {:status 400 :error :db/exceeded-cost}))
    (when (and fuel (volatile? fuel))
      (vswap! fuel + add-amount))))

(defn replace-vars-wikidata
  [all-wd intersecting-vars vars]
  (mapv (fn [clause] (mapv (fn [clause-item]
                             (if-let [key-replace (intersecting-vars (symbol clause-item))]
                               (let [replacement  (get vars key-replace)
                                     replacement' (if (number? (#?(:clj read-string :cljs cljs.reader/read-string) replacement)) replacement (str "\"" replacement "\""))] replacement') clause-item)) clause)) all-wd))

(defn wikidata->tuples
  [q-map clause r {:keys [vars] :as res} optional? fuel max-fuel]
  (go-try
    (if (nil? r)
      (let [all-wd (wikidata/get-all-wd-clauses (:where q-map))]
        ;; If there is a WD clause in the where clause, then we will evaluate
        ;; all the optional WD clauses at the same time as all the other WD clauses.
        ;; therefore, when it comes time to evaluate an optional WD clause, we ignore it.
        (if (and optional? (not (empty? all-wd)))
          [nil r]
          (let [optional          (wikidata/get-all-wd-optional-clauses (:where q-map))
                all-wd-vars       (-> (apply concat (map clause->keys all-wd)) set)
                all-vars          (into all-wd-vars
                                        (apply concat (map clause->keys optional)))
                intersecting-vars (-> (remove nil? (map #(all-vars %) (keys vars))) set)
                matching-vars     (apply concat (map #(intersecting-keys-tuples-clause res %) all-wd))
                matching-vals     (select-from-tuples matching-vars res)
                all-wd-w-intm     (replace-vars-wikidata all-wd intersecting-vars vars)
                all-vars'         (remove intersecting-vars all-vars)
                wikidataTuples    (<? (wikidata/get-wikidata-tuples q-map all-wd-w-intm matching-vars matching-vals all-vars' optional))
                _                 (add-fuel (-> wikidataTuples :tuples count) fuel max-fuel)]
            [wikidataTuples r])))
      ;; If we are not evaluating the last clause, we drop ALL wikidata clauses from rest.
      ;; Then we add the current WD clause to the end of rest.
      ;; When we finally evaluate WD, we pull from the FULL query map
      [nil (conj (wikidata/drop-all-wd-clauses r) clause)])))

(defn db-ident?
  [source]
  (= (-> source (str/split #"/") count) 2))

(defn parse-block-from-source
  [block]
  (let [block' (safe-read-string block)]
    (if (int? block') block' block)))


(defn- isolate-source-name
  [ledger-id]
  (re-find #"[a-z]+" ledger-id))

(defn- isolate-source-block
  [ledger-id]
  (re-find #"[A-Z0-9]+" ledger-id))


(defn get-source-clause
  ([db clause]
   (get-source-clause db clause {} {}))
  ([db clause prefixes opts]
   (go-try (let [source (first clause)]
             (cond (= "$fdb" source)
                   [db (into [] (subvec clause 1 4))]

                   ;; block
                   (str/starts-with? source "$fdb")
                   (let [block (parse-block-from-source (subs source 4))
                         db    (<? (time-travel/as-of-block db block))
                         _     (when (util/exception? db)
                                 (throw db))]
                     [db (into [] (subvec clause 1 4))])

                   (= "$wd" source)
                   ["$wd" clause]

                   ;; The db permissions are resolved in query-async. Here we just retrieve the db and time-travel if needed
                   :else
                   (let [source-name  (isolate-source-name source)
                         source-block (isolate-source-block source)]
                     (if-let [db-id (get prefixes (keyword source-name))]
                       (let [block (parse-block-from-source source-block)
                             db    (<? (get-in opts [:sources db-id]))
                             db'   (if block
                                     (<? (time-travel/as-of-block db block))
                                     db)]
                         [db' (into [] (subvec clause 1 4))])

                       ; else
                       (throw (ex-info (str "The data source: " source " is not supported in Fluree")
                                       {:status 400
                                        :error  :db/invalid-query})))))))))

(defn tuples->map
  [start-map tuples]
  (reduce (fn [acc [sub obj]]
            (update acc sub conj obj)) start-map tuples))

(defn expand-map
  [tuple-map]
  (zipmap (keys tuple-map)
          (map #(hash-map :done false :followed #{} :all (set %)) (vals tuple-map))))

(defn follow-all-original-subject-paths
  [subjects tuple-map]
  (let [expanded-map (expand-map tuple-map)]
    (loop [[subject & r] subjects
           acc expanded-map]
      (cond (not subject)
            acc

            (get-in acc [subject :done])
            (recur r acc)

            :else
            (let [subject-all            (get-in acc [subject :all])
                  subject-followed       (get-in acc [subject :followed])
                  subjects-to-follow     (set/difference subject-all subject-followed)
                  acc*                   (reduce (fn [acc subject-to-follow]
                                                   (let [all-followed      (get-in acc [subject :followed])
                                                         self?             (= subject subject-to-follow)
                                                         already-followed? (if self? false (all-followed subject-to-follow))
                                                         acc'              (update-in acc [subject :followed] conj subject-to-follow)]
                                                     ;; If subject is self or already followed, we move onto the next subject-to-follow
                                                     (if (or already-followed? self?)
                                                       acc'

                                                       ;; If subject-to-follow isn't self, we can add all the subject-to-follow's all
                                                       ;; to subject's all
                                                       (let [subject-to-follow-all (get-in acc' [subject-to-follow :all])
                                                             acc'                  (update-in acc'
                                                                                              [subject :all]
                                                                                              (fn [existing]
                                                                                                (apply conj existing subject-to-follow-all)))]
                                                         ;; Then, if subject to follow is done, we can also add all of
                                                         ;; subject-to-follow's all to subject's followed
                                                         (if (get-in acc' [subject-to-follow :done])
                                                           (update-in acc' [subject :followed]
                                                                      (fn [existing] (apply conj existing subject-to-follow-all)))
                                                           acc'))))) acc subjects-to-follow)
                  subject-followed-count (get-in acc [subject :followed])
                  subject-all-count      (get-in acc [subject :all])
                  subject-done?          (= subject-followed-count subject-all-count)]
              (if subject-done?
                (recur r (assoc-in acc* [subject :done] true))
                (recur subjects acc*)))))))

(defn recur-map->tuples
  [subjects recur-map]
  (reduce (fn [acc subject]
            (let [subject-vals (get-in recur-map [subject :followed])]

              (concat acc (map #(vector subject %) subject-vals)))) [] subjects))

(defn tuples->recur
  [db predicate recur-map depth var-first?]
  (go-try (let [max-depth (or depth 100)
                recur-map (loop [acc   recur-map
                                 depth 1]
                            (if (>= depth max-depth)
                              acc
                              (let [search-vals (-> acc vals flatten set
                                                    (set/difference (set (keys acc))))]
                                (if (empty? search-vals)
                                  acc
                                  (let [res    (loop [acc []
                                                      [search-val & r] search-vals]
                                                 (if search-val
                                                   (recur (concat acc (<? (query-range/index-range db :spot = [search-val predicate]))) r)
                                                   acc))
                                        tuples (transform-tuples-to-idxs [0 2] res)
                                        acc*   (tuples->map acc tuples)]
                                    (recur acc* (inc depth)))))))

                subjects  (keys recur-map)
                recur-map (follow-all-original-subject-paths subjects recur-map)
                tuples    (recur-map->tuples subjects recur-map)]
            (if var-first? tuples (distinct (map #(-> % second vector) tuples))))))

(defn fdb-clause->tuples
  [db {:keys [headers tuples vars] :as res} clause fuel max-fuel opts]
  (go-try (let [{:keys [search rel] search-opts :opts} (clause->rel db vars clause)
                common-keys (intersecting-keys-tuples-clause res clause)
                object-fn   (:object-fn search-opts)
                recur-depth (:recur search-opts)
                [search-opts' clause'] (reduce (fn [[acc clause'] common-key]
                                                 (let [idx-of    (util/index-of clause (str common-key))
                                                       k         (condp = idx-of 0 :subject-fn 1 :predicate-fn 2 :object-fn)
                                                       res-idx   (util/index-of headers common-key)
                                                       v         (into #{} (map first (transform-tuples-to-idxs [res-idx] tuples)))
                                                       single-v? (= 1 (count v))
                                                       v         (if (and (not single-v?) object-fn (= k object-fn))
                                                                   (comp v object-fn)
                                                                   v)]
                                                   (if single-v?
                                                     [acc (assoc clause' idx-of (first v))]
                                                     [(assoc acc k v) clause'])))
                                               [{} search] common-keys)
                ;; Currently, only pass in object-fn to search opts. Seems to be faster to filter
                ;; subject after. I'm sure this depends on a number of variables
                ;; TODO - determine what, when, and how to filter - in index range? after index-range?
                search-opts' {:object-fn (or (:object-fn search-opts') object-fn)}
                res         (<? (query-range/search db clause' search-opts'))
                res'        (if (:parse-json? opts)
                              (json/parse-json-flakes db res)
                              res)
                ;; Currently, not supporting subject and predicate fns, but leaving this here.
                ;{:keys [subject-fn predicate-fn]} opts
                ;res         (cond->> res
                ;                     subject-fn    (filter #(subject-fn (.-s %)))
                ;                     predicate-fn  (filter #(predicate-fn (.-p %))))
                _           (add-fuel (count res') fuel max-fuel)
                tuples      (transform-tuples-to-idxs (vals rel) res')
                tuples'     (if recur-depth
                              (let [clause-1st (first clause')
                                    var-first? (variable? (first clause))
                                    predicate  (nth clause' 1)

                                    ;; Potentially, predicate could have been a variable and previous
                                    ;; where-items resolved it, but we can only handle one resolve
                                    ;; predicate here.
                                    _          (when (variable? predicate)
                                                 (throw (ex-info (str "Cannot use predicate recursion when predicate is variable. Provided: " clause')
                                                                 {:status 400
                                                                  :error  :db/invalid-query})))
                                    _          (when-not (variable? (nth clause 2))
                                                 (throw (ex-info (str "Cannot use predicate recursion when object is not a variable. Provided: " clause')
                                                                 {:status 400
                                                                  :error  :db/invalid-query})))
                                    recur-map  (cond var-first?
                                                     (tuples->map {} tuples)

                                                     (number? clause-1st)
                                                     (assoc {} clause-1st (flatten tuples))

                                                     (coll? clause-1st)
                                                     (assoc {} (-> res' first first) (flatten tuples)))]

                                (<? (tuples->recur db predicate recur-map recur-depth var-first?)))
                              tuples)]
            {:headers (keys rel)
             :vars    vars
             :tuples  tuples'})))


(defn full-text->tuples
  [{:keys [conn network ledger-id] :as db} res clause]
  #?(:cljs (throw (ex-info "Full text search is not supported in JS"
                           {:status 400
                            :error  :db/invalid-query}))
     :clj  (if (:memory conn)
             (throw (ex-info "Full text search is not supported in when running in-memory"
                             {:status 400
                              :error  :db/invalid-query}))
             (let [lang (-> db :settings :language (or :default))
                   [var search search-param] clause
                   var  (variable? var)]
               (with-open [^Closeable store (full-text/open-storage conn network ledger-id lang)]
                 (full-text/search store db [var search search-param]))))))


;; Can be: ["?item" "rdf:type" "person"]
;; Can be: [234 "rdf:type" "?collection"]
;; Can be: ["?item" "rdf:type" "?collection"] -> but item is already bound. Need forward filtering here...

(defn collection->tuples
  [db res clause]
  (go-try (let [subject-var (variable? (first clause))
                object-var  (variable? (last clause))]
            (cond (and subject-var object-var)
                  (throw (ex-info "When using rdf:type, either a subject or a type (collection) must be specified."
                                  {:status 400
                                   :error  :db/invalid-query}))

                  subject-var
                  ;; _tx and _block return the same things
                  (if (#{"_tx" "_block"} (last clause))
                    (let [min-sid (-> db :t)
                          max-sid 0]
                      {:headers [subject-var]
                       :tuples  (map #(conj [] %) (range min-sid max-sid))
                       :vars    {}})

                    (let [partition (dbproto/-c-prop db :partition (last clause))
                          max-sid   (-> db :ecount (get partition))
                          min-sid   (flake/min-subject-id partition)
                          flakes    (<? (query-range/index-range db :spot
                                                                 >= [max-sid]
                                                                 <= [min-sid]))
                          xf        (comp (map (fn [f] [(flake/s f)])) (distinct))]
                      {:headers [subject-var]
                       :tuples  (sequence xf flakes)
                       :vars    {}}))

                  object-var
                  (let [s       (first clause)
                        subject (if (number? s) s (<? (dbproto/-subid db s)))
                        cid     (flake/sid->cid subject)
                        cname   (dbproto/-c-prop db :name cid)]
                    {:headers [object-var]
                     :tuples  [[cname]]
                     :vars    {}})))))


(def all-functions #{"STR" "RAND" "ABS" "CEIL" "FLOOR" "CONCAT"
                     "STRLEN"

                     "UCASE" "LCASE" "ENCODE_FOR_URI" "CONTAINS"
                     "STRSTARTS" "STRENDS" "STRBEFORE" "STRAFTER" "YEAR" "MONTH"
                     "DAY" "HOURS" "MINUTES" "SECONDS" "TIMEZONE" "TZ" "NOW"
                     "UUID" "STRUUID" "MD5" "SHA1" "SHA256" "SHA384" "SHA512"
                     "COALESCE" "IF" "STRLANG" "STRDT" "sameTerm" "isIRI" "isURI"
                     "isBLANK" "isLITERAL" "isNUMERIC"})


;; Uses SPARQL aggregates + additional ones as extension.
;; https://docs.data.world/tutorials/sparql/list-of-sparql-aggregate-functions.html
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
  [x]
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))

(defn interm-aggregate?
  [x]
  (and (string? x)
       (re-matches #"^#\(.+\)$" x)))

(defn parse-aggregate*
  "Returns map of aggregate function executable code or error if invalid aggregate function."
  [parsed-code as valid-var]
  (let [list-count (count parsed-code)
        [fun arg var] (cond
                        (= 3 list-count)
                        [(first parsed-code) (second parsed-code) (last parsed-code)]

                        (= 2 list-count)
                        (if (= 'sample (first parsed-code))
                          (throw (ex-info (str "The sample aggregate function takes two arguments: n and a variable, provided: "
                                               (pr-str parsed-code))
                                          {:status 400 :error :db/invalid-query}))
                          [(first parsed-code) nil (last parsed-code)])

                        :else
                        (throw (ex-info (str "Invalid aggregate selection, provided: " (pr-str parsed-code))
                                        {:status 400 :error :db/invalid-query})))
        agg-fn     (if-let [agg-fn (built-in-aggregates fun)]
                     (if arg (fn [coll] (agg-fn arg coll)) agg-fn)
                     (throw (ex-info (str "Invalid aggregate selection function, provided: " (pr-str parsed-code))
                                     {:status 400 :error :db/invalid-query})))
        [agg-fn variable] (let [distinct? (and (coll? var) (= (first var) 'distinct))
                                variable  (if distinct? (second var) var)
                                agg-fn    (if distinct? (fn [coll] (-> coll distinct agg-fn))
                                                        agg-fn)]
                            [agg-fn variable])
        fn-str     (str "(fn [" variable "] " (pr-str parsed-code) ")")]
    (when-not (valid-var variable)
      (throw (ex-info (str "Invalid select variable in aggregate select, provided: " (pr-str parsed-code))
                      {:status 400 :error :db/invalid-query})))
    {:variable variable
     :as       as
     :fn-str   fn-str
     :function agg-fn}))


(defn parse-aggregate
  "Parses string aggregate function and returns execution map if valid."
  [code-str valid-var]
  (let [list-agg    (#?(:clj read-string :cljs cljs.reader/read-string) code-str)
        as?         (= 'as (first list-agg))
        as          (if as?
                      (-> (str "?" (last list-agg)) symbol)
                      (->> list-agg (str "?") symbol))
        code-parsed (if as?
                      (let [func-list (second list-agg)]
                        (if (coll? func-list)
                          func-list
                          (throw (ex-info (str "Invalid aggregate selection. As can only be used in conjunction with other functions. Provided: " code-str)
                                          {:status 400 :error :db/invalid-query}))))
                      list-agg)]
    (parse-aggregate* code-parsed as valid-var)))


(defn calculate-aggregate
  [tuples aggregate-fn-map]
  (let [{:keys [variable as function]} aggregate-fn-map
        agg-params (flatten (select-from-tuples [variable] tuples))
        agg-result (function agg-params)]
    [as agg-result]))

(defn add-aggregate-cols
  [res aggregate]
  (reduce (fn [res agg]
            (let [[as agg-result] (calculate-aggregate res agg)
                  {:keys [headers tuples]} res
                  tuples'  (map #(conj (vec %) agg-result) tuples)
                  headers' (conj (vec headers) as)]
              {:headers headers' :tuples tuples'}))
          res aggregate))

(defn match-tuples-lists
  "Combines two lists of tuples, a-tuples and b-tuples, into a single aggregated
  tuples list based on matching criteria.

  Matching criteria is a-idxs and b-idxs - which represent the multiple index (columns)
  of each tuples set that must be compared. i.e. if column 1 in a-tuples is to be compared
  to column 3 in b-tuples, then a-idxs will be [0] and b-idxs will be [2]. Indexes start at 0.
  Multiple indexes can be compared, i.e. a-idxs of [1 2] means compare both 1 and 2 columns.
  Order matters. The count of a-idxs and b-idxs should always be identical, else there would never
  be any matches.

  When there is a match, all non-matching columns from matching b-tuples are appended to the
  respective matched a-tuple. i.e. if b-tuples had 4 columns (indexes 0 -> 3),
  and was matching on [2], then columns [0 1 3] would be appended to the respective matched a-tuple.

  If left-outer-join? is true, instead of discarding any non-matches,
  we retain all the a-tuples, but pad the extra b-columns (b-not-idxs) with 'nil'"
  [a-idxs a-tuples b-idxs b-tuples b-not-idxs left-outer-join?]
  ;; make a comparison map of b-tuples for quick lookups, with the key
  ;; being the b-ixs values to be compared, and value being the b-tuples (can be many)
  ;; that match that comparison
  (let [b-map (reduce (fn [acc tuple]
                        (let [b-compare (map #(nth tuple %) b-idxs)]
                          (update acc b-compare conj tuple)))
                      {} b-tuples)]
    ;; iterate over a-tuples, and their respective match criteria, to see if matching b-tuple(s) exist
    (seq (reduce
           (fn [acc a-tuple]
             (let [a-compare (map #(nth a-tuple %) a-idxs)]
               (if-let [b-matched (get b-map a-compare)]
                 ;; found match, appends all b-tuples columns (b-not-idxs) to the matching a-tuple
                 (reduce #(conj %1 (concat a-tuple (map (fn [idx] (nth %2 idx)) b-not-idxs))) acc b-matched)
                 ;; no match, but if left-outer-join retain a-tuple and pad with nil values
                 (if left-outer-join?
                   (conj acc (concat a-tuple (repeat (count b-not-idxs) nil)))
                   acc))))
           [] a-tuples))))


(defn find-match+row-nums
  "Given a single tuple from A, a-idxs, b-idxs, b-not-idxs, and b-tuples, return any tuples in b that match.
  Along with their row-numbers"
  [a-tuple a-idxs b-tuples b-idxs b-not-idxs]
  (let [a-tuple-part (map #(nth a-tuple %) a-idxs)]
    (reduce-kv (fn [[acc b-rows] row b-tuple]
                 (if (= a-tuple-part (map #(nth b-tuple %) b-idxs))
                   [(conj (or acc []) (concat a-tuple (map #(nth b-tuple %) b-not-idxs))) (conj b-rows row)]
                   [acc b-rows]))
               [nil #{}] (into [] b-tuples))))

(defn inner-join
  [a-res b-res]
  (let [common-keys (intersecting-keys-tuples a-res b-res)
        a-idxs      (map #(util/index-of (:headers a-res) %) common-keys)
        b-idxs      (map #(util/index-of (:headers b-res) %) common-keys)
        b-not-idxs  (-> b-res :headers count (#(range 0 %))
                        set (set/difference (set b-idxs)) (#(apply vector %)))
        c-tuples    (match-tuples-lists a-idxs (:tuples a-res) b-idxs (:tuples b-res) b-not-idxs false)
        c-headers   (concat (:headers a-res) (map #(nth (:headers b-res) %) b-not-idxs))]
    {:headers c-headers
     :vars    (merge (:vars a-res) (:vars b-res))
     :tuples  c-tuples}))

(defn left-outer-join
  "OPTIONAL clause is equivalent to a left outer join. If there are no matches in the b-tuples,
  we just return a 'match' where each element of the match from b-tuple is nil."
  [a-tuples b-tuples]
  (let [common-keys (intersecting-keys-tuples a-tuples b-tuples)
        a-idxs      (map #(util/index-of (:headers a-tuples) %) common-keys)
        b-idxs      (map #(util/index-of (:headers b-tuples) %) common-keys)
        b-not-idxs  (-> b-tuples :headers count (#(range 0 %))
                        set (set/difference (set b-idxs)) (#(apply vector %)))
        c-tuples    (match-tuples-lists a-idxs (:tuples a-tuples) b-idxs (:tuples b-tuples) b-not-idxs true)
        c-headers   (concat (:headers a-tuples) (map #(nth (:headers b-tuples) %) b-not-idxs))]
    {:headers c-headers
     :vars    (merge (:vars a-tuples) (:vars b-tuples))
     :tuples  c-tuples}))


(declare resolve-where-clause)

(defn tuples->filter-required
  [headers tuples valid-vars filter-code-req]
  (let [filter-code-req-str  (str "(and " (str/join " " filter-code-req) ")")
        [filter-code-req* _] (or (filter/extract-filter-fn filter-code-req-str valid-vars)
                                 (throw (ex-info (str "Invalid required filters, provided: " filter-code-req-str)
                                                 {:status 400 :error :db/invalid-query})))
        filter-code-req-str* (str filter-code-req*)]
    (filter #(filter/filter-row headers % filter-code-req-str*) tuples)))

(defn tuples->filter-optional
  [headers tuples valid-vars filter-code-opts]
  (reduce (fn [tuples filt]
            (let [[filt* filt-vars] (or (filter/extract-filter-fn filt valid-vars)
                                        (throw (ex-info (str "Invalid filter, provided: " filt)
                                                        {:status 400 :error :db/invalid-query})))
                  filt-str        (str filt*)
                  filt-vars-idxs  (map #(util/index-of headers %) filt-vars)
                  filtered-tuples (reduce (fn [acc clause]
                                            (if (every? #(nth clause %) filt-vars-idxs)
                                              (if (filter/filter-row headers clause filt-str)
                                                (conj acc clause)
                                                acc)
                                              (conj acc clause)))
                                          [] tuples)]
              filtered-tuples)) tuples filter-code-opts))


(defn tuples->filtered
  [{:keys [headers vars tuples] :as tuple-map} filters optional?]
  (let [valid-vars (set headers)
        header-vec (into [] headers)
        _          (when (string? filters)
                     (throw (ex-info (str "Filter must be enclosed in square brackets. Provided: " filters)
                                     {:status 400
                                      :error  :db/invalid-query})))]
    ;; if optional is true, ALL filters are optional. This needs to be refactored.
    (if optional?
      (let [res
            {:headers headers
             :vars    vars
             :tuples  (tuples->filter-optional header-vec tuples valid-vars filters)}]
        res)
      (let [filter-code-req (filter/get-filters filters false)
            tuples          (if (not-empty filter-code-req)
                              (tuples->filter-required header-vec tuples valid-vars filter-code-req)
                              tuples)
            filter-code-opt (filter/get-filters filters true)
            tuples          (if (not-empty filter-code-opt)
                              (tuples->filter-optional header-vec tuples valid-vars filter-code-opt)
                              tuples)]
        {:headers headers
         :vars    vars
         :tuples  tuples}))))

(defn bind-clause->vars
  [res clause]
  (let [[k v] clause
        k         (variable? k)
        _         (when-not k (throw (ex-info (str "Invalid intermediate aggregate value. Provided: " clause)
                                              {:status 400 :error :db/invalid-query})))
        {:keys [headers vars]} res
        var-value (if (interm-aggregate? v)
                    (->> (parse-aggregate (subs v 1) (set (if-let [valid-var (keys vars)]
                                                            (conj headers valid-var)
                                                            headers)))
                         (calculate-aggregate res)
                         second)
                    v)]
    {k var-value}))

(declare clause->tuples)

(defn optional->left-outer-joins
  [db q-map optional-clauses where-tuples fuel max-fuel opts]
  (async/go-loop [[opt-clause & r] optional-clauses
                  tuples where-tuples]
    (if opt-clause
      (let [[next-tuples r] (<? (clause->tuples db q-map tuples opt-clause r true fuel max-fuel opts))]
        (cond (nil? next-tuples) (recur r tuples)
              (:filter opt-clause) (recur r next-tuples)
              :else (recur r (left-outer-join tuples next-tuples))))
      tuples)))

(defn res-absorb-vars
  [res]
  (reduce (fn [acc [var-name var-val]]
            (let [tuples'  (map #(conj % var-val) (:tuples res))
                  headers' (conj (:headers res) var-name)]
              {:tuples tuples' :headers headers'})) res (:vars res)))


(defn clause->tuples
  "Tuples and optional? are only used for Wikidata, because need to both limit calls to Wikidata,
  and ensure that returned results are as limited as possible (but still relevant)."
  [db {:keys [prefixes] :as q-map} {:keys [vars] :as res} clause r optional? fuel max-fuel opts]
  (go-try
    (cond (map? clause)
          ;; Could be a union, bind, filter, optional, and more
          (let [segment-type (first (keys clause))]
            (condp = segment-type

              :optional
              [(<? (optional->left-outer-joins db q-map (-> clause :optional) res fuel max-fuel opts)) r]

              :union
              (loop [[clause-group & rest] (-> clause :union)
                     tuples nil]
                (if clause-group
                  (let [new-res   (<? (resolve-where-clause db clause-group q-map vars fuel max-fuel opts))
                        ;; We only want to absorb the vars that are newly created within the clause-group
                        ;; so we need to dissoc any vars that already existed.
                        new-res*  (reduce (fn [res var]
                                            (update new-res :var dissoc var))
                                          new-res (keys vars))
                        new-res** (res-absorb-vars new-res*)]
                    (if tuples
                      (recur rest (union/results tuples new-res**))
                      (recur rest new-res**)))
                  [tuples r]))

              :bind
              (let [bindings (-> clause :bind)
                    vars     (map #(bind-clause->vars res %) bindings)
                    vars     (into {} vars)]
                [(update res :vars merge vars) r])

              :filter
              [(tuples->filtered res (-> clause :filter) optional?) r]))

          (and (= 3 (count clause)) (str/starts-with? (second clause) "fullText:"))
          [(full-text->tuples db res clause) r]

          (and (= 3 (count clause)) (= (second clause) "rdf:type"))
          [(<? (collection->tuples db res clause)) r]

          (= 3 (count clause))
          [(<? (fdb-clause->tuples db res clause fuel max-fuel opts)) r]

          (= 2 (count clause))
          [(update res :vars merge (bind-clause->vars res clause)) r]

          (= 1 (count clause))
          (if (sequential? (first clause))
            (throw (ex-info (str "Invalid where clause, it appears you have an extra nested vector here: " clause)
                            {:status 400 :error :db/invalid-query}))
            (throw (ex-info (str "Invalid where clause, it should have 2+ tuples but instead found: " clause)
                            {:status 400 :error :db/invalid-query})))

          :else
          (let [[db clause] (<? (get-source-clause db clause prefixes opts))]
            (cond (= "$wd" db) (<? (wikidata->tuples q-map clause r res optional? fuel max-fuel))

                  (str/starts-with? (second clause) "fullText:")
                  [(full-text->tuples db res clause) r]

                  :else
                  [(<? (fdb-clause->tuples db res clause fuel max-fuel opts)) r])))))


(defn resolve-where-clause
  ([db where q-map vars fuel max-fuel]
   (resolve-where-clause db where q-map vars fuel max-fuel {}))
  ([db where q-map vars fuel max-fuel opts]
   (go-try
     (loop [[clause & r] where
            res {:vars vars}]
       (if clause
         (let [[next-res r] (<? (clause->tuples db q-map res clause r false fuel max-fuel opts))]
           (cond (= 2 (count clause))
                 (recur r next-res)

                 (empty? (dissoc res :vars))
                 (recur r (or next-res res))

                 (nil? next-res)
                 (recur r res)

                 :else
                 (recur r (inner-join res next-res))))
         res)))))

(defn q
  [{:keys [vars query-map fuel max-fuel db] :as opts}]
  (go-try
    (let [{:keys [where optional filter]} query-map
          where-res    (<? (resolve-where-clause db where query-map vars fuel max-fuel opts))
          optional-res (if optional
                         (<? (optional->left-outer-joins db query-map optional where-res fuel max-fuel opts))
                         where-res)]
      (if filter
        (tuples->filtered optional-res filter nil)
        optional-res))))
