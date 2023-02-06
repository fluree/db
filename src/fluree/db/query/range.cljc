(ns fluree.db.query.range
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.index :as index]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            #?(:clj  [clojure.core.async :refer [chan go go-loop <! >!] :as async]
               :cljs [cljs.core.async :refer [chan <! >!] :refer-macros [go go-loop] :as async])
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn- pred-id-strict
  "Will throw if predicate doesn't exist."
  [db p]
  (when p
    (or (dbproto/-p-prop db :id p)
        (throw (ex-info (str "Invalid predicate, does not exist: " p)
                        {:status 400, :error :db/invalid-predicate})))))


(defn- coerce-predicate
  "If a predicate is provided as a string value, coerce to pid"
  [db pred]
  (if (string? pred)
    (or (dbproto/-p-prop db :id pred)
        (throw (ex-info (str "Invalid predicate, does not exist: " pred)
                        {:status 400, :error :db/invalid-predicate})))
    pred))


(defn- match->flake-parts
  "Takes a match from index-range, and based on the index
  returns flake-ordered components of [s p o t op m].
  Coerces idents and string predicate names."
  [db idx match]
  (let [[p1 p2 p3 p4 op m] match]
    (case idx
      :spot [p1 (coerce-predicate db p2) p3 p4 op m]
      :psot [p2 (coerce-predicate db p1) p3 p4 op m]
      :post [p3 (coerce-predicate db p1) p2 p4 op m]
      :opst [p3 (coerce-predicate db p2) p1 p4 op m]
      :tspo [p2 (coerce-predicate db p3) p4 p1 op m])))


(def ^{:private true :const true} subject-min-match [util/max-long])
(def ^{:private true :const true} subject-max-match [util/min-long])
(def ^{:private true :const true} pred-min-match [0])
(def ^{:private true :const true} pred-max-match [flake/MAX-PREDICATE-ID])
(def ^{:private true :const true} txn-max-match [util/min-long])
(def ^{:private true :const true} txn-min-match [0])


(defn- min-match
  "Smallest index flake part match by index"
  [idx]
  (case idx
    :spot subject-min-match
    :psot pred-min-match
    :post pred-min-match
    :opst subject-min-match
    :tspo txn-min-match))


(defn- max-match
  "Biggest index flake part match by index"
  [idx]
  (case idx
    :spot subject-max-match
    :psot pred-max-match
    :post pred-max-match
    :opst subject-max-match
    :tspo txn-max-match))

(defn resolve-subid
  "Expands an IRI @id for a subject and returns the index's subject-id integer (sid).

  Only called when integer or nil is not provided, so can assume always have a compact
  or full IRI as either a keyword or string."
  [db id]
  (dbproto/-subid db id))

(defn resolve-match-flake
  [test s p o t op m]
  (let [[o' dt] (if (vector? o)
                  [(first o) (second o)]
                  [o nil])
        m' (or m (if (identical? >= test) util/min-integer util/max-integer))]
    (flake/create s p o' dt t op m')))

(defn resolved-leaf?
  [node]
  (and (index/leaf? node)
       (index/resolved? node)))

(defn intersects-range?
  "Returns true if the supplied `node` contains flakes between the `lower` and
  `upper` flakes, according to the `node`'s comparator."
  [node range-set]
  (not (or (and (:rhs node)
                (flake/lower-than-all? (:rhs node) range-set))
           (and (not (:leftmost? node))
                (flake/higher-than-all? (:first node) range-set)))))

(defn query-filter
  "Returns a transducer to filter flakes according to the boolean function values
  of the `:subject-fn`, `:predicate-fn`, and `:object-fn` keys from the supplied
  options map. All three functions are optional, and each supplied function will
  be applied to its corresponding flake component, and only flakes where each
  function evaluates to a truthy value will be included."
  [{:keys [subject-fn predicate-fn object-fn]}]
  (let [filter-xfs (cond-> []
                     subject-fn   (conj (filter (fn [f] (subject-fn (flake/s f)))))
                     predicate-fn (conj (filter (fn [f] (predicate-fn (flake/p f)))))
                     object-fn    (conj (filter (fn [f] (object-fn (flake/o f))))))]
    (apply comp filter-xfs)))

(defn extract-query-flakes
  "Returns a transducer to extract flakes from each leaf from a stream of index
  leaf nodes that satisfy the bounds specified in the supplied query options
  map. The result of the transformation will be a stream of collections of
  flakes from the leaf nodes in the input stream, with one flake collection for
  each input leaf."
  [{:keys [start-flake start-test end-flake end-test flake-xf] :as opts}]
  (let [query-xf (comp (map :flakes)
                       (map (fn [flakes]
                              (flake/subrange flakes
                                              start-test start-flake
                                              end-test end-flake)))
                       (map (fn [flakes]
                              (into [] (query-filter opts) flakes))))]
    (if flake-xf
      (let [slice-xf (map (fn [flakes]
                            (sequence flake-xf flakes)))]
        (comp query-xf slice-xf))
      query-xf)))

(defn resolve-flake-slices
  "Returns a channel that will contain a stream of chunked flake collections that
  contain the flakes between `start-flake` and `end-flake` and are within the
  transaction range starting at `from-t` and ending at `to-t`."
  [{:keys [lru-cache-atom] :as conn} root novelty error-ch
   {:keys [from-t to-t start-flake end-flake] :as opts}]
  (let [resolver  (index/->CachedTRangeResolver conn novelty from-t to-t lru-cache-atom)
        cmp       (:comparator root)
        range-set (flake/sorted-set-by cmp start-flake end-flake)
        in-range? (fn [node]
                    (intersects-range? node range-set))
        query-xf  (extract-query-flakes opts)]
    (index/tree-chan resolver root in-range? resolved-leaf? 1 query-xf error-ch)))

(defn unauthorized?
  [f]
  (= f ::unauthorized))

(defn authorize-flake
  [db error-ch flake]
  (go
    (try* (if (or (schema-util/is-schema-flake? flake)
                  (<? (perm-validate/allow-flake? db flake)))
            flake
            ::unauthorized)
          (catch* e
                  (log/error e
                             "Error authorizing flake in ledger"
                             (select-keys db [:network :ledger-id :t]))
                  (>! error-ch e)))))

(defn authorize-flakes
  "Authorize each flake in the supplied `flakes` collection asynchronously,
  returning a collection containing only allowed flakes according to the
  policy of the supplied `db`."
  [db error-ch flakes]
  (->> flakes
       (map (partial authorize-flake db error-ch))
       (async/map (fn [& fs]
                    (into [] (remove unauthorized?) fs)))))

(defn filter-authorized
  "Returns a channel that will eventually return a stream of flake slices
  containing only the schema flakes and the flakes validated by
  fluree.db.permissions-validate/allow-flake? function for the database `db`
  from the `flake-slices` channel"
  [{:keys [policy] :as db} start end error-ch flake-slices]
  #?(:cljs
     flake-slices ; Note this bypasses all permissions in CLJS for now!

     :clj
     (if (true? (get-in policy [:f/view :root?]))
       flake-slices
       (let [auth-fn (fn [flakes ch]
                       (-> (authorize-flakes db error-ch flakes)
                           (async/pipe ch)))
             out-ch  (chan)]
         (async/pipeline-async 2 out-ch auth-fn flake-slices)
         out-ch))))

(defn filter-subject-page
  "Returns a transducer to filter a stream of flakes to only contain flakes from
  at most `limit` subjects, skipping the flakes from the first `offset`
  subjects."
  [limit offset]
  (let [subject-page-xfs (cond-> [(partition-by flake/s)]
                           offset (conj (drop offset))
                           limit  (conj (take limit))
                           true   (conj cat))]
    (apply comp subject-page-xfs)))

(defn into-page
  "Collects flakes from the stream of flake collections in the `flake-slices`
  channel into a sorted vector according to the `limit`, `offset`, and
  `flake-limit` parameters. The result will have flakes from at most `limit`
  subjects, not including flakes from the first `offset` subjects, and having at
  most `flake-limit` flakes in total."
  [limit offset flake-limit flake-slices]
  (let [page-xfs (cond-> [cat]
                   (or limit offset) (conj (filter-subject-page limit offset))
                   flake-limit       (conj (take flake-limit)))
        page-xf  (apply comp page-xfs)]
    (async/transduce page-xf conj [] flake-slices)))

(defn index-range*
  "Return a channel that will eventually hold a sorted vector of the range of
  flakes from `db` that meet the criteria specified in the `opts` map."
  [{:keys [conn] :as db}
   error-ch
   {:keys [idx start-flake end-flake limit offset flake-limit] :as opts}]
  (let [idx-root (get db idx)
        idx-cmp  (get-in db [:comparators idx])
        novelty  (get-in db [:novelty idx])]
    (->> (resolve-flake-slices conn idx-root novelty error-ch opts)
         (filter-authorized db start-flake end-flake error-ch)
         (into-page limit offset flake-limit))))

(defn expand-range-interval
  "Finds the full index or time range interval including the maximum and minimum
  tests when only one test is provided"
  [idx test match]
  (condp identical? test
    =  [>= match <= match]
    <  [> (min-match idx) < match]
    <= [> (min-match idx) <= match]
    >  [> match <= (max-match idx)]
    >= [>= match < (max-match idx)]))

(defn time-range
  "Range query across an index.

  Ranges take the natural numeric sort orders, but all results will return in
  reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :from-t - start transaction (transaction 't' is negative, so smallest number
            is most recent). Defaults to db's t
  :to-t - stop transaction - can be null, which pulls full history
  :xform - xform applied to each result individually. This is not used
           when :chan is supplied.
  :flake-limit - max number of flakes to return"
  ([db idx test match opts]
   (let [[start-test start-match end-test end-match]
         (expand-range-interval idx test match)]
     (time-range db idx start-test start-match end-test end-match opts)))
  ([{:keys [t conn ] :as db} idx start-test start-match end-test end-match opts]
   (let [{:keys [limit offset flake-limit from-t to-t]
          :or   {from-t t, to-t t}}
         opts

         start-parts (match->flake-parts db idx start-match)
         end-parts   (match->flake-parts db idx end-match)

         start-flake (apply resolve-match-flake start-test start-parts)
         end-flake   (apply resolve-match-flake end-test end-parts)
         error-ch    (chan)

         ;; index-range*
         idx-root (get db idx)
         idx-cmp  (get-in db [:comparators idx])
         novelty  (get-in db [:novelty idx])

         ;; resolve-flake-slices
         resolver  (index/->CachedHistoryRangeResolver conn novelty from-t to-t (:lru-cache-atom conn))
         cmp       (:comparator idx-root)
         range-set (flake/sorted-set-by cmp start-flake end-flake)
         in-range? (fn [node] (intersects-range? node range-set))
         query-xf  (extract-query-flakes {:idx         idx
                                          :start-test  start-test
                                          :start-flake start-flake
                                          :end-test    end-test
                                          :end-flake   end-flake})]
     (go-try
       (let [history-ch (->> (index/tree-chan resolver idx-root in-range? resolved-leaf? 1 query-xf error-ch)
                             (filter-authorized db start-flake end-flake error-ch)
                             (into-page limit offset flake-limit))]
         (async/alt!
           error-ch ([e]
                     (throw e))
           history-ch ([hist-range]
                       hist-range)))))))

(defn index-range
  "Range query across an index as of a 't' defined by the db.

  Ranges take the natural numeric sort orders, but all results will
  return in reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :xform - xform applied to each result individually. This is not used when :chan is supplied.
  :limit - max number of flakes to return"
  ([db idx] (index-range db idx {}))
  ([db idx opts] (index-range db idx >= (min-match idx) <= (max-match idx) opts))
  ([db idx test match] (index-range db idx test match {}))
  ([db idx test match opts]
   (let [[start-test start-match end-test end-match]
         (expand-range-interval idx test match)]
     (index-range db idx start-test start-match end-test end-match opts)))
  ([db idx start-test start-match end-test end-match]
   (index-range db idx start-test start-match end-test end-match {}))
  ([{:keys [policy t] :as db} idx start-test start-match end-test end-match
    {:keys [object-fn] :as opts}]
   (let [[s1 p1 o1 t1 op1 m1]
         (match->flake-parts db idx start-match)

         [s2 p2 o2 t2 op2 m2]
         (match->flake-parts db idx end-match)

         [[o1 o2] object-fn] (if-some [bool (cond (boolean? o1) o1
                                                  (boolean? o2) o2
                                                  :else nil)]
                               [[nil nil] (fn [o] (= o bool))]
                               [[o1 o2] object-fn])]

     (go-try
       (let [s1*         (if (or (number? s1) (nil? s1))
                           s1
                           (<? (resolve-subid db s1)))
             start-flake (resolve-match-flake start-test s1* p1 o1 t1 op1 m1)
             s2*         (cond
                           (or (number? s2) (nil? s2))
                           s2

                           (= s2 s1)                        ;; common case when 'test' is =
                           s1*

                           :else
                           (<? (resolve-subid db s2)))
             end-flake   (resolve-match-flake end-test s2* p2 o2 t2 op2 m2)
             error-ch    (chan)
             range-ch    (index-range* db
                                       error-ch
                                       (assoc opts
                                         :idx idx
                                         :from-t t
                                         :to-t t
                                         :start-test start-test
                                         :start-flake start-flake
                                         :end-test end-test
                                         :end-flake end-flake
                                         :object-fn object-fn))]
         (async/alt!
           error-ch ([e]
                     (throw e))
           range-ch ([idx-range]
                     idx-range)))))))

(defn non-nil-non-boolean?
  [o]
  (and (not (nil? o))
       (not (boolean? o))))

(defn tag-string?
  [possible-tag]
  (re-find #"^[a-zA-Z0-9-_]*/[a-zA-Z0-9-_]*:[a-zA-Z0-9-]*$" possible-tag))

(def ^:const tag-sid-start (flake/min-subject-id const/$_tag))
(def ^:const tag-sid-end (flake/max-subject-id const/$_tag))

(defn is-tag-flake?
  "Returns true if flake is a root setting flake."
  [f]
  (<= tag-sid-start (flake/o f) tag-sid-end))


(defn coerce-tag-flakes
  [db flakes]
  (async/go-loop [[flake & r] flakes
                  acc []]
    (if flake
      (if (is-tag-flake? flake)
        (let [[s p o _ t op m] (flake/Flake->parts flake)
              o* (<? (dbproto/-tag db o p))]
          (recur r (conj acc (flake/create s p o* const/$xsd:anyURI t op m))))
        (recur r (conj acc flake))) acc)))

(defn search
  ([db fparts]
   (search db fparts {}))
  ([db fparts opts]
   (go-try
     (let [[s p o t] fparts
           idx-predicate? (dbproto/-p-prop db :idx? p)
           tag-predicate? (if p (= :tag (dbproto/-p-prop db :type p)) false)
           o-coerce?      (and tag-predicate? (string? o))
           o              (cond (not o-coerce?)
                                o

                                (tag-string? o)
                                (<? (dbproto/-tag-id db o))
                                ;; Returns tag-id

                                ;; if string, but not tag string, we have a string
                                ;; like "query" with no namespace, we need to ns.
                                (string? o)
                                (let [tag-name (str (dbproto/-p-prop db :name p) ":" o)]
                                  (<? (dbproto/-tag-id db tag-name))))

           res            (cond
                            s
                            (if (= "_id" p)
                              (<? (index-range db :spot = [s nil nil t] opts))
                              (<? (index-range db :spot = [s p o t] opts)))

                            (and p (non-nil-non-boolean? o) idx-predicate? (not (fn? o)))
                            (<? (index-range db :post = [p o s t] opts))

                            (and p (not idx-predicate?) o)
                            (let [obj-fn (if-let [obj-fn (:object-fn opts)]
                                           (fn [x] (and (obj-fn x) (= x o)))
                                           (fn [x] (= x o)))]
                              ;; check for special case where search specifies _id and an integer, i.e. [nil _id 12345]
                              (if (and (= "_id" p) (int? o))
                                ;; TODO - below should not need a `take 1` - `:limit 1` does not work properly - likely fixed in tsop branch, remove take 1 once :limit works
                                (take 1 (<? (index-range db :spot = [o] (assoc opts :limit 1))))
                                (<? (index-range db :psot = [p s nil t] (assoc opts :object-fn obj-fn)))))

                            p
                            (<? (index-range db :psot = [p s o t] opts))

                            o
                            (<? (index-range db :opst = [o p s t] opts)))]
       (log/debug "search res:" res)
       (if tag-predicate?
         (<? (coerce-tag-flakes db res))
         res)))))

(defn collection
  "Returns spot index range for only the requested collection."
  ([db name] (collection db name nil))
  ([db name opts]
   (go
     (try*
      (if-let [id (dbproto/-c-prop db :id name)]
        (<? (index-range db :spot
                         >= [(flake/max-subject-id id)]
                         <= [(flake/min-subject-id id)]
                         opts))
        (throw (ex-info (str "Invalid collection name: " (pr-str name))
                        {:status 400
                         :error  :db/invalid-collection})))
      (catch* e e)))))

(defn _block-or_tx-collection
  "Returns spot index range for only the requested collection."
  [db opts]
  (index-range db :spot > [0] <= [util/min-long] opts))

(defn txn-from-flakes
  "Returns vector of transactions from a set of flakes.
   Each transaction is a map with the following keys:
   1. db - the associated ledger
   2. tx - a map containing all transaction data in the original cmd
   3. nonce - the nonce
   4. auth - the authority that submitted the transaction
   5. expire - expiration"
  [flakes]
  (loop [[flake' & r] flakes result* []]
    (if (nil? flake')
      result*
      (let [obj     (flake/o flake')
            cmd-map (try*
                     (json/parse obj)
                     (catch* e nil))                       ; log an error if transaction is not parsable?
            {:keys [type db tx nonce auth expire]} cmd-map]
        (recur r
               (if (= type "tx")
                 (conj result* {:db db :tx tx :nonce nonce :auth auth :expire expire})
                 result*))))))
