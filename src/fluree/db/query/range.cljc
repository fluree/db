(ns fluree.db.query.range
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.index :as index]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj  [clojure.core.async :refer [chan go go-loop <! >!] :as async]
               :cljs [cljs.core.async :refer [chan <! >!] :refer-macros [go go-loop] :as async])
            #?(:clj [fluree.db.permissions-validate :as perm-validate])
            [fluree.db.util.async :refer [<? go-try]])
  #?(:cljs (:require-macros [fluree.db.util.async])))

#?(:clj (set! *warn-on-reflection* true))

(defn- pred-id-strict
  "Will throw if predicate doesn't exist."
  [db p]
  (when p
    (or (dbproto/-p-prop db :id p)
        (throw (ex-info (str "Invalid predicate, does not exist: " p)
                        {:status 400, :error :db/invalid-predicate})))))


(defn- match->flake-parts
  "Takes a match from index-range, and based on the index
  returns flake-ordered components of [s p o t op m].
  Coerces idents and string predicate names."
  [db idx match]
  (let [[p1 p2 p3 p4 op m] match]
    (case idx
      :spot [p1 (dbproto/-p-prop db :id p2) p3 p4 op m]
      :psot [p2 (dbproto/-p-prop db :id p1) p3 p4 op m]
      :post [p3 (dbproto/-p-prop db :id p1) p2 p4 op m]
      :opst [p3 (dbproto/-p-prop db :id p2) p1 p4 op m]
      :tspo [p2 (dbproto/-p-prop db :id p3) p4 p1 op m])))


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
  [db id]
  (let [out (chan)]
    (if-not id
      (async/close! out)
      (if (util/pred-ident? id)
        (-> db
            (dbproto/-subid id)
            (async/pipe out))
        (async/put! out id)))
    out))

(defn resolve-match-flake
  [db test parts]
  (go-try
   (let [[s p o t op m] parts
         s' (<? (resolve-subid db s))
         o' (<? (resolve-subid db o))
         m' (or m (if (identical? >= test) util/min-integer util/max-integer))]
     (flake/->Flake s' p o' t op m'))))

(defn query-filter-xf
  [{:keys [subject-fn predicate-fn object-fn]}]
  (let [filter-xfs (cond-> []
                     subject-fn   (conj (filter (fn [f] (subject-fn (flake/s f)))))
                     predicate-fn (conj (filter (fn [f] (predicate-fn (flake/p f)))))
                     object-fn    (conj (filter (fn [f] (object-fn (flake/o f))))))]
    (apply comp filter-xfs)))

(defn intersects-range?
  [{idx-cmp :comparator :as node} lower upper]
  (not (or (and (:rhs node)
                (neg? (idx-cmp (:rhs node) lower)))
           (and (not (:leftmost? node))
                (neg? (idx-cmp upper (:first node)))))))

(defn resolved-leaf?
  [node]
  (and (index/leaf? node)
       (index/resolved? node)))

(defn query-flakes
  [{:keys [from-t to-t novelty start-flake start-test end-flake end-test] :as opts}]
  (comp (map (fn [leaf]
               (index/at-t leaf to-t novelty)))
        (map :flakes)
        (map (partial index/t-range from-t to-t))
        (map (fn [flakes]
               (flake/subrange flakes
                               start-test start-flake
                               end-test end-flake)))
        (map (fn [flakes]
               (into [] (query-filter-xf opts) flakes)))))

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
                             (select-keys db [:network :dbid :t]))
                  (>! error-ch e)))))

(defn authorize-flakes
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
  [{:keys [permissions] :as db} start end error-ch flake-slices]
  #?(:cljs
     flake-slices ; Note this bypasses all permissions in CLJS for now!

     :clj
     (let [s1 (flake/s start)
           p1 (flake/p start)
           s2 (flake/s end)
           p2 (flake/p end)]
       (if (perm-validate/no-filter? permissions s1 s2 p1 p2)
         flake-slices
         (let [auth-fn (fn [flakes ch]
                         (go
                           (let [allowed-flakes (<! (authorize-flakes db error-ch flakes))]
                             (when (seq allowed-flakes)
                               (>! ch allowed-flakes))
                             (async/close! ch))))
               out-ch  (chan)]
           (async/pipeline-async 2 out-ch auth-fn flake-slices)
           out-ch)))))

(defn take-only
  [ch limit]
  (if limit
    (async/take limit ch)
    ch))

(defn filter-subject-frame
  [limit offset flake-slices]
  (if (or limit offset)
    (let [offset  (or offset 0)
          subj-ch (chan 1 (comp cat
                                (partition-by flake/s)
                                (drop offset)))]
      (-> flake-slices
          (async/pipe subj-ch)
          (take-only limit)))
    flake-slices))

(defn into-flake-set
  [idx-cmp flake-limit flake-slices]
  (let [flakeset-xf (map (fn [flakes]
                           (apply flake/sorted-set-by idx-cmp flakes)))]
    (if flake-limit
      (let [flake-ch (async/pipe flake-slices
                                 (chan 1 (comp cat
                                               (partition-all flake-limit)
                                               flakeset-xf)))]
        (async/take 1 flake-ch))
      (let [flake-vec-ch (async/reduce into [] flake-slices)]
        (async/pipe flake-vec-ch
                    (chan 1 flakeset-xf))))))

(defn index-range*
  [{:keys [conn] :as db}
   error-ch
   {:keys [idx start-flake end-flake limit offset flake-limit] :as opts}]
  (let [idx-root  (get db idx)
        idx-cmp   (get-in db [:comparators idx])
        novelty   (get-in db [:novelty idx])
        in-range? (fn [node]
                    (intersects-range? node start-flake end-flake))
        query-xf  (query-flakes (assoc opts :novelty novelty))]
    (->> (index/tree-chan conn idx-root in-range? resolved-leaf? query-xf error-ch)
         (filter-authorized db start-flake end-flake error-ch)
         (filter-subject-frame limit offset)
         (into-flake-set idx-cmp flake-limit))))

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

  Uses a DB, but in the future support supplying a connection and db name, as we
  don't need a 't'

  Ranges take the natural numeric sort orders, but all results will return in
  reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :from-t - start transaction (transaction 't' is negative, so smallest number
            is most recent). Defaults to db's t
  :to-t - stop transaction - can be null, which pulls full history
  :xform - xform applied to each result individually. This is not used
           when :chan is supplied.
  :limit - max number of flakes to return"
  ([db idx] (time-range db idx {}))
  ([db idx opts] (time-range db idx >= (min-match idx) <= (max-match idx) opts))
  ([db idx test match] (time-range db idx test match {}))
  ([db idx test match opts]
   (let [[start-test start-match end-test end-match]
         (expand-range-interval idx test match)]
     (time-range db idx start-test start-match end-test end-match opts)))
  ([db idx start-test start-match end-test end-match]
   (time-range db idx start-test start-match end-test end-match {}))
  ([{t :t :as db} idx start-test start-match end-test end-match opts]
   (let [{:keys [limit from-t to-t]
          :or   {from-t t, to-t t}}
         opts

         idx-compare (get-in db [:comparators idx])
         start-parts (match->flake-parts db idx start-match)
         end-parts   (match->flake-parts db idx end-match)]
     (go-try
      (let [start-flake (<? (resolve-match-flake db start-test start-parts))
            end-flake   (<? (resolve-match-flake db end-test end-parts))
            error-ch    (chan)
            range-ch    (index-range* db
                                      error-ch
                                      {:idx idx
                                       :from-t from-t
                                       :to-t to-t
                                       :start-test start-test
                                       :start-flake start-flake
                                       :end-test end-test
                                       :end-flake end-flake
                                       :flake-limit limit})]
        (async/alt!
          error-ch ([e]
                    (throw e))
          range-ch ([hist-range]
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
  ([{:keys [permissions t] :as db} idx start-test start-match end-test end-match
    {:keys [object-fn] :as opts}]
   (let [idx-compare (get-in db [:comparators idx])

         [s1 p1 o1 t1 op1 m1]
         (match->flake-parts db idx start-match)

         [s2 p2 o2 t2 op2 m2]
         (match->flake-parts db idx end-match)

         [[o1 o2] object-fn] (if-some [bool (cond (boolean? o1) o1
                                                  (boolean? o2) o2
                                                  :else nil)]
                               [[nil nil] (fn [o] (= o bool))]
                               [[o1 o2] object-fn])]

     (go-try
      (let [start-flake (<? (resolve-match-flake db start-test [s1 p1 o1 t1 op1 m1]))
            end-flake   (<? (resolve-match-flake db end-test [s2 p2 o2 t2 op2 m2]))
            error-ch    (chan)
            range-ch    (index-range* db
                                      error-ch
                                      (assoc opts
                                             :idx         idx
                                             :from-t      t
                                             :to-t        t
                                             :start-test  start-test
                                             :start-flake start-flake
                                             :end-test    end-test
                                             :end-flake   end-flake
                                             :object-fn   object-fn))]
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
        (let [[s p o t op m] (flake/Flake->parts flake)
              o (<? (dbproto/-tag db o p))]
          (recur r (conj acc (flake/parts->Flake [s p o t op m]))))
        (recur r (conj acc flake))) acc)))

(defn search
  ([db fparts]
   (search db fparts {}))
  ([db fparts opts]
   (go-try (let [[s p o t] fparts
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
                                  (<? (index-range db :spot = [s p o t] opts))

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
                                  (<? (index-range db :opst = [o p s t] opts)))
                 res*           (if tag-predicate?
                                  (<? (coerce-tag-flakes db res))
                                  res)]
             res*))))

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

(defn block-with-tx-data
  "Returns block data as a map, with the following keys:
  1. block - block number
  2. t - fluree \"time\" since ledger creation
  3. sigs - List of transactor signatures that signed this block
  4. instant - instant this block was created, per the transactor.
  5. hash - hash of current block
  6. prev-hash - hash of previous block, if relevant
  7. flakes - list of flakes comprising block
  8. txn - list of transactions in block
  "
  [blocks]
  (loop [[block' & r] blocks result* []]
    (if (nil? block')
      result*
      (let [{:keys [block t flakes]} block'
            prev-hash   (some #(when (= (flake/p %) const/$_block:prevHash) (flake/o %)) flakes)
            hash        (some #(when (= (flake/p %) const/$_block:hash) (flake/o %)) flakes)
            instant     (some #(when (= (flake/p %) const/$_block:instant) (flake/o %)) flakes)
            sigs        (some #(when (= (flake/p %) const/$_block:sigs) (flake/o %)) flakes)
            txn-flakes  (filter #(= (flake/p %) const/$_tx:tx) flakes)
            txn-flakes' (txn-from-flakes txn-flakes)]
        (recur r (conj result* {:block     block
                                :t         t
                                :hash      hash
                                :prev-hash prev-hash
                                :instant   instant
                                :sigs      sigs
                                :flakes    flakes
                                :txn       txn-flakes'}))))))
