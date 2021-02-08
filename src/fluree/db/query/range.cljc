(ns fluree.db.query.range
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj  [clojure.core.async :refer [go chan <! >!] :as async]
               :cljs [cljs.core.async :refer [go chan <! >!] :as async])
            #?(:clj [fluree.db.permissions-validate :as perm-validate])
            [fluree.db.util.async :refer [<? go-try]])
  #?(:clj (:import (fluree.db.flake Flake)))
  #?(:cljs (:require-macros [fluree.db.util.async])))

(defn value-with-nil-pred
  "Checks whether an index range is :spot, starts with [s1 -1 o1] and ends
  with [s1 int/max p1]"
  [idx ^Flake start-flake ^Flake end-flake]
  (and (= :spot idx)
       (not (nil? (.-o start-flake)))
       (= (.-o start-flake) (.-o end-flake))
       (= -1 (.-p start-flake))
       (= flake/MAX-PREDICATE-ID (.-p end-flake))))


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
  "Smallest index flake part match by index"
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

(defn resolve-flake-parts
  [db idx test match]
  (go-try
    (let [[s p o t op m]
          (match->flake-parts db idx match)

          s' (<? (resolve-subid db s))
          o' (<? (resolve-subid db o))
          ;; for >=, start at the beginning of the possible range for exp and for > start at the end
          p' (if (and (nil? p) o) -1 p)
          m' (or m (if (identical? >= test) util/min-integer util/max-integer))]
      [s' p' o' t op m'])))

(defn filter-flakes
  [sorted-flakes ^Flake start-flake ^Flake end-flake idx]
  (if (value-with-nil-pred idx start-flake end-flake)
    (->> sorted-flakes
         (filter (fn [^Flake f]
                   (not= (.-o f) (.-o start-flake))))
         (flake/disj-all sorted-flakes))
    sorted-flakes))

(defn base-subrange
  [next-node idx from-t to-t novelty start-test start-flake end-test end-flake]
  (go-try
   (-> next-node
       (dbproto/-resolve-history-range from-t to-t novelty)
       <?
       (flake/subrange start-test start-flake end-test end-flake)
       (filter-flakes start-flake end-flake idx))))

(defn take-flakes
  [base-result acc limit db i no-filter?]
  (go-try
   (if no-filter?
     (into (flake/take (- limit i) base-result) acc)
     (loop [[f & r] base-result   ;; we must filter, check each flake
            i'   i
            acci base-result]
       (if (or (nil? f) (> i' limit))
         (into acci acc)
         (recur r
                (inc i')
                ;; Note this bypasses all permissions in CLJS for now!
                #?(:cljs acci      ;; always allow for now
                   :clj  (if (<? (perm-validate/allow-flake? db f))
                           acci
                           (disj acci f)))))))))

(defn time-range-chan
  [db idx start-test start-match end-test end-match opts]
  (let [out-chan (chan)]
    (go
      (let [[s1 p1 o1 t1 op1 m1] (<? (resolve-flake-parts db idx start-test start-match))
            [s2 p2 o2 t2 op2 m2] (<? (resolve-flake-parts db idx end-test end-match))

            ;; flip values, because they do have a lexicographical sort order
            start-flake        (flake/->Flake s1 p1 o1 t1 op1 m1)
            end-flake          (flake/->Flake s2 p2 o2 t2 op2 m2)
            limit              (or (:limit opts) util/max-long)
            permissions        (:permissions db)
            idx-compare        (get-in db [:index-configs idx :comparator])
            from-t             (or (:from-t opts) (:t db))
            to-t               (:to-t opts)
            ;; Note this bypasses all permissions in CLJS for now!
            no-filter? #?(:cljs true                         ;; always allow for now
                          :clj (perm-validate/no-filter? permissions s1 s2 p1 p2))
            novelty            (get-in db [:novelty idx])
            root-node          (-> (get db idx)
                                   (dbproto/-resolve)
                                   (<?))]
        (loop [next-flake start-flake
               i          0
               acc        []]
          (let [next-node    (<? (dbproto/-lookup-leaf root-node next-flake))
                rhs          (dbproto/-rhs next-node)        ;; can be nil if at farthest right point
                base-result  (<? (base-subrange next-node idx from-t to-t novelty start-test start-flake end-test end-flake))

                acc*         (<? (take-flakes base-result acc limit db i no-filter?))
                i*           (count acc*)
                more?        (and rhs
                                  (neg? (idx-compare rhs end-flake))
                                  (< i* limit))]
            (if-not more?
              acc*
              (recur rhs i* acc*))))))))

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
   ;; only one test provided, we need to figure out the other test.
   (let [[start-test start-match end-test end-match]
         (condp identical? test
           = [>= match <= match]
           < [> (min-match idx) < match]
           <= [> (min-match idx) <= match]
           > [> match <= (max-match idx)]
           >= [>= match < (max-match idx)])]
     (time-range db idx start-test start-match end-test end-match opts)))
  ([db idx start-test start-match end-test end-match]
   (time-range db idx start-test start-match end-test end-match {}))
  ([db idx start-test start-match end-test end-match opts]
   ;; formulate a comparison flake based on conditions
   (time-range-chan db idx start-test start-match end-test end-match opts)))


(defn subject-groups->allow-flakes
  "Starting with flakes grouped by subject id, filters the flakes until
  either flake-limit or subject-limit reached."
  [db subject-groups flake-start subject-start flake-limit subject-limit]
  (async/go
    (loop [[subject-flakes & r] subject-groups
           flake-count   flake-start
           subject-count subject-start
           acc           []]
      (if (or (nil? subject-flakes) (>= flake-count flake-limit) (>= subject-count subject-limit))
        [flake-count subject-count acc]
        (let [subject-filtered #?(:clj (<? (perm-validate/allow-flakes? db subject-flakes))
                                  :cljs subject-flakes)
              flakes-new-count         (count subject-filtered)
              subject-new-count        (if (= 0 flakes-new-count) 0 1)]
          (recur r (+ flake-count flakes-new-count)
                 (+ subject-count subject-new-count)
                 (into acc subject-filtered)))))))

(defn find-next-valid-node
  [root-node rhs t novelty fast-forward-db?]
  (go-try
    (loop [lookup-leaf (<? (dbproto/-lookup-leaf root-node rhs))]
      (let [node (try*
                   (<? (dbproto/-resolve-to-t lookup-leaf t novelty fast-forward-db?))
                   (catch* e nil))]
        (if node node
                 (if-let [rhs (:rhs lookup-leaf)]
                   (recur (<? (dbproto/-lookup-leaf root-node rhs)))
                   nil))))))

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
   ;; only one test provided, we need to figure out the other test.
   (let [[start-test start-match end-test end-match]
         (condp identical? test
           = [>= match <= match]
           < [> (min-match idx) < match]
           <= [> (min-match idx) <= match]
           > [> match <= (max-match idx)]
           >= [>= match < (max-match idx)])]
     (index-range db idx start-test start-match end-test end-match opts)))
  ([db idx start-test start-match end-test end-match]
   (index-range db idx start-test start-match end-test end-match {}))
  ([db idx start-test start-match end-test end-match opts]
   ;; formulate a comparison flake based on conditions
   (go-try
     (let [[s1 p1 o1 t1 op1 m1] (match->flake-parts db idx start-match)
           [s2 p2 o2 t2 op2 m2] (match->flake-parts db idx end-match)
           {:keys [subject-fn predicate-fn object-fn]} opts
           s1                 (if (util/pred-ident? s1)
                                (<? (dbproto/-subid db s1))
                                s1)
           s2                 (if (util/pred-ident? s2)
                                (<? (dbproto/-subid db s2))
                                s2)
           [[o1 o2] object-fn] (if-let [bool (cond (boolean? o1) o1 (boolean? o2) o2 :else nil)]
                                 [[nil nil] (fn [o] (= o bool))]
                                 [[o1 o2] object-fn])
           o1                 (if (util/pred-ident? o1)
                                (<? (dbproto/-subid db o1))
                                o1)
           o2                 (if (util/pred-ident? o2)
                                (<? (dbproto/-subid db o2))
                                o2)
           ;; for >=, start at the beginning of the possible range for exp and for > start at the end
           p1                 (if (and (nil? p1) o1) -1 p1)
           p2                 (if (and (nil? p2) o2) flake/MAX-PREDICATE-ID p2)
           m1                 (or m1 (if (identical? >= start-test) util/min-integer util/max-integer))
           m2                 (or m2 (if (identical? <= end-test) util/max-integer util/min-integer))
           ;; flip values, because they do have a lexicographical sort order
           start-flake        (flake/->Flake s1 p1 o1 t1 op1 m1)
           end-flake          (flake/->Flake s2 p2 o2 t2 op2 m2)
           {:keys [flake-limit limit offset]
            :or   {flake-limit util/max-long
                   offset      0}} opts
           limit              (or limit util/max-long)
           max-limit?         (= limit util/max-long)
           permissions        (:permissions db)
           idx-compare        (get-in db [:index-configs idx :comparator])
           t                  (:t db)
           novelty            (get-in db [:novelty idx])
           fast-forward-db?   (:tt-id db)
           root-node          (-> (get db idx)
                                  (dbproto/-resolve)
                                  (<?))
           node-start         (<? (find-next-valid-node root-node start-flake t novelty fast-forward-db?))
           no-filter? #?(:cljs true
                         :clj (perm-validate/no-filter? permissions s1 s2 p1 p2))]
       (if node-start (loop [next-node node-start
                             offset    offset               ;; offset counts down from the offset
                             i         0                    ;; i is count of flakes
                             s         0                    ;; s is the count of subjects
                             acc       []]                  ;; acc is all of the flakes we have accumulated thus far
                        (let [base-result  (flake/subrange (:flakes next-node) start-test start-flake end-test end-flake)
                              base-result' (cond->> base-result
                                                    (value-with-nil-pred idx start-flake end-flake) (filter #(= (.-o %) (.-o start-flake)))
                                                    subject-fn (filter #(subject-fn (.-s %)))
                                                    predicate-fn (filter #(predicate-fn (.-p %)))
                                                    object-fn (filter #(object-fn (.-o %))))
                              rhs          (dbproto/-rhs next-node) ;; can be nil if at farthest right point
                              [offset* i* s* acc*] (if (and max-limit? (= 0 offset) no-filter?)
                                                     (let [i+   (count base-result')
                                                           acc* (into acc (take (- flake-limit i) base-result'))]
                                                       ;; we don't care about s if max-limit
                                                       [0 (+ i i+) s acc*])

                                                     (let [partitioned              (partition-by #(.-s %) base-result')
                                                           count-partitioned-result (count partitioned)]
                                                       (if (> offset count-partitioned-result)
                                                         [(- offset count-partitioned-result) i s acc]
                                                         (let [offset-res (drop offset partitioned)
                                                               offset*    0
                                                               [i* s* res-flakes] (if no-filter?
                                                                                    (let [offset-res-count (count offset-res)
                                                                                          subject-count    (+ s offset-res-count)
                                                                                          limit-drop       (- subject-count limit)
                                                                                          [s* limit-take*] (if (pos-int? limit-drop)
                                                                                                             [limit (- offset-res-count limit-drop)]
                                                                                                             [subject-count subject-count])
                                                                                          res-flakes       (->> (take limit-take* offset-res)
                                                                                                                (apply concat))
                                                                                          res-i-count      (count res-flakes)
                                                                                          i*               (+ i res-i-count)
                                                                                          [i* res-flakes] (if (> i* flake-limit)
                                                                                                            [flake-limit (take (- res-i-count (- i* flake-limit))
                                                                                                                               res-flakes)]

                                                                                                            [i* res-flakes])]
                                                                                      [i* s* res-flakes])

                                                                                    ;; if there is a filter, we want to handle limit and filtering
                                                                                    ;; at the same time
                                                                                    (<? (subject-groups->allow-flakes db offset-res i s flake-limit limit)))]
                                                           [offset* i* s* (into acc res-flakes)]))))
                              ;; TODO - handle situation where subject is across multiple nodes...
                              more?        (and rhs
                                                (neg? (idx-compare rhs end-flake))
                                                (< i* flake-limit)
                                                (< s* limit))
                              next-node    (when more?
                                             (<? (find-next-valid-node root-node rhs t novelty fast-forward-db?)))
                              more?        (and more? next-node)]
                          (if-not more?
                            acc*
                            (recur next-node offset* i* s* acc*))))
                      nil)))))

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
  [^Flake f]
  (<= tag-sid-start (.-o f) tag-sid-end))


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
                                    (<? (index-range db :psot = [p s nil t] (assoc opts :object-fn obj-fn))))

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
       (let [id (dbproto/-c-prop db :id name)]
         (if id
           (<? (index-range db :spot
                            >= [(flake/max-subject-id id)]
                            <= [(flake/min-subject-id id)]
                            opts))
           (throw (ex-info (str "Invalid collection name: " (pr-str name))
                           {:status 400
                            :error  :db/invalid-collection}))))
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
      (let [obj     (.-o flake')
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
            prev-hash   (some #(when (= (.-p %) const/$_block:prevHash) (.-o %)) flakes)
            hash        (some #(when (= (.-p %) const/$_block:hash) (.-o %)) flakes)
            instant     (some #(when (= (.-p %) const/$_block:instant) (.-o %)) flakes)
            sigs        (some #(when (= (.-p %) const/$_block:sigs) (.-o %)) flakes)
            txn-flakes  (filter #(= (.-p %) const/$_tx:tx) flakes)
            txn-flakes' (txn-from-flakes txn-flakes)]
        (recur r (conj result* {:block     block
                                :t         t
                                :hash      hash
                                :prev-hash prev-hash
                                :instant   instant
                                :sigs      sigs
                                :flakes    flakes
                                :txn       txn-flakes'}))))))
