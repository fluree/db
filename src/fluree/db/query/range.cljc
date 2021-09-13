(ns fluree.db.query.range
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.iri :as iri-util])
  #?(:clj (:import (fluree.db.flake Flake)))
  #?(:cljs (:require-macros [fluree.db.util.async])))

#?(:clj (set! *warn-on-reflection* true))


(defn value-with-nil-pred
  "Checks whether an index range is :spot, starts with [s1 -1 o1] and ends with [s1 int/max p1]"
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
        (throw (ex-info (str "Invalid predicate, does not exist: " p) {:status 400 :error :db/invalid-predicate})))))


(defn- match->flake-parts
  "Takes a match from index-range, and based on the index
  returns flake-ordered components of [s p o t op m].
  Coerces idents and string predicate names."
  [db idx match]
  (let [[p1 p2 p3 t op m] match]
    (case idx
      :spot [p1 (pred-id-strict db p2) p3 t op m]
      :psot [p2 (pred-id-strict db p1) p3 t op m]
      :post [p3 (pred-id-strict db p1) p2 t op m]
      :opst [p3 (pred-id-strict db p2) p1 t op m])))



(def ^{:private true :const true} subject-min-match [util/max-long])
(def ^{:private true :const true} subject-max-match [util/min-long])
(def ^{:private true :const true} pred-min-match [0])
(def ^{:private true :const true} pred-max-match [flake/MAX-PREDICATE-ID])


(defn- min-match
  "Smallest index flake part match by index"
  [idx]
  (case idx
    :spot subject-min-match
    :psot pred-min-match
    :post pred-min-match
    :opst subject-min-match))


(defn- max-match
  "Smallest index flake part match by index"
  [idx]
  (case idx
    :spot subject-max-match
    :psot pred-max-match
    :post pred-max-match
    :opst subject-max-match))


(defn time-range
  "Range query across an index.

  Uses a DB, but in the future support supplying a connection and db name, as we don't need a 't'

  Ranges take the natural numeric sort orders, but all results will
  return in reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :from-t - start transaction (transaction 't' is negative, so smallest number is most recent). Defaults to db's t
  :to-t - stop transaction - can be null, which pulls full history
  :xform - xform applied to each result individually. This is not used when :chan is supplied.
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
   (go-try
     (let [[s1 p1 o1 t1 op1 m1] (match->flake-parts db idx start-match)
           [s2 p2 o2 t2 op2 m2] (match->flake-parts db idx end-match)
           s1                 (if (util/pred-ident? s1)
                                (<? (dbproto/-subid db s1))
                                s1)
           s2                 (if (util/pred-ident? s2)
                                (<? (dbproto/-subid db s2))
                                s2)
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
           ^Flake start-flake (flake/->Flake s1 p1 o1 t1 op1 m1)
           end-flake          (flake/->Flake s2 p2 o2 t2 op2 m2)
           limit              (or (:limit opts) util/max-long)
           permissions        (:permissions db)
           idx-compare        (get-in db [:index-configs idx :comparator])
           from-t             (or (:from-t opts) (:t db))
           to-t               (:to-t opts)
           ;; Note this bypasses all permissions in CLJS for now!
           no-filter?         #?(:clj (perm-validate/no-filter? permissions s1 s2 p1 p2)
                                 :cljs (if (identical? *target* "nodejs")
                                         (perm-validate/no-filter? permissions s1 s2 p1 p2)
                                         ;; always allow for browser-mode
                                         true))
           novelty            (get-in db [:novelty idx])
           root-node          (-> db
                                  (get idx)
                                  dbproto/-resolve
                                  <?)]
       (loop [next-node (<? (dbproto/-lookup-leaf root-node start-flake))
              i         0
              acc       nil]
         (let [flakes       (<? (dbproto/-resolve-history-range next-node from-t to-t novelty))
               base-result  (flake/subrange flakes start-test start-flake end-test end-flake)
               base-result' (if (value-with-nil-pred idx start-flake end-flake)
                              (reduce
                                (fn [filtered-result ^Flake f]
                                  (if (= (.-o f) (.-o start-flake))
                                    filtered-result
                                    (disj filtered-result f)))
                                base-result base-result)
                              base-result)

               rhs          (dbproto/-rhs next-node)        ;; can be nil if at farthest right point
               acc*         (if no-filter?
                              (into (flake/take (- limit i) base-result') acc)
                              (loop [[f & r] base-result'   ;; we must filter, check each flake
                                     i'   i
                                     acci base-result']
                                (if (or (nil? f) (> i' limit))
                                  (into acci acc)
                                  (recur r
                                         (inc i')
                                         ;; Note this bypasses all permissions in CLJS (browser) for now!
                                         #?(:clj  (if (<? (perm-validate/allow-flake? db f))
                                                    acci
                                                    (disj acci f))
                                            :cljs (if (identical? *target* "nodejs")
                                                    ; check permissions for nodejs
                                                    (if (<? (perm-validate/allow-flake? db f))
                                                      acci
                                                      (disj acci f))
                                                    ; always include for browser
                                                    acci))))))
               i*           (count acc*)
               more?        (and rhs
                                 (neg? (idx-compare rhs end-flake))
                                 (< i* limit))]
           (if-not more?
             acc*
             (recur (<? (dbproto/-lookup-leaf root-node rhs)) i* acc*))))))))


(defn subject-groups->allow-flakes
  "Starting with flakes grouped by subject id, filters the flakes until
  either flake-limit or subject-limit reached."
  [db subject-groups flake-start subject-start flake-limit subject-limit]
  (go-try
    (loop [[subject-flakes & r] subject-groups
           flake-count   flake-start
           subject-count subject-start
           acc           []]
      (if (or (nil? subject-flakes) (>= flake-count flake-limit) (>= subject-count subject-limit))
        [flake-count subject-count acc]
        (let [subject-filtered #?(:clj (<? (perm-validate/allow-flakes? db subject-flakes))
                                  :cljs (if (identical? *target* "nodejs")
                                          (<? (perm-validate/allow-flakes? db subject-flakes))
                                          subject-flakes))
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
           [[o1 o2] object-fn] (if-some [bool (cond (boolean? o1) o1 (boolean? o2) o2 :else nil)]
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
           ^Flake start-flake (flake/->Flake s1 p1 o1 t1 op1 m1)
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
           no-filter?         #?(:clj (perm-validate/no-filter? permissions s1 s2 p1 p2)
                                 :cljs (if (identical? *target* "nodejs")
                                         (perm-validate/no-filter? permissions s1 s2 p1 p2)
                                         true))]
       (if node-start (loop [next-node node-start
                             offset    offset               ;; offset counts down from the offset
                             i         0                    ;; i is count of flakes
                             s         0                    ;; s is the count of subjects
                             acc       []]                  ;; acc is all of the flakes we have accumulated thus far
                        (let [base-result  (flake/subrange (:flakes next-node) start-test start-flake end-test end-flake)
                              base-result' (cond->> base-result

                                                    (value-with-nil-pred idx start-flake end-flake)
                                                    (filter #(= (.-o ^Flake %) (.-o start-flake)))

                                                    subject-fn
                                                    (filter #(subject-fn (.-s ^Flake %)))

                                                    predicate-fn
                                                    (filter #(predicate-fn (.-p ^Flake %)))

                                                    object-fn
                                                    (filter #(object-fn (.-o ^Flake %))))
                              rhs          (dbproto/-rhs next-node) ;; can be nil if at farthest right point
                              [offset* i* s* acc*] (if (and max-limit? (= 0 offset) no-filter?)
                                                     (let [i+   (count base-result')
                                                           acc* (into acc (take (- flake-limit i) base-result'))]
                                                       ;; we don't care about s if max-limit
                                                       [0 (+ i i+) s acc*])

                                                     (let [partitioned              (partition-by #(.-s ^Flake %) base-result')
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
  (and (some? o)
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
  "Coerces a list of tag flakes into flakes that contain the tag name (not subj id) as the .-o value."
  [db flakes]
  (go-try
    (loop [[^Flake flake & r] flakes
           acc []]
      (if flake
        (->> (<? (dbproto/-tag db (:o flake) (:p flake)))
             (assoc flake :o)
             (conj acc)
             (recur r))
        acc))))


(defn coerce-tag-object
  "When a predicate is type :tag and the query object (o) is a string,
  resolves the tag string to a tag subject id (sid)."
  [db p o-string]
  (go-try
    (if (tag-string? o-string)
      ;; Returns tag-id
      (<? (dbproto/-tag-id db o-string))
      ;; if string, but not tag string, we have a string
      ;; like "query" with no namespace, we need to ns.
      (let [tag-name (str (dbproto/-p-prop db :name p) ":" o-string)]
        (<? (dbproto/-tag-id db tag-name))))))



(defn search
  ([db fparts]
   (search db fparts {}))
  ([db fparts {:keys [context object-fn] :as opts}]
   (go-try (let [[s p o t] fparts
                 pid            (when p (iri-util/class-sid p db context))
                 idx-predicate? (dbproto/-p-prop db :idx? pid)
                 ref?           (if p (dbproto/-p-prop db :ref? pid) false) ;; ref? is either a type :tag or :ref
                 o-coerce?      (and ref? (string? o))
                 o              (cond (not o-coerce?)
                                      o

                                      (= :tag (dbproto/-p-prop db :type pid))
                                      (<? (coerce-tag-object db pid o))

                                      :else                 ;; type is :ref, supplied iri
                                      (<? (dbproto/-subid db [const/$iri o])))

                 s*             (cond (string? s)
                                      (<? (dbproto/-subid db s))

                                      (util/pred-ident? s)
                                      (<? (dbproto/-subid db s))

                                      :else s)

                 res            (cond
                                  s
                                  (if (nil? s*)             ;; subject could not be resolved, no results
                                    nil
                                    (<? (index-range db :spot = [s* pid o t] opts)))

                                  (and idx-predicate? (non-nil-non-boolean? o) (not (fn? o)))
                                  (<? (index-range db :post = [pid o s* t] opts))

                                  (and p (not idx-predicate?) o)
                                  (let [obj-fn (if (boolean? o)
                                                 (if object-fn
                                                   (fn [x] (and (= x o) (object-fn x)))
                                                   (fn [x] (= x o)))
                                                 object-fn)]
                                    ;; check for special case where search specifies _id and an integer, i.e. [nil _id 12345]
                                    (if (and (= "_id" p) (int? o))
                                      ;; TODO - below should not need a `take 1` - `:limit 1` does not work properly - likely fixed in tsop branch, remove take 1 once :limit works
                                      (take 1 (<? (index-range db :spot = [o] (assoc opts :limit 1))))
                                      (<? (index-range db :psot = [pid s nil t] (assoc opts :object-fn obj-fn)))))

                                  pid
                                  (<? (index-range db :psot = [pid s* o t] opts))

                                  o
                                  (<? (index-range db :opst = [o pid s* t] opts)))]
             (cond
               (and ref? (= :tag (dbproto/-p-prop db :type pid)))
               (<? (coerce-tag-flakes db res))

               (= "@id" p)
               (map #(assoc % :o (iri-util/compact (:o %) context)) res)

               :else res)))))


(defn collection
  "Returns spot index range for only the requested collection."
  ([db name] (collection db name nil))
  ([db name opts]
   (go
     (try*
       (if-let [partition (dbproto/-c-prop db :partition name)]
         (<? (index-range db :spot
                          >= [(flake/max-subject-id partition)]
                          <= [(flake/min-subject-id partition)]
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
      (let [obj     (.-o ^Flake flake')
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
            prev-hash   (some
                          #(let [f ^Flake %]
                             (when (= (.-p f) const/$_block:prevHash)
                               (.-o f)))
                          flakes)
            hash        (some
                          #(let [f ^Flake %]
                             (when (= (.-p f) const/$_block:hash)
                               (.-o f)))
                          flakes)
            instant     (some
                          #(let [f ^Flake %]
                             (when (= (.-p f) const/$_block:instant)
                               (.-o f)))
                          flakes)
            sigs        (some
                          #(let [f ^Flake %]
                             (when (= (.-p f) const/$_block:sigs)
                               (.-o f)))
                          flakes)
            txn-flakes  (filter #(= (.-p ^Flake %) const/$_tx:tx) flakes)
            txn-flakes' (txn-from-flakes txn-flakes)]
        (recur r (conj result* {:block     block
                                :t         t
                                :hash      hash
                                :prev-hash prev-hash
                                :instant   instant
                                :sigs      sigs
                                :flakes    flakes
                                :txn       txn-flakes'}))))))

