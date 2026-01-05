(ns fluree.db.query.range
  (:require [clojure.core.async :refer [chan go >!] :as async]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy.query :as policy]
            [fluree.db.track :as track]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? empty-channel go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.schema :as schema-util]))

#?(:clj (set! *warn-on-reflection* true))

(defn- coerce-predicate
  "If a predicate is provided as a string value, coerce to pid"
  [db pred]
  (if (string? pred)
    (or (iri/encode-iri db pred)
        (throw (ex-info (str "Invalid predicate, does not exist: " pred)
                        {:status 400, :error :db/invalid-predicate})))
    pred))

(defn match->flake-parts
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

(def ^{:private true :const true} subject-min-match [flake/min-s])
(def ^{:private true :const true} subject-max-match [flake/max-s])
(def ^{:private true :const true} pred-min-match [flake/min-p])
(def ^{:private true :const true} pred-max-match [flake/max-p])
(def ^{:private true :const true} txn-max-match [flake/min-t])
(def ^{:private true :const true} txn-min-match [flake/max-t])

(defn- min-match
  "Smallest index flake part match by index"
  [idx]
  (case idx
    :spot subject-min-match
    :post pred-min-match
    :psot pred-min-match
    :opst subject-min-match
    :tspo txn-min-match))

(defn- max-match
  "Biggest index flake part match by index"
  [idx]
  (case idx
    :spot subject-max-match
    :post pred-max-match
    :psot pred-max-match
    :opst subject-max-match
    :tspo txn-max-match))

(defn resolve-match-flake
  [test s p o t op m]
  (let [[o' dt] (if (vector? o)
                  [(first o) (second o)]
                  [o nil])
        m' (or m (if (identical? >= test) util/min-integer util/max-integer))]
    (flake/create s p o' dt t op m')))

(defn intersects-range?
  "Returns true if the supplied `node` contains flakes between the `lower` and
  `upper` flakes, according to the `node`'s comparator."
  [node range-set]
  (not (or (and (:rhs node)
                (flake/lower-than-all? (:rhs node) range-set))
           (and (not (:leftmost? node))
                (flake/higher-than-all? (:first node) range-set)))))

(defn extract-query-flakes
  "Returns a transducer to extract flakes from each leaf from a stream of index
  leaf nodes, transformed by the `flake-xf` parameter specified in the supplied
  query options map. The result of the transformation will be a stream of
  collections of flakes from the leaf nodes in the input stream, with one flake
  collection for each input leaf."
  [{:keys [flake-xf] :as _opts}]
  (let [xfs (cond-> [(filter index/resolved-leaf?) (map :flakes)]
              flake-xf (conj (map (fn [flakes]
                                    (into [] flake-xf flakes)))))]
    (apply comp xfs)))

(defn unauthorized?
  [f]
  (= f ::unauthorized))

(defn authorize-flake-exception
  "Wraps upstream exception and logs out a warning message"
  [e db flake]
  (let [e* (ex-info (str "Policy exception authorizing Flake in "
                         (:alias db) "?t=" (:t db)
                         ". " (ex-message e))
                    {:error  :db/policy-exception
                     :status 400
                     :flake  flake}
                    e)]
    (log/warn (ex-message e*))
    e*))

(defn authorize-flake
  [db tracker error-ch flake]
  (go
    (try* (if (or (schema-util/is-schema-flake? db flake)
                  (<? (policy/allow-flake? db tracker flake)))
            flake
            ::unauthorized)
          (catch* e
            (>! error-ch (authorize-flake-exception e db flake))))))

(defn authorize-flakes
  "Authorize each flake in the supplied `flakes` collection asynchronously,
  returning a collection containing only allowed flakes according to the
  policy of the supplied `db`."
  [db tracker error-ch flakes]
  (->> flakes
       (map (partial authorize-flake db tracker error-ch))
       (async/map (fn [& fs]
                    (into [] (remove unauthorized?) fs)))))

#?(:clj
   (defn filter-authorized
     "Returns a channel that will eventually return a stream of flake slices
     containing only the schema flakes and the flakes validated by
     allow-flake? function for the database `db`
     from the `flake-slices` channel"
     [db tracker error-ch flake-slices]
     (if (policy/unrestricted? db)
       flake-slices
       (let [auth-fn (fn [flakes ch]
                       (-> (authorize-flakes db tracker error-ch flakes)
                           (async/pipe ch)))
             out-ch  (chan)]
         (async/pipeline-async 2 out-ch auth-fn flake-slices)
         out-ch)))

   :cljs
   (defn filter-authorized
     "Returns the unfiltered channel `flake-slices`.

     Note: this bypasses all permissions in CLJS for now!"
     [_ _ _ flake-slices]
     flake-slices))

(defn resolve-flake-slices
  "Returns a channel containing a stream of flake collections from the index.

  Options:
  - :to-t        - transaction time upper bound for the range
  - :start-flake - starting flake bound (inclusive)
  - :end-flake   - ending flake bound (inclusive)
  - :prefetch-n  - max concurrent leaf resolves for cold index loads (default 3)
  - :flake-xf    - transducer to apply to each leaf's flakes"
  ([db idx error-ch opts]
   (resolve-flake-slices db nil idx error-ch opts))
  ([{:keys [index-catalog] :as db} tracker idx error-ch
    {:keys [to-t start-flake end-flake prefetch-n] :or {prefetch-n 3} :as opts}]
   (if-let [root (get db idx)]
     (let [novelty   (get-in db [:novelty idx])
           novelty-t (get-in db [:novelty :t])
           resolver  (index/index-catalog->t-range-resolver index-catalog novelty-t novelty to-t)
           query-xf  (extract-query-flakes opts)]
       (->> (index/tree-chan resolver root start-flake end-flake any? prefetch-n query-xf error-ch)
            (filter-authorized db tracker error-ch)))
     empty-channel)))

(defn- subject->spot-range
  "Returns [start-flake end-flake] for a subject SID in :spot ordering."
  [sid]
  [(flake/create sid flake/min-p flake/min-s flake/min-dt flake/min-t flake/min-op flake/min-meta)
   (flake/create sid flake/max-p flake/max-s flake/max-dt flake/max-t flake/max-op flake/max-meta)])

(defn resolve-subject-slices
  "Returns a channel of [sid flakes] pairs by fetching all :spot flakes for each
  subject SID in `sids` (which must be sorted ascending).

  Intended for subject-star joins: first produce a selective subject SID list,
  then fetch all predicate/object flakes for those subjects from :spot and
  filter/join in-memory.

  Notes:
  - Uses the same resolver machinery as `resolve-flake-slices` (honors novelty and :to-t).
  - Applies policy filtering (CLJ) just like `resolve-flake-slices`.

  opts:
  - :to-t        transaction time upper bound
  - :prefetch-n  max concurrent leaf resolves for cold index loads (default 3)
  - :buffer      output buffer size (default 64)"
  [db tracker error-ch sids {:keys [to-t prefetch-n buffer] :or {prefetch-n 3 buffer 64}}]
  (if (empty? sids)
    empty-channel
    (if-let [root (get db :spot)]
      (let [novelty   (get-in db [:novelty :spot])
            novelty-t (get-in db [:novelty :t])
            resolver  (index/index-catalog->t-range-resolver (:index-catalog db) novelty-t novelty to-t)
            raw-ch    (index/batched-prefix-range-lookup resolver root sids subject->spot-range error-ch
                                                         {:mode :seek :prefetch-n prefetch-n :buffer buffer})
            out-ch    (chan buffer)]
        (if (policy/unrestricted? db)
          (async/pipe raw-ch out-ch)
          (async/pipeline-async
           2
           out-ch
           (fn [[sid flakes] ch]
             (go-try
               (let [authorized (<? (authorize-flakes db tracker error-ch flakes))]
                 (>! ch [sid authorized]))))
           raw-ch))
        out-ch)
      empty-channel)))

(defn resolve-subject-predicate-slices
  "Returns a channel of [sid flakes] pairs by fetching all flakes for each
  subject SID in `sids` (sorted ascending) restricted to the single predicate
  `p-sid`.

  Intended for joining a large stream of already-bound subject solutions against
  a fixed predicate (e.g. ?s p ?o) without performing one index seek per
  solution.

  Notes:
  - Uses the same resolver machinery as `resolve-flake-slices` (honors novelty and :to-t).
  - Applies policy filtering (CLJ) just like `resolve-flake-slices`.

  opts:
  - :to-t        transaction time upper bound
  - :prefetch-n  max concurrent leaf resolves for cold index loads (default 3)
  - :buffer      output buffer size (default 64)
  - :mode        :seek or :scan (passed to batched-prefix-range-lookup)
  - :use-psot?   when true and :psot exists, use it; otherwise default to :spot"
  [db tracker error-ch p-sid sids {:keys [to-t prefetch-n buffer mode use-psot?]
                                  :or {prefetch-n 3 buffer 64}}]
  (let [mode* (or mode :seek)]
    (if (or (empty? sids) (nil? p-sid))
      empty-channel
      (let [idx  (if (and use-psot? (get db :psot)) :psot :spot)
            root (get db idx)]
        (if-not root
          empty-channel
          (let [novelty   (get-in db [:novelty idx])
                novelty-t (get-in db [:novelty :t])
                resolver  (index/index-catalog->t-range-resolver (:index-catalog db) novelty-t novelty to-t)
                subject->range
                (fn [sid]
                  ;; Mirror `where/resolve-flake-range` bounds for (s p ?o):
                  ;; only bracket on meta, leave o/dt/t/op nil.
                  [(flake/create sid p-sid nil nil nil nil util/min-integer)
                   (flake/create sid p-sid nil nil nil nil util/max-integer)])
                raw-ch    (index/batched-prefix-range-lookup resolver root sids subject->range error-ch
                                                             {:mode mode* :prefetch-n prefetch-n :buffer buffer})
                out-ch    (chan buffer)]
            (if (policy/unrestricted? db)
              (async/pipe raw-ch out-ch)
              (async/pipeline-async
               2
               out-ch
               (fn [[sid flakes] ch]
                 (go-try
                   (let [authorized (<? (authorize-flakes db tracker error-ch flakes))]
                     (>! ch [sid authorized]))))
               raw-ch))
            out-ch))))))

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
  [db tracker error-ch {:keys [idx limit offset flake-limit] :as opts}]
  (->> (resolve-flake-slices db tracker idx error-ch opts)
       (into-page limit offset flake-limit)))

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
  :from-t - start transaction. Defaults to db's t
  :to-t - stop transaction - can be null, which pulls full history
  :xform - xform applied to each result individually. This is not used
           when :chan is supplied.
  :flake-limit - max number of flakes to return"
  ([db idx test match opts]
   (time-range db nil idx test match opts))
  ([db tracker idx test match opts]
   (let [[start-test start-match end-test end-match]
         (expand-range-interval idx test match)]
     (time-range db tracker idx start-test start-match end-test end-match opts)))
  ([{:keys [t index-catalog] :as db} tracker idx start-test start-match end-test end-match opts]
   (if-let [idx-root (get db idx)]
     (let [{:keys [limit offset flake-limit from-t to-t]
            :or   {from-t t, to-t t}}
           opts

           start-parts (match->flake-parts db idx start-match)
           end-parts   (match->flake-parts db idx end-match)

           start-flake (apply resolve-match-flake start-test start-parts)
           end-flake   (apply resolve-match-flake end-test end-parts)
           error-ch    (chan)

           ;; index-range*
           idx-cmp  (get-in db [:comparators idx])
           novelty  (get-in db [:novelty idx])

           ;; resolve-flake-slices
           {:keys [cache]} index-catalog
           resolver        (index/->CachedHistoryRangeResolver index-catalog t novelty from-t to-t cache)
           range-set       (flake/sorted-set-by idx-cmp start-flake end-flake)
           in-range?       (fn [node] (intersects-range? node range-set))
           query-xf        (extract-query-flakes {:idx         idx
                                                  :start-test  start-test
                                                  :start-flake start-flake
                                                  :end-test    end-test
                                                  :end-flake   end-flake})]
       (go-try
         (let [history-ch (->> (index/tree-chan resolver idx-root start-flake end-flake
                                                in-range? 1 query-xf error-ch)
                               (filter-authorized db tracker error-ch)
                               (into-page limit offset flake-limit))]
           (async/alt!
             error-ch ([e]
                       (throw e))
             history-ch ([hist-range]
                         hist-range)))))
     empty-channel)))

(defn index-range
  "Range query across an index as of a 't' defined by the db.

  Ranges take the natural numeric sort orders, but all results will
  return in reverse order (newest subjects and predicates first).

  Returns core async channel.

  opts:
  :xform - xform applied to each result individually. This is not used when :chan is supplied.
  :limit - max number of flakes to return"
  ([db idx] (index-range db idx {}))
  ([db idx opts] (index-range db nil idx >= (min-match idx) <= (max-match idx) opts))
  ([db idx test match] (index-range db nil idx test match {}))
  ([db idx test match opts] (index-range db nil idx test match opts))
  ([db tracker idx test match opts]
   (let [[start-test start-match end-test end-match]
         (expand-range-interval idx test match)]
     (index-range db tracker idx start-test start-match end-test end-match opts)))
  ([db tracker idx start-test start-match end-test end-match]
   (index-range db tracker idx start-test start-match end-test end-match {}))
  ([{:keys [t] :as db} tracker idx start-test start-match end-test end-match opts]
   (let [[s1 p1 o1 t1 op1 m1]
         (match->flake-parts db idx start-match)

         [s2 p2 o2 t2 op2 m2]
         (match->flake-parts db idx end-match)]

     (go-try
       (let [s1*         (if (or (iri/sid? s1) (nil? s1))
                           s1
                           (iri/encode-iri db s1))
             start-flake (resolve-match-flake start-test s1* p1 o1 t1 op1 m1)
             s2*         (cond
                           (or (iri/sid? s2) (nil? s2))
                           s2

                           (= s2 s1) ; common case when 'test' is =
                           s1*

                           :else
                           (iri/encode-iri db s2))
             end-flake   (resolve-match-flake end-test s2* p2 o2 t2 op2 m2)
             error-ch    (chan)
             track-fuel  (track/track-fuel! tracker error-ch)
             flake-xf*   (->> [(:flake-xf opts) track-fuel]
                              (remove nil?)
                              (apply comp))
             range-ch    (index-range* db
                                       tracker
                                       error-ch
                                       (assoc opts
                                              :idx idx
                                              :to-t t
                                              :start-test start-test
                                              :start-flake start-flake
                                              :end-test end-test
                                              :end-flake end-flake
                                              :flake-xf flake-xf*))]
         (async/alt!
           error-ch ([e]
                     (throw e))
           range-ch ([idx-range]
                     idx-range)))))))
