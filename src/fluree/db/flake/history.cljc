(ns fluree.db.flake.history
  (:require [clojure.core.async :as async :refer [go >! <!]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.flake.format :as jld-format]
            [fluree.db.flake.index :as index]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.range :as query-range]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn s-flakes->json-ld
  "Build a subject map out a set of flakes with the same subject.
  {:id :ex/foo :ex/x 1 :ex/y 2}"
  [db tracker cache context compact error-ch s-flakes]
  (jld-format/format-subject-flakes db cache context compact
                                    {:wildcard? true, :depth 0}
                                    0 tracker error-ch s-flakes))

(defn t-flakes->json-ld
  "Build a collection of subject maps out of a set of flakes with the same t.

  [{:id :ex/foo :ex/x 1 :ex/y 2}...]
  "
  [db tracker context compact cache error-ch t-flakes]
  (let [s-flakes-ch (->> t-flakes
                         (group-by flake/s)
                         (vals)
                         (async/to-chan!))

        s-out-ch    (async/chan)]
    (async/pipeline-async
     2
     s-out-ch
     (fn [assert-flakes ch]
       (-> db
           (s-flakes->json-ld tracker cache context compact error-ch assert-flakes)
           (async/pipe ch)))
     s-flakes-ch)
    s-out-ch))

(defn history-flakes->json-ld
  "Build a collection of maps for each t that contains the t along with the asserted and
  retracted subject maps.

  [{:id :ex/foo :f/assert [{},,,} :f/retract [{},,,]]}]
  "
  [db tracker context error-ch flakes]
  (let [cache       (volatile! {})

        compact     (json-ld/compact-fn context)

        out-ch      (async/chan)

        t-flakes-ch (->> flakes
                         (sort-by flake/t <)
                         (group-by flake/t)
                         (vals)
                         (async/to-chan!))
        t-key       (json-ld/compact const/iri-fluree-t compact)
        assert-key  (json-ld/compact const/iri-assert compact)
        retract-key (json-ld/compact const/iri-retract compact)]

    (async/pipeline-async
     2
     out-ch
     (fn [t-flakes ch]
       (-> (go
             (try*
               (let [{assert-flakes  true
                      retract-flakes false} (group-by flake/op t-flakes)

                     t        (flake/t (first t-flakes))

                     asserts  (->> assert-flakes
                                   (t-flakes->json-ld db tracker context compact cache error-ch)
                                   (async/into [])
                                   <!)

                     retracts (->> retract-flakes
                                   (t-flakes->json-ld db tracker context compact cache error-ch)
                                   (async/into [])
                                   <!)]
                 {t-key       t
                  assert-key  asserts
                  retract-key retracts})
               (catch* e
                 (log/error! ::history-conversion-error e {:msg "Error converting history flakes."})
                 (log/error e "Error converting history flakes.")
                 (>! error-ch e))))

           (async/pipe ch)))
     t-flakes-ch)
    out-ch))

(defn history-pattern
  "Given a parsed query, convert the iris from the query
  to subject ids and return the best index to query against."
  [db context query]
  (go-try
    (let [;; parses to [:subject <:id>] or [:flake {:s <> :p <> :o <>}]}
          [query-type parsed-query] query

          {:keys [s p o]} (if (= :subject query-type)
                            {:s parsed-query}
                            parsed-query)
          [s p o] [(when s (iri/encode-iri db (json-ld/expand-iri s context)))
                   (when p (iri/encode-iri db (json-ld/expand-iri p context)))
                   (when o (json-ld/expand-iri o context))]

          idx     (index/for-components s p o nil)
          pattern (case idx
                    :spot [s p o]
                    :post [p o s]
                    :opst [o p s])]
      [pattern idx])))

(defn commit-wrapper-flake?
  "Returns `true` for a flake that represents
  data from the outer wrapper of a commit
  (eg commit message, time, v)"
  [f]
  (let [ns-code (-> f flake/s iri/get-ns-code)]
    (contains? iri/commit-namespace-codes ns-code)))

(defn extract-annotation-flakes
  "Removes the annotation flakes from the assert flakes."
  [commit-wrapper-flakes assert-flakes]
  (let [annotation-sid (some->> commit-wrapper-flakes
                                (filter #(= (flake/p %) const/$_commit:annotation))
                                (first)
                                (flake/o))
        {annotation-flakes true
         assert-flakes*    false}
        (group-by (fn [f] (= annotation-sid (flake/s f))) assert-flakes)]
    [assert-flakes* annotation-flakes]))

(defn commit-metadata-flake?
  "Returns `true` if a flake is part of commit metadata.

  These are flakes that we insert which describe
  the data, but are not part of the data asserted
  by the user."
  [f]
  (let [pred (flake/p f)]
    (or (#{const/$_commitdata:t
           const/$_commitdata:size
           const/$_previous
           const/$_commitdata:flakes} pred)
        (= const/$_address pred))))

(defn extra-data-flake?
  [f]
  (= const/$rdfs:Class (flake/o f)))

(defn commit-t-flakes->json-ld
  "Build a commit maps given a set of all flakes with the same t."
  [{:keys [commit-catalog] :as db} tracker context {:keys [commit data txn] :as include} compact cache error-ch t-flakes]
  (go
    (try*
      (let [{commit-wrapper-flakes :commit-wrapper
             commit-meta-flakes    :commit-meta
             assert-flakes         :assert-flakes
             retract-flakes        :retract-flakes}
            (group-by (fn [f]
                        (cond
                          (commit-wrapper-flake? f)
                          :commit-wrapper

                          (commit-metadata-flake? f)
                          :commit-meta

                          (and (flake/op f)
                               (not (extra-data-flake? f)))
                          :assert-flakes

                          (and (not (flake/op f))
                               (not (extra-data-flake? f)))
                          :retract-flakes

                          :else
                          :ignore-flakes))
                      t-flakes)
            [_ annotation-flakes] (extract-annotation-flakes commit-wrapper-flakes assert-flakes)

            commit-wrapper-chan (jld-format/format-subject-flakes db cache context compact
                                                                  {:wildcard? true, :depth 0}
                                                                  0 tracker error-ch commit-wrapper-flakes)

            commit-meta-chan    (jld-format/format-subject-flakes db cache context compact
                                                                  {:wildcard? true, :depth 0}
                                                                  0 tracker error-ch commit-meta-flakes)

            commit-wrapper      (<! commit-wrapper-chan)
            commit-meta         (<! commit-meta-chan)
            asserts             (->> assert-flakes
                                     (t-flakes->json-ld db tracker context compact cache error-ch)
                                     (async/into [])
                                     <!)
            retracts            (->> retract-flakes
                                     (t-flakes->json-ld db tracker context compact cache error-ch)
                                     (async/into [])
                                     <!)
            annotation          (<? (t-flakes->json-ld db tracker context compact cache error-ch annotation-flakes))

            assert-key          (json-ld/compact const/iri-assert compact)
            retract-key         (json-ld/compact const/iri-retract compact)
            t-key               (json-ld/compact const/iri-fluree-t compact)
            data-key            (json-ld/compact const/iri-data compact)
            commit-key          (json-ld/compact const/iri-commit compact)
            annotation-key      (json-ld/compact const/iri-annotation compact)]
        (if include
          (cond-> (if txn
                    (let [txn-key     (json-ld/compact const/iri-txn compact)
                          txn-address (get commit-wrapper txn-key)
                          raw-txn     (when txn-address
                                        (<? (storage/read-json commit-catalog txn-address)))]
                      (assoc {} txn-key raw-txn))
                    {})
            commit (-> (assoc commit-key commit-wrapper)
                       (assoc-in [commit-key data-key] commit-meta)
                       (cond-> annotation (assoc-in [commit-key annotation-key] annotation)))
            data   (-> (assoc-in [data-key assert-key] asserts)
                       (assoc-in [data-key retract-key] retracts)
                       (assoc-in [data-key t-key] (get commit-meta t-key))))
          (-> {commit-key commit-wrapper}
              (assoc-in [commit-key data-key] commit-meta)
              (assoc-in [commit-key data-key assert-key] asserts)
              (assoc-in [commit-key data-key retract-key] retracts)
              (cond-> annotation (assoc-in [commit-key annotation-key] annotation)))))
      (catch* e
        (log/error! ::commit-conversion-error e {:msg "Error converting commit flakes."})
        (log/error e "Error converting commit flakes.")
        (>! error-ch e)))))

(defn commit-flakes->json-ld
  "Create a collection of commit maps."
  [db tracker context include error-ch flake-slice-ch]
  (let [cache       (volatile! {})
        compact     (json-ld/compact-fn context)

        t-flakes-ch (async/chan 1 (comp cat (partition-by flake/t)))
        out-ch      (async/chan)]

    (async/pipe flake-slice-ch t-flakes-ch)
    (async/pipeline-async
     2
     out-ch
     (fn [t-flakes ch]
       (-> (commit-t-flakes->json-ld db tracker context include compact cache error-ch
                                     t-flakes)
           (async/pipe ch)))
     t-flakes-ch)
    out-ch))

(defn with-consecutive-ts
  "Return a transducer that processes a stream of history results
  and chunk together results with consecutive `t`s. "
  [t-key]
  (let [last-t             (volatile! nil)
        last-partition-val (volatile! true)]
    (partition-by (fn [result]
                    (let [result-t     (get result t-key)
                          chunk-last-t @last-t]
                      (vreset! last-t result-t)
                      (if (or (nil? chunk-last-t)
                              (= chunk-last-t (dec result-t)))
                        ;;partition-by will not create a new paritition
                        ;;if returned value is the same as before
                        @last-partition-val
                        ;; partition-by will create a new partition
                        (vswap! last-partition-val not)))))))

(defn add-commit-details
  "Adds commit-details to history results from the history-results-ch.
  Chunks together history results with consecutive `t`s to reduce `time-range`
  calls. "
  [db tracker context include error-ch history-results-ch]
  (let [t-key      (json-ld/compact const/iri-fluree-t context)
        out-ch     (async/chan 2 cat)
        chunked-ch (async/chan 2 (with-consecutive-ts t-key))]
    (async/pipe history-results-ch chunked-ch)
    (async/pipeline-async
     2
     out-ch
     (fn [chunk ch]
       (async/pipe
        (go
          (let [to-t                       (-> chunk peek (get t-key))
                from-t                     (-> chunk (nth 0) (get t-key))
                flake-slices-ch            (query-range/time-range
                                            db tracker :tspo = []
                                            {:from-t from-t, :to-t to-t})
                consecutive-commit-details (<! (->> flake-slices-ch
                                                    (commit-flakes->json-ld
                                                     db tracker context include error-ch)
                                                    (async/into [])))]
            (map into chunk consecutive-commit-details)))
        ch))
     chunked-ch)
    out-ch))

(defn query-history
  [db tracker context from-t to-t commit-details? include error-ch history-q]
  (go-try
    (let [[pattern idx]  (<? (history-pattern db context history-q))
          flake-slice-ch (query-range/time-range db tracker idx = pattern {:from-t from-t :to-t to-t})
          flakes         (async/<! (async/reduce into [] flake-slice-ch))
          result-ch      (cond->> (history-flakes->json-ld db tracker context error-ch flakes)
                           (or commit-details? include) (add-commit-details db tracker context include error-ch)
                           true            (async/into []))]
      (<! result-ch))))

(defn query-commits
  [db tracker context from-t to-t include error-ch]
  (let [flake-slice-ch    (query-range/time-range db tracker :tspo = [] {:from-t from-t :to-t to-t})
        commit-results-ch (commit-flakes->json-ld db tracker context include error-ch flake-slice-ch)]
    (async/into [] commit-results-ch)))
