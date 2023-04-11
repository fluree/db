(ns fluree.db.query.history
  (:require
   [clojure.core.async :as async]
   [malli.core :as m]
   [fluree.json-ld :as json-ld]
   [fluree.db.constants :as const]
   [fluree.db.datatype :as datatype]
   [fluree.db.dbproto :as dbproto]
   [fluree.db.flake :as flake]
   [fluree.db.query.json-ld.response :as json-ld-resp]
   [fluree.db.util.async :refer [<? go-try]]
   [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
   [fluree.db.util.log :as log]
   [fluree.db.util.validation :as v]
   [fluree.db.query.range :as query-range]
   [fluree.db.db.json-ld :as jld-db]))

(def commit-key-re (re-pattern "^.+[/:#]commit$"))

(defn commit-key
  [commit]
  (re-matches commit-key-re (-> commit keys first)))

(defn history-query-result?
  [v]
  ;; TODO: Move this into malli schema directly when it supports one-to-one
  ;;       key schema -> value schema mapping in maps.
  ;;       https://github.com/metosin/malli/issues/881
  (when (map? v)
    (and
      (if-let [ck (commit-key v)]
        (every? (fn [k] (some #(re-matches % k)
                              (map re-pattern
                                   ["^.+[/:#]id$" "^.+[/:#]address$"
                                    "^.+[/:#]alias$" "^.+[/:#]branch$"
                                    "^.+[/:#]context$"
                                    "^.+[/:#]data$" "^.+[/:#]time$" ".+[/:#]v$"
                                    "^.+[/:#]issuer$"])))
                (-> v (get ck) keys))
        true)
      (every? (fn [k] (some #(re-matches % k)
                            (map re-pattern
                                 [".+[/:#]t$" ".+[/:#]assert$"
                                  "^.+[/:#]retract$"
                                  ".+[/:#]commit$"])))
              (keys v)))))

(def registry
  (merge
    (m/base-schemas)
    (m/sequence-schemas)
    (m/predicate-schemas)
    (m/comparator-schemas)
    v/registry
    {::iri                   ::v/iri
     ::at-context            ::v/at-context
     ::context               ::v/context
     ::t-val                 [:or
                              [:= "latest"]
                              pos-int?
                              [:re datatype/iso8601-datetime-re]]
     ::history-query         [:and
                              [:map
                               ["history" {:optional true}
                                [:orn
                                 [:subject ::iri]
                                 [:flake
                                  [:or
                                   [:catn
                                    [:s ::iri]]
                                   [:catn
                                    [:s [:maybe ::iri]]
                                    [:p ::iri]]
                                   [:catn
                                    [:s [:maybe ::iri]]
                                    [:p ::iri]
                                    [:o [:not :nil]]]]]]]
                               ["commit-details" {:optional true} :boolean]
                               ["@context" {:optional true} ::context]
                               ["t"
                                [:and
                                 [:map
                                  ["from" {:optional true} ::t-val]
                                  ["to" {:optional true} ::t-val]
                                  ["at" {:optional true} ::t-val]]
                                 [:fn {:error/message "Either \"from\" and/or \"to\" OR ONLY \"at\" `t` keys must be provided."}
                                  (fn [{:strs [from to at]}]
                                    ;; if you have at, you cannot have from or to
                                    (if at
                                      (not (or from to))
                                      (or from to)))]
                                 [:fn {:error/message "\"from\" value must be less than or equal to \"to\" value."}
                                  (fn [{:strs [from to]}] (if (and (number? from) (number? to))
                                                            (<= from to)
                                                            true))]]]]
                              [:fn {:error/message "Must supply either a \"history\" or \"commit-details\" key."}
                               (fn [{:strs [history commit-details]}]
                                 (or history commit-details))]]
     ::history-query-result  [:fn history-query-result?]
     ::history-query-results [:sequential ::history-query-result]}))

(def history-query-validator
  (m/validator ::history-query {:registry registry}))

(def history-query-parser
  (m/parser ::history-query {:registry registry}))

(defn history-query?
  "Provide a time range :t and either :history or :commit-details, or both.

  :history - either a subject iri or a vector in the pattern [s p o] with either the
  s or the p is required. If the o is supplied it must not be nil.

  :context - json-ld context to use in expanding the :history iris.

  :commit-details - if true, each result will have a :commit key with the commit map as a value.

  :t  - a map containing either:
  - :at
  - either :from or :to

  accepted values for t maps:
       - positive t-value
       - datetime string
       - :latest keyword "
  [query]
  (history-query-validator query))


(defn s-flakes->json-ld
  "Build a subject map out a set of flakes with the same subject.

  {\"id\" \"ex:foo\" \"ex:x\" 1 \"ex:y\" 2}"
  [db cache context compact fuel error-ch s-flakes]
  (async/go
    (try*
      (let [json-chan (json-ld-resp/flakes->res db cache context compact fuel 1000000
                                                {:wildcard? true, :depth 0}
                                                0 s-flakes)]
        (-> (<? json-chan)
            ;; add the id in case the iri flake isn't present in s-flakes
            (assoc "id" (-> s-flakes first flake/s (->> (dbproto/-iri db)) <?
                            (json-ld/compact compact)))))
      (catch* e
              (log/error e "Error transforming s-flakes.")
              (async/>! error-ch e)))))

(defn t-flakes->json-ld
  "Build a collection of subject maps out of a set of flakes with the same t.

  [{\"id\" \"ex:foo\" \"ex:x\" 1 \"ex:y\" 2}...]
  "
  [db context compact cache fuel error-ch t-flakes]
  (let [s-flakes-ch (->> t-flakes
                         (group-by flake/s)
                         (vals)
                         (async/to-chan!))

        s-out-ch (async/chan)]
    (async/pipeline-async 2
                          s-out-ch
                          (fn [assert-flakes ch]
                            (-> (s-flakes->json-ld db cache context compact fuel error-ch assert-flakes)
                                (async/pipe ch)))
                          s-flakes-ch)
    s-out-ch))

(defn history-flakes->json-ld
  "Build a collection of maps for each t that contains the t along with the asserted and
  retracted subject maps.

  [{\"id\" \"ex:foo\" \"f:assert\" [{},,,} \"f:retract\" [{},,,]]}]
  "
  [db context error-ch flakes]
  (let [fuel  (volatile! 0)
        cache (volatile! {})

        compact (json-ld/compact-fn context)

        out-ch   (async/chan)

        t-flakes-ch (->> flakes
                         (sort-by flake/t >)
                         (group-by flake/t)
                         (vals)
                         (async/to-chan!))
        t-key       (json-ld/compact const/iri-t compact)
        assert-key  (json-ld/compact const/iri-assert compact)
        retract-key (json-ld/compact const/iri-retract compact)]

    (async/pipeline-async 2
                          out-ch
                          (fn [t-flakes ch]
                            (-> (async/go
                                  (try*
                                    (let [{assert-flakes true
                                           retract-flakes false} (group-by flake/op t-flakes)

                                          t (- (flake/t (first t-flakes)))

                                          asserts (->> (t-flakes->json-ld db context compact cache fuel error-ch assert-flakes)
                                                       (async/into [])
                                                       (async/<!))

                                          retracts (->> (t-flakes->json-ld db context compact cache fuel error-ch retract-flakes)
                                                        (async/into [])
                                                        (async/<!))]
                                      {t-key t
                                       assert-key asserts
                                       retract-key retracts})
                                    (catch* e
                                            (log/error e "Error converting history flakes.")
                                            (async/>! error-ch e))))

                                (async/pipe ch)))
                          t-flakes-ch)
    out-ch))

(defn history-pattern
  "Given a parsed query, convert the iris from the query
  to subject ids and return the best index to query against."
  [db context query]
  (go-try
    (let [ ;; parses to [:subject <:id>] or [:flake {:s <> :p <> :o <>}]}
          [query-type parsed-query] query

          {:keys [s p o]} (if (= :subject query-type)
                            {:s parsed-query}
                            parsed-query)

          ids [(when s (<? (dbproto/-subid db (jld-db/expand-iri db s context) true)))
               (when p (<? (dbproto/-subid db (jld-db/expand-iri db p context) true)))
               (when o (jld-db/expand-iri db o context))]

          [s p o] [(get ids 0) (get ids 1) (get ids 2)]
          [pattern idx] (cond
                          (not (nil? s))
                          [ids :spot]

                          (and (nil? s) (not (nil? p)) (nil? o))
                          [[p s o] :psot]

                          (and (nil? s) (not (nil? p)) (not (nil? o)))
                          [[p o s] :post])]
      [pattern idx])))

(defn commit-wrapper-flake?
  "Returns `true` for a flake that represents
  data from the outer wrapper of a commit
  (eg commit message, time, v)"
  [f]
  (= (flake/s f) (flake/t f)))

(defn commit-metadata-flake?
  "Returns `true` if a flake is part of commit metadata.

  These are flakes that we insert which describe
  the data, but are not part of the data asserted
  by the user. "
  [f]
  (#{const/$_commitdata:t
     const/$_commitdata:size
     const/$_previous
     const/$_commitdata:flakes
     const/$_address} (flake/p f)))

(defn extra-data-flake?
  [f]
  (or (= const/$iri (flake/p f))
      (= const/$rdfs:Class (flake/o f))))

(defn commit-t-flakes->json-ld
  "Build a commit maps given a set of all flakes with the same t."
  [db context compact cache fuel error-ch t-flakes]
  (async/go
    (try*
      (let [{commit-wrapper-flakes :commit-wrapper
             commit-meta-flakes    :commit-meta
             assert-flakes         :assert-flakes
             retract-flakes        :retract-flakes}
            (group-by (fn [f]
                        (cond
                          (commit-wrapper-flake? f)                            :commit-wrapper
                          (commit-metadata-flake? f)                           :commit-meta
                          (and (flake/op f) (not (extra-data-flake? f)))       :assert-flakes
                          (and (not (flake/op f)) (not (extra-data-flake? f))) :retract-flakes
                          :else                                                :ignore-flakes))
                      t-flakes)
            commit-wrapper-chan (json-ld-resp/flakes->res db cache context compact fuel 1000000
                                                          {:wildcard? true, :depth 0}
                                                          0 commit-wrapper-flakes)

            commit-meta-chan (json-ld-resp/flakes->res db cache context compact fuel 1000000
                                                       {:wildcard? true, :depth 0}
                                                       0 commit-meta-flakes)


            commit-wrapper (<? commit-wrapper-chan)
            commit-meta    (<? commit-meta-chan)
            asserts        (->> (t-flakes->json-ld db context compact cache fuel error-ch assert-flakes)
                                (async/into [])
                                (async/<!))
            retracts       (->> (t-flakes->json-ld db context compact cache fuel error-ch retract-flakes)
                                (async/into [])
                                (async/<!))

            assert-key  (json-ld/compact const/iri-assert compact)
            retract-key (json-ld/compact const/iri-retract compact)
            data-key    (json-ld/compact const/iri-data compact)
            commit-key  (json-ld/compact const/iri-commit compact)]

        (-> {commit-key commit-wrapper}
            (assoc-in [commit-key data-key] commit-meta)
            (assoc-in  [commit-key data-key assert-key] asserts)
            (assoc-in  [commit-key data-key retract-key] retracts)))
      (catch* e
              (log/error e "Error converting commit flakes.")
              (async/>! error-ch e)))))

(defn commit-flakes->json-ld
  "Create a collection of commit maps."
  [db context error-ch flake-slice-ch]
  (let [fuel    (volatile! 0)
        cache   (volatile! {})
        compact (json-ld/compact-fn context)

        t-flakes-ch (async/chan 1 (comp cat (partition-by flake/t)))
        out-ch     (async/chan)]

    (async/pipe flake-slice-ch t-flakes-ch)
    (async/pipeline-async 2
                          out-ch
                          (fn [t-flakes ch]
                            (-> (commit-t-flakes->json-ld db context compact cache fuel error-ch t-flakes)
                                (async/pipe ch)))
                          t-flakes-ch)
    out-ch))

(defn with-consecutive-ts
  "Return a transducer that processes a stream of history results
  and chunk together results with consecutive `t`s. "
  [t-key]
  (let [last-t (volatile! nil)
        last-partition-val (volatile! true)]
    (partition-by (fn [result]
                    (let [result-t (get result t-key)
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
  [db context error-ch history-results-ch]
  (let [t-key (json-ld/compact const/iri-t context)
        out-ch (async/chan 2 cat)
        chunked-ch (async/chan 2 (with-consecutive-ts t-key))]
    (async/pipe history-results-ch chunked-ch)
    (async/pipeline-async 2
                          out-ch
                          (fn [chunk ch]
                            (async/pipe (async/go
                                          (let [to-t  (- (-> chunk peek (get t-key)))
                                                from-t  (- (-> chunk (nth 0) (get t-key)))
                                                flake-slices-ch (query-range/time-range db :tspo = [] {:from-t from-t :to-t to-t})
                                                consecutive-commit-details (<? (async/into [] (commit-flakes->json-ld db context error-ch flake-slices-ch)))]
                                            (map into chunk consecutive-commit-details)))
                                        ch))
                          chunked-ch)
    out-ch))
