(ns fluree.db.query.history
  (:require 
   [clojure.core.async :as async]
   [malli.core :as m]
   [fluree.json-ld :as json-ld]
   [fluree.db.constants :as const]
   [fluree.db.dbproto :as dbproto]
   [fluree.db.flake :as flake]
   [fluree.db.query.json-ld.response :as json-ld-resp]
   [fluree.db.query.fql.parse :as fql-parse]
   [fluree.db.query.fql.resp :refer [flakes->res]]
   [fluree.db.util.async :refer [<? go-try]]
   [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
   [fluree.db.util.log :as log]))

(def History
  [:map {:registry {::iri [:or :keyword :string]
                    ::context [:map-of :any :any]}}
   [:history
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
   [:context {:optional true} ::context]
   [:t {:optional true}
    [:and
     [:map
      [:from {:optional true} pos-int?]
      [:to {:optional true} pos-int?]]
     [:fn {:error/message "Either \"from\" or \"to\" `t` keys must be provided."}
      (fn [{:keys [from to]}] (or from to))]
     [:fn {:error/message "\"from\" value must be less than or equal to \"to\" value."}
      (fn [{:keys [from to]}] (if (and from to)
                                (<= from to)
                                true))]]]])

(def history-query-validator
  (m/validator History))

(def history-query-parser
  (m/parser History))

(defn history-query?
  "Requires:
  :history - either a subject iri or a vector in the pattern [s p o] with either the
  s or the p is required. If the o is supplied it must not be nil.
  Optional:
  :context - json-ld context to use in expanding the :history iris.
  :t - a map with keys :from and :to, at least one is required if :t is provided."
  [query]
  (history-query-validator query))

(defn t-flakes->json-ld
  [db compact cache fuel error-ch t-flakes]
  (async/go
    (try*
      (let [assert-flakes  (not-empty (filter flake/op t-flakes))
            retract-flakes (not-empty (filter (complement flake/op) t-flakes))

            asserts-chan   (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                     {:wildcard? true, :depth 0}
                                                     0 assert-flakes)
            retracts-chan  (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                     {:wildcard? true, :depth 0}
                                                     0 retract-flakes)

            asserts (<? asserts-chan)
            retracts (<? retracts-chan)

            ;; t is always positive for users
            result         (cond-> {:t (- (flake/t (first t-flakes)))}
                             asserts (assoc :assert asserts)
                             retracts (assoc :retract retracts))]
        result)
      (catch* e
              (log/error e "Error converting history flakes.")
              (async/>! error-ch e)))))

(defn history-flakes->json-ld
  [db q flakes]
  (go-try
    (let [fuel    (volatile! 0)
          cache   (volatile! {})
          compact (json-ld/compact-fn (fql-parse/parse-context q db))

          error-ch   (async/chan)
          out-ch     (async/chan)
          results-ch (async/into [] out-ch)

          t-flakes-ch (->> (sort-by flake/t flakes)
                           (partition-by flake/t)
                           (async/to-chan!))]

      (async/pipeline-async 2
                            out-ch
                            (fn [t-flakes ch]
                              (-> (t-flakes->json-ld db compact cache fuel error-ch t-flakes)
                                  (async/pipe ch)))
                            t-flakes-ch)
      (async/alt!
        error-ch ([e] e)
        results-ch ([result] result)))))

(defn get-history-pattern
  [history]
  (let [[s p o t]     [(get history 0) (get history 1) (get history 2) (get history 3)]
        [pattern idx] (cond
                        (not (nil? s))
                        [history :spot]

                        (and (nil? s) (not (nil? p)) (nil? o))
                        [[p s o t] :psot]

                        (and (nil? s) (not (nil? p)) (not (nil? o)))
                        [[p o s t] :post])]
    [pattern idx]))

(def CommitDetails
  [:map 
   [:commit-details
    [:and
     [:map
      [:from {:optional true} pos-int?]
      [:to {:optional true} pos-int?]]
     [:fn {:error/message "Either \"from\" or \"to\" `t` keys must be provided."}
      (fn [{:keys [from to]}] (or from to))]
     [:fn {:error/message "\"from\" value must be less than or equal to \"to\" value."}
      (fn [{:keys [from to]}] (if (and from to)
                                (<= from to)
                                true))]]]])

(def commit-details-query-validator
  (m/validator CommitDetails))

(def commit-details-query-parser
  (m/parser CommitDetails))

(defn commit-details-query?
  "Requires:
  TODO"
  [query]
  (commit-details-query-validator query))

(defn s-flakes->json-ld
  [db cache compact fuel error-ch s-flakes]
  (async/go
    (try*
      (let [json-chan (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                {:wildcard? true, :depth 0}
                                                0 s-flakes)]
        (-> (<? json-chan)
            ;; add the id in case the iri flake isn't present in s-flakes
            (assoc :id (json-ld/compact (<? (dbproto/-iri db (flake/s (first s-flakes)))) compact))))
      (catch* e
              (log/error e "Error converting history flakes.")
              (async/>! error-ch e)))))

(defn t-flakes->json-ld2
  [db compact cache fuel error-ch t-flakes]
  (go-try
    (let [assert-flakes (not-empty (filter flake/op t-flakes))
          retract-flakes (not-empty (filter (complement flake/op) t-flakes))

          s-asserts-ch (->> (sort-by flake/s assert-flakes)
                            (partition-by flake/s)
                            (async/to-chan!))
          s-retracts-ch (->> (sort-by flake/s retract-flakes)
                             (partition-by flake/s)
                             (async/to-chan!))

          s-asserts-out-ch (async/chan)
          s-retracts-out-ch (async/chan)

          s-asserts-json-ch (async/into [] s-asserts-out-ch)
          s-retracts-json-ch (async/into [] s-retracts-out-ch)]
      ;; process asserts
      (async/pipeline-async 2
                            s-asserts-out-ch
                            (fn [assert-flakes ch]
                              (-> (s-flakes->json-ld db cache compact fuel error-ch assert-flakes)
                                  (async/pipe ch)))
                            s-asserts-ch)
      ;; process retracts
      (async/pipeline-async 2
                            s-retracts-out-ch
                            (fn [retract-flakes ch]
                              (-> (s-flakes->json-ld db cache compact fuel error-ch retract-flakes)
                                  (async/pipe ch)))
                            s-retracts-ch)
      {(json-ld/compact const/iri-t compact) (- (flake/t (first t-flakes)))
       (json-ld/compact const/iri-assert compact) (<? s-asserts-json-ch)
       (json-ld/compact const/iri-retract compact) (<? s-retracts-json-ch)})))

(defn commit-metadata-flake?
  [f]
  (= (flake/s f) (flake/t f)))

(defn commit-data-flake?
  [f]
  (#{const/$_commitdata:t
     const/$_commitdata:size
     const/$_commitdata:flakes} (flake/p f)))

(defn commit-t-flakes->json-ld
  [db compact cache fuel error-ch t-flakes]
  (async/go
    (try*
     (let [{commit-metadata-flakes :commit-meta
            commit-data-flakes :commit-data
            data-flakes :data} (group-by (fn [f]
                                           (cond
                                             (commit-metadata-flake? f) :commit-meta
                                             (commit-data-flake?  f) :commit-data
                                             :else :data)) t-flakes)

           commit-meta-chan (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                      {:wildcard? true, :depth 0}
                                                      0 commit-metadata-flakes)

           commit-data-chan (json-ld-resp/flakes->res db cache compact fuel 1000000
                                                      {:wildcard? true, :depth 0}
                                                      0 commit-data-flakes)
           commit-meta (<? commit-meta-chan)

           commit-data (<? commit-data-chan)

           {asserts (json-ld/compact const/iri-assert compact)
            retracts (json-ld/compact const/iri-retract compact)} (<? (t-flakes->json-ld2 db compact cache fuel error-ch data-flakes))
           ;; t is always positive for users
           result         (assoc {(json-ld/compact const/iri-commit compact) commit-meta}
                                 (json-ld/compact const/iri-commit-data compact ) commit-data)]
       (-> result
           (assoc-in  [(json-ld/compact const/iri-commit-data compact)(json-ld/compact const/iri-assert compact)] asserts)
           (assoc-in  [(json-ld/compact const/iri-commit-data compact)(json-ld/compact const/iri-retract compact)] retracts)))
     (catch* e
             (log/error e "Error converting commit flakes.")
             (async/>! error-ch e)))))

(defn commit-flakes->json-ld
  [db q flakes]
  (def flakes flakes)
  (go-try
    (let [fuel    (volatile! 0)
          cache   (volatile! {})
          compact (json-ld/compact-fn (fql-parse/parse-context q db))

          error-ch   (async/chan)
          out-ch     (async/chan)
          results-ch (async/into [] out-ch)
          non-iri-flakes (remove #(or (= const/$iri (flake/p %))
                                      (= const/$rdfs:Class (flake/o %))) flakes)
          t-flakes-ch (->> (sort-by flake/t non-iri-flakes)
                           (partition-by flake/t)
                           (async/to-chan!))]

      (async/pipeline-async 2
                            out-ch
                            (fn [t-flakes ch]
                              (-> (commit-t-flakes->json-ld db compact cache fuel error-ch t-flakes)
                                  (async/pipe ch)))
                            t-flakes-ch)
      (async/alt!
        error-ch ([e] e)
        results-ch ([result] result)))))
