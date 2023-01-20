(ns fluree.db.query.history
  (:require 
   [clojure.core.async :as async]
   [malli.core :as m]
   [fluree.json-ld :as json-ld]
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

