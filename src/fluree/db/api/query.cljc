(ns fluree.db.api.query
  "Primary API ns for any user-invoked actions. Wrapped by language & use specific APIS
  that are directly exposed"
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.fql.parse :as fql-parse]
            [fluree.db.query.range :as query-range]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :as async-util :refer [<? go-try]]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.json-ld :as json-ld]
            [fluree.db.db.json-ld :as jld-db]
            [malli.core :as m]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            [fluree.db.datatype :as datatype]))

#?(:clj (set! *warn-on-reflection* true))

;; main query interface for APIs, etc.


(declare query)


(defn db-ident?
  [source]
  (= (-> source (str/split #"/") count) 2))


(defn- isolate-ledger-id
  [ledger-id]
  (re-find #"[a-z0-9]+/[a-z0-9]+" ledger-id))



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

(defn t-flakes->json-ld
  [db compact cache fuel error-ch t-flakes]
  (go-try
    (let [{assert-flakes  true
           retract-flakes false} (group-by flake/op t-flakes)

          s-flake-partitions (fn [flakes]
                               (->> flakes
                                    (group-by flake/s)
                                    (vals)
                                    (async/to-chan!)))

          s-asserts-ch  (s-flake-partitions assert-flakes)
          s-retracts-ch (s-flake-partitions retract-flakes)

          s-asserts-out-ch  (async/chan)
          s-retracts-out-ch (async/chan)

          s-asserts-json-ch  (async/into [] s-asserts-out-ch)
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
       (json-ld/compact const/iri-assert compact) (async/<! s-asserts-json-ch)
       (json-ld/compact const/iri-retract compact) (async/<! s-retracts-json-ch)})))

(defn history-flakes->json-ld
  [db q flakes]
  (go-try
    (let [fuel    (volatile! 0)
          cache   (volatile! {})
          context (fql-parse/parse-context q db)
          compact (json-ld/compact-fn context)

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
      [:from {:optional true} [:or
                               pos-int?
                               datatype/iso8601-datetime-re]]
      [:to {:optional true} [:or
                             pos-int?
                             datatype/iso8601-datetime-re]]]
     [:fn {:error/message "Either \"from\" or \"to\" `t` keys must be provided."}
      (fn [{:keys [from to]}] (or from to))]
     [:fn {:error/message "\"from\" value must be less than or equal to \"to\" value."}
      (fn [{:keys [from to]}] (if (and from to (number? from) (number? to))
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

(defn history
  [db query-map]
  (go-try
    (if-not (history-query? query-map)
      (throw (ex-info (str "History query not properly formatted. Provided "
                           (pr-str query-map))
                      {:status 400
                       :error  :db/invalid-query}))

      (let [{:keys [history t context]} (history-query-parser query-map)

            ;; parses to [:subject <:id>] or [:flake {:s <> :p <> :o <>}]}
            [query-type parsed-query] history

            {:keys [s p o]} (if (= :subject query-type)
                              {:s parsed-query}
                              parsed-query)

            query [(when s (<? (dbproto/-subid db (jld-db/expand-iri db s context) true)))
                   (when p (jld-db/expand-iri db p context))
                   (when o (jld-db/expand-iri db o context))]

            [pattern idx] (get-history-pattern query)

            ;; from and to are positive ints, need to convert to negative or fill in default values
            {:keys [from to]} t
            [from-t to-t]     [(cond (string? from) (<? (time-travel/datetime->t db from))
                                     (number? from) (- from)
                                     :else          -1)
                               (cond (string? to) (<? (time-travel/datetime->t db to))
                                     (number? to) (- to)
                                     :else        (:t db))]
            flakes            (<? (query-range/time-range db idx = pattern {:from-t from-t :to-t to-t}))
            results           (<? (history-flakes->json-ld db query-map flakes))]
        results))))

(defn query
  "Execute a query against a database source, or optionally
  additional sources if the query spans multiple data sets.
  Returns core async channel containing result."
  [sources query]
  (go-try
   (let [{query :subject, issuer :issuer}
         (or (<? (cred/verify query))
             {:subject query})

         {:keys [opts t]} query
         db               (if (async-util/channel? sources) ;; only support 1 source currently
                            (<? sources)
                            sources)
         db*              (-> (if t
                                (<? (time-travel/as-of db t))
                                db)
                              (assoc-in [:policy :cache] (atom {})))
          meta?         (:meta opts)
          opts*         (assoc opts :issuer issuer)
          start         #?(:clj (System/nanoTime)
                           :cljs (util/current-time-millis))
         result        (<? (fql/query db* (assoc query :opts opts*)))]
      (if meta?
        {:status 200
         :result result
         :time   (util/response-time-formatted start)}
        result))))

(defn multi-query
  "Performs multiple queries in a map, with the key being the alias for the query
  and the value being the query itself. Each query result will be in a response
  map with its respective alias as the key.

  If any errors occur, an :errors key will be present with a map of each alias
  to its error information. Check for the presence of this key if detection of
  an error is important.

  An optional :opts key contains options, which for now is limited to:
   - meta: true or false - If false, will just report out the result as a map.
           If true will roll up all status. Response map will contain keys:
           - status - aggregate status (200 all good, 207 some good, or 400+ for differing errors
           - result - query result
           - errors - map of query alias to their respective error"
  [source flureeQL]
  (go-try
   (let [global-meta?       (get-in flureeQL [:opts :meta]) ;; if true, need to collect meta for each query to total up
         ;; update individual queries for :meta if not otherwise specified
         queries            (reduce-kv
                             (fn [acc alias query]
                               (let [query-meta?  (get-in query [:opts :meta])
                                     meta?        (or global-meta? query-meta?)
                                     remove-meta? (and meta? (not query-meta?)) ;; query didn't ask for meta, but multiquery did so must strip it

                                     opts*        (assoc (:opts query) :meta meta?
                                                         :-remove-meta? remove-meta?)
                                     query*       (assoc query :opts opts*)]
                                 (assoc acc alias query*)))
                             {} (dissoc flureeQL :opts))
         start-time #?(:clj (System/nanoTime) :cljs (util/current-time-millis))
         ;; kick off all queries in parallel, each alias now mapped to core async channel
         pending-resp       (map (fn [[alias q]] [alias (query source q)]) queries)]
     (loop [[[alias port] & r] pending-resp
            status-global nil                            ;; overall status.
            response      {}]
       (if (nil? port)                                   ;; done?
         (if global-meta?
           {:result response
            :status status-global
            :time   (util/response-time-formatted start-time)}
           response)
         (let [{:keys [meta -remove-meta?]} (get-in queries [alias :opts])
               res            (async/<! port)
               error?         (:error res)               ;; if error key is present in response, it is an error
               status-global* (when meta
                                (let [status (:status res)]
                                  (cond
                                    (nil? status-global)
                                    status

                                    (= status-global status)
                                    status

                                    ;; any 200 response with any other is a 207
                                    (or (= 200 status) (= 200 status-global) (= 207 status-global))
                                    207

                                    ;; else take the max status
                                    :else
                                    (max status status-global))))
               response*      (if error?
                                (assoc-in response [:errors alias] res)
                                (assoc response alias (if -remove-meta?
                                                        (:result res)
                                                        res)))]
           (recur r status-global* response*)))))))
