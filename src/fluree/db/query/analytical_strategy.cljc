(ns fluree.db.query.analytical-strategy
  (:require #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.index :as index]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.query.fql-parser :refer [parse-db]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn- first-where
  "Returns first where clause in parsed query"
  [parsed-query]
  (-> parsed-query
      :where
      first))

(defn- where-subj-xf
  "Transducing function to extract matching subjects from initial where clause."
  [{:keys [novelty from-t to-t object-cache start-test start-flake end-test end-flake]}]
  (comp
    (map :flakes)
    (map (fn [flakes]
           (flake/subrange flakes
                           start-test start-flake
                           end-test end-flake)))
    (map (fn [flakes]
           (map flake/s flakes)))))

;; TODO - what if the first clause has a filter fn in the .-o location?
(defn- subjects-chan
  "Returns chan of subjects in chunks per index-leaf
  that can be pulled as needed based on the selection criteria of a where clause."
  [{:keys [conn novelty t] :as db} error-ch where-clause]
  (let [{:keys [s p o]} where-clause
        p*          (or (dbproto/-p-prop db :id p)
                        (throw (ex-info (str "Invalid predicate in query: " p)
                                        {:status 400 :error :db/invalid-query})))
        o*          (if-let [val (:value o)]
                      val
                      (throw (ex-info (str "TODO: NOT YET IMPLEMENTED " o) o)))
        start-flake (flake/->Flake nil p* o* nil nil -2147483647)
        end-flake   (flake/->Flake nil p* o* nil nil 2147483647)
        idx         :post
        opts        {:object-fn   nil
                     :idx         idx
                     :from-t      t
                     :to-t        t
                     :start-test  >=
                     :start-flake start-flake
                     :end-test    <=
                     :end-flake   end-flake}
        idx-root    (get db idx)
        in-range?   (fn [node]
                      (query-range/intersects-range? node start-flake end-flake))
        query-xf    (where-subj-xf (assoc opts
                                     :novelty novelty
                                     :object-cache (:object-cache conn)))
        resolver    (index/wrap-t-range conn (:async-cache conn) novelty t t)
        tree-chan   (index/tree-chan resolver idx-root in-range? query-range/resolved-leaf? 1 query-xf error-ch)]
    tree-chan))


(defn retrieve-select-spec
  "Returns a parsed selection specification.

  This strategy is only deployed if there is a single selection graph crawl,
  so this assumes this case is true in code."
  [db {:keys [select opts] :as parsed-query}]
  (let [select-smt (-> select
                       :select
                       first
                       :selection)]
    (parse-db db select-smt opts)))

(defn keep-subject?
  "Filters a set of flakes for a single subject and returns true if
  the subject meets the filter map.

  filter-map is a map where pred-ids are keys and values are a list of filtering functions
  where each flake of pred-id must return a truthy value if the subject is allowed.
  "
  [flakes vars filter-map]
  ;; TODO - fns with multiple vars will have to re-calc vars every time, this could be done once for the entire query
  (loop [[f & r] flakes]
    (if f
      (if-let [filter-fns (get filter-map (flake/p f))]
        (when (every? (fn [func] (func f vars)) filter-fns)
          (recur r))
        (recur r))
      true)))


(defn pipeline-select
  "Returns a channel that will eventually return a stream of flake slices
  containing only the schema flakes and the flakes validated by
  fluree.db.permissions-validate/allow-flake? function for the database `db`
  from the `flake-slices` channel"
  [db cache max-n fuel max-fuel select-spec error-ch queue-ch vars filter-map]
  (let [res-ch (async/chan)
        stop!  (fn [e] (when e (async/put! error-ch e)) (async/close! queue-ch) nil)
        af     (fn [sid port]
                 (async/go
                   (try*
                     (let [flakes (<? (query-range/index-range db :spot = [sid]))
                           keep?  (if filter-map
                                    (keep-subject? flakes vars filter-map)
                                    true)]
                       (when keep?
                         (->> (<? (fluree.db.query.fql/flakes->res db cache fuel max-fuel select-spec flakes))
                              (async/put! port))))
                     (async/close! port)
                     (catch* e (stop! e) (async/close! port) nil))))]

    (async/pipeline-async 3 res-ch af queue-ch)

    (async/go
      (loop [acc    []
             max-n* max-n]
        (let [next-res (async/<! res-ch)]
          (cond
            (nil? next-res)
            acc

            (zero? max-n*)
            (do (stop! nil)
                acc)

            (util/exception? next-res)
            (stop! next-res)

            :else
            (let []
              (if (> @fuel max-fuel)
                (stop! (ex-info (str "Query exceeded max fuel while processing: " max-fuel
                                     ". If you have permission, you can set the max fuel for a query with: 'opts': {'fuel' 10000000}")
                                {:error :db/insufficient-fuel :status 400}))
                (recur (conj acc next-res) (dec max-n*))))))))))


(defn simple-subject-crawl
  "Executes a simple subject crawl analytical query execution strategy.

  Strategy involves:
  (a) Get a list of subjects from first where clause
  (b) select all flakes for each subject
  (c) filter subjects based on subsequent where clause(s)
  (d) apply offset/limit for (c)
  (e) send result into :select graph crawl
  "
  [db {:keys [vars where] :as parsed-query}]
  (go-try
    (let [{:keys [limit max-fuel]
           :or   {limit    util/max-integer
                  max-fuel util/max-long}} parsed-query
          error-ch    (async/chan)
          subj-chunks (subjects-chan db error-ch (first where))
          filter-map  (:s-filter (second where))
          cache       (volatile! {})
          fuel        (volatile! 0)
          select-spec (retrieve-select-spec db parsed-query)
          subj-filter (constantly true)]
      (loop [res []
             n   0]
        (let [[subj-chunk ch] (async/alts! [error-ch subj-chunks])]
          (cond
            (= ch error-ch)
            (throw subj-chunk)

            (nil? subj-chunk)
            res

            :else
            (let [queue-ch (async/chan)
                  _        (async/onto-chan! queue-ch subj-chunk)
                  next-res (<? (pipeline-select db cache (- limit n) fuel max-fuel select-spec error-ch queue-ch vars filter-map))
                  res*     (into res next-res)
                  n*       (count res*)]
              (if (= limit n*)
                res*
                (recur res* n*)))))))))




(comment

  (fluree.db.query.analytical-parse/parse db {:select ["?handle", "(as (avg ?nums) avg)"],
                                              :where [["?person", "message/group", "?handle"],
                                                      ["?person", "message/group", "?nums"]],
                                              :groupBy "?handle",
                                              :opts {:prettyPrint true}})
  (fluree.db.query.analytical-parse/parse db {:select {"?var" ["*"]}
                                              :where  [["?var", "message/group", 351843720888321]
                                                       ["?var", "message/updatedAt", "#(> ?time 1642340102)"]],
                                              :opts   {:limit 10}})
  (fluree.db.query.analytical-parse/parse db {:select {"?var" ["*"]}
                                              :where  [["?var", "message/group", 351843720888321]
                                                       ["?var", "message/updatedAt", "?time"]
                                                       {:filter ["#(> ?time 1642340102)"]}],
                                              :opts   {:limit 10}})

  [1642431676585 1642431673298 1642431670102 1642431666579 1642431663551 1642431660604]


  (time
    (def res
      (let [db    (async/<!! pravica3/db)
            query {:select {"?var" ["*"]}
                   :where  [["?var", "message/group", 351843720888321]
                            ["?var", "message/updatedAt", "#(> ?time 1642340102)"]],
                   :opts   {:limit 10}}
            pq    (fluree.db.query.analytical-parse/parse db query)]
        (async/<!! (simple-subject-crawl db pq)))))

  res

  (time
    (def res2
      (let [db    (async/<!! pravica3/db)
            query {:select {"?var" ["*"]}
                   :where  [["?var", "message/group", 351843720888321]
                            ["?var", "message/updatedAt", "?time"]],
                   :filter ["(> ?time 1642340102)"]
                   :opts   {:limit 10}}
            pq    (fluree.db.query.analytical-parse/parse db query)]
        (async/<!! (simple-subject-crawl db pq)))))

  (= res res2)


  (async/<!! (query-range/index-range db :spot = [404620279116670]))

  (-> db

      :schema
      :pred
      (get "message/updatedAt"))

  )


(comment
  (def db (async/<!! pravica3/db))

  (time
    (def res
      (let [db           (async/<!! pravica3/db)
            parsed-query {:select {:select [{:selection ["*"]}]}
                          :where  [{:s '?var
                                    :p 1035
                                    :o {:value 351843720888321}}]
                          :limit  1000}]
        (async/<!! (simple-subject-crawl db parsed-query)))))

  res



  (def subj-chan
    (let [db           (async/<!! pravica3/db)
          where-clause {:s '?var
                        :p 1035
                        :o {:value 351843720888321}}]
      (subjects-chan db nil where-clause)))

  (async/<!! subj-chan)

  )

(comment


  (let [range-opts {:object-fn   nil,
                    :idx         :post,
                    :from-t      -876,
                    :to-t        -876,
                    :start-test  >=,
                    :start-flake #Flake [nil 1035 351843720888321 nil nil -2147483647],
                    :end-test    <=,
                    :end-flake   #Flake [nil 1035 351843720888321 nil nil 2147483647]
                    :limit       100
                    :offset      nil}]
    (->> (query-range/index-range* db nil range-opts)
         async/<!!
         count))

  (def tree-chan
    (let [range-opts {:object-fn   nil,
                      :idx         :post,
                      :from-t      -876,
                      :to-t        -876,
                      :start-test  >=,
                      :start-flake #Flake [nil 1035 351843720888321 nil nil -2147483647],
                      :end-test    <=,
                      :end-flake   #Flake [nil 1035 351843720888321 nil nil 2147483647]}
          {:keys [start-flake end-flake idx]} range-opts
          {:keys [conn novelty]} db
          idx-root   (get db idx)
          in-range?  (fn [node]
                       (query-range/intersects-range? node start-flake end-flake))
          query-xf   (query-range/extract-query-flakes (assoc range-opts
                                                         :novelty novelty
                                                         :object-cache (:object-cache conn)))
          error-ch   nil]

      (index/tree-chan conn idx-root in-range? query-range/resolved-leaf? 1 query-xf error-ch)))

  (async/<!! tree-chan)


  range-opts

  )