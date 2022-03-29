(ns fluree.db.query.subject-crawl.core
  (:require #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.query.fql-parser :refer [parse-db]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.query.subject-crawl.subject :refer [subj-crawl]]
            [fluree.db.query.subject-crawl.rdf-type :refer [rdf-type-crawl]]))

#?(:clj (set! *warn-on-reflection* true))

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


(defn simple-subject-crawl
  "Executes a simple subject crawl analytical query execution strategy.

  Strategy involves:
  (a) Get a list of subjects from first where clause
  (b) select all flakes for each subject
  (c) filter subjects based on subsequent where clause(s)
  (d) apply offset/limit for (c)
  (e) send result into :select graph crawl
  "
  [db {:keys [vars where limit offset fuel] :as parsed-query}]
  (let [error-ch    (async/chan)
        f-where     (first where)
        rdf-type?   (= :rdf/type (:type f-where))
        filter-map  (:s-filter (second where))
        cache       (volatile! {})
        fuel-vol    (volatile! 0)
        select-spec (retrieve-select-spec db parsed-query)
        opts        {:rdf-type?     rdf-type?
                     :db            db
                     :cache         cache
                     :fuel-vol      fuel-vol
                     :max-fuel      fuel
                     :select-spec   select-spec
                     :error-ch      error-ch
                     :vars          vars
                     :filter-map    filter-map
                     :limit         limit
                     :offset        offset
                     :permissioned? (not (get-in db [:permissions :root?]))
                     :parallelism   3}]
    (if rdf-type?
      (rdf-type-crawl f-where error-ch opts)
      (subj-crawl f-where error-ch opts))))


(comment

  (def db (async/<!! pravica3/db))

  ;; time @1000 perm-db: 24,000ms
  ;; time @1000 db: 290 ms
  ;; time @100 db: 33-40ms
  (time
    (def res
      (let [db    (async/<!! pravica3/db)
            query {:select {"?var" ["*"]}
                   :where  [["?var", "message/group", 351843720888321]
                            ["?var", "message/updatedAt", "#(> ?time 1642340102)"]],
                   :opts   {:limit 110}}
            pq    (fluree.db.query.analytical-parse/parse db query)]
        (async/<!! (simple-subject-crawl db pq)))))



  res

  (time
    (def res
      (let [db    (async/<!! pravica3/db)
            query {:select {"?var" ["*"]}
                   :where  [["?var", "rdf:type", "_user"]]
                   :opts {:limit 2 :offset 4}}
            pq    (fluree.db.query.analytical-parse/parse db query)]
        (async/<!! (simple-subject-crawl db pq)))))

  ;; time perm-db: 43-50ms
  ;; time db: 14-22 ms
  (time
    (def res
      (let [db    (async/<!! pravica3/db)
            query {:select {"?var" ["*", {"_user/auth" ["*"]}]}
                   :where  [["?var", "_user/username", nil]]}
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

  (count res2)

  )


