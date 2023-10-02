(ns fluree.db.query.subject-crawl.subject
  (:require [clojure.core.async :refer [<! >!] :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.index :as index]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.subject-crawl.common :refer [result-af resolve-ident-vars
                                                          filter-subject]]
            [fluree.db.permissions-validate :refer [filter-subject-flakes]]
            [fluree.db.dbproto :as dbproto]))

#?(:clj (set! *warn-on-reflection* true))

(defn- subjects-chan
  "Returns chan of subjects in chunks per index-leaf
  that can be pulled as needed based on the selection criteria of a where clause."
  [{:keys [conn novelty t] :as db} error-ch vars {:keys [p o] :as _where-clause}]
  (let [idx-root    (get db :post)
        novelty     (get novelty :post)
        o*          (if-some [v (:value o)]
                      v
                      (when-let [variable (:variable o)]
                        (get vars variable)))
        p*          (:value p)
        o-dt        (:datatype o)
        first-flake (flake/create nil p* o* o-dt nil nil util/min-integer)
        last-flake  (flake/create nil p* o* o-dt nil nil util/max-integer)
        filter-xf   (when (:filter o)
                      (let [f (filter/extract-combined-filter (:filter o))]
                        (filter (fn [flake]
                                  (-> flake flake/o f)))))
        query-xf    (comp (query-range/extract-query-flakes {:flake-xf filter-xf})
                          cat
                          (map flake/s)
                          (distinct))
        resolver    (index/->CachedTRangeResolver conn novelty t t (:lru-cache-atom conn))]
    (index/tree-chan resolver idx-root first-flake last-flake any? 10 query-xf error-ch)))

(defn permissioned-db?
  [db]
  (not (get-in db [:policy const/iri-view :root?])))

(defn flakes-xf
  [{:keys [db fuel-vol max-fuel error-ch vars filter-map] :as _opts}]
  (fn [sid port]
    (async/go
      (try*
        ;; TODO: Right now we enforce permissions after the index-range call, but
        ;; TODO: in some circumstances we can know user can see no subject flakes
        ;; TODO: and if detected, could avoid index-range call entirely.
        (let [flake-range (cond->> (<? (query-range/index-range db :spot = [sid]))
                            filter-map (filter-subject vars filter-map))
              flakes      (if (permissioned-db? db)
                            (<? (filter-subject-flakes db flake-range))
                            flake-range)]
          (when (seq flakes)
            (async/put! port flakes))

          (async/close! port))
        (catch* e (async/put! error-ch e) (async/close! port) nil)))))


(defn subjects-id-chan
  "For queries that specify _id as the predicate, we will have a
  single subject as a value."
  [db error-ch vars {:keys [o] :as f-where}]
  (log/trace "subjects-id-chan f-where:" f-where)
  (let [return-ch (async/chan)
        _id-val   (or (:value o)
                      (get vars (:variable o)))]
    (when-not _id-val
      (throw (ex-info (str "When using _id as the predicate, a value must be provided: " f-where)
                      {:status 400 :error :db/invalid-query})))
    (async/go
      (if (number? _id-val)
        (async/>! return-ch _id-val)
        (let [sid (<! (dbproto/-subid db _id-val))]
          (cond (util/exception? sid)
                (>! error-ch sid)

                (some? sid)
                (>! return-ch sid))))
      (async/close! return-ch))
    return-ch))


(defn subj-crawl
  [{:keys [db error-ch f-where limit offset parallelism vars ident-vars
           finish-fn] :as opts}]
  (go-try
    (log/trace "subj-crawl opts:" opts)
    (let [vars*     (if ident-vars
                      (<? (resolve-ident-vars db vars ident-vars))
                      vars)
          opts*     (assoc opts :vars vars*)
          sid-ch    (if (#{:_id :iri} (:type f-where))
                      (subjects-id-chan db error-ch vars* f-where)
                      (subjects-chan db error-ch vars* f-where))
          flakes-af (flakes-xf opts*)
          offset-xf (if offset
                      (drop offset)
                      identity)
          flakes-ch  (async/chan 32 offset-xf)
          limit-ch   (if limit
                       (async/take limit flakes-ch)
                       flakes-ch)
          result-ch (async/chan)
          final-ch  (async/into [] result-ch)]

      (async/pipeline-async parallelism flakes-ch flakes-af sid-ch)
      (async/pipeline-async parallelism result-ch (result-af opts*) limit-ch)

      (async/alt!
        error-ch ([e] e)
        final-ch ([results]
                  (finish-fn results))))))
