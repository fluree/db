(ns fluree.db.query.subject-crawl.subject
  (:require [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake.format :as jld-format]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.query.range :as query-range]
            [fluree.db.index :as index]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.subject-crawl.common :refer [filter-subject]]
            [fluree.db.json-ld.policy.query :as policy :refer [filter-subject-flakes]]
            [fluree.db.json-ld.iri :as iri]))

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
        resolver    (index/conn->t-range-resolver conn novelty t t)]
    (index/tree-chan resolver idx-root first-flake last-flake any? 10 query-xf error-ch)))

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
              flakes      (if (policy/unrestricted? db)
                            flake-range
                            (<? (filter-subject-flakes db flake-range)))]
          (when (seq flakes)
            (async/put! port flakes))

          (async/close! port))
        (catch* e (async/put! error-ch e) (async/close! port) nil)))))


(defn subjects-id-chan
  "For queries that specify an explicit iri as node id, we will have a single
  subject as a value."
  [db error-ch vars {:keys [o] :as f-where}]
  (log/trace "subjects-id-chan f-where:" f-where)
  (let [return-ch (async/chan)
        iri       (or (:value o)
                      (get vars (:variable o)))]
    (when-not iri
      (throw (ex-info (str "No IRI value provided: " f-where)
                      {:status 400 :error :db/invalid-query})))
    (async/go
      (let [sid (iri/encode-iri db iri)]
        (cond (util/exception? sid)
              (>! error-ch sid)

              (some? sid)
              (>! return-ch sid)))
      (async/close! return-ch))
    return-ch))

(defn result-af
  [{:keys [db cache context compact-fn select-spec error-ch] :as _opts}]
  (fn [flakes port]
    (-> db
        (jld-format/format-subject-flakes cache context compact-fn select-spec
                                      0 nil error-ch flakes)
        (async/pipe port))))

(defn subj-crawl
  [{:keys [db error-ch f-where limit offset parallelism vars finish-fn] :as opts}]
  (go-try
    (log/trace "subj-crawl opts:" opts)
    (let [opts*     (assoc opts :vars vars)
          sid-ch    (if (#{:iri} (:type f-where))
                      (subjects-id-chan db error-ch vars f-where)
                      (subjects-chan db error-ch vars f-where))
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
