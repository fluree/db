(ns fluree.db.query.subject-crawl.subject
  (:require [clojure.core.async :refer [<! >!] :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.query.range :as query-range]
            [fluree.db.index :as index]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.query.exec :refer [drop-offset take-limit]]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.subject-crawl.common :refer [where-subj-xf result-af
                                                          resolve-ident-vars filter-subject]]
            [fluree.db.permissions-validate :refer [filter-subject-flakes]]
            [fluree.db.dbproto :as dbproto]))

#?(:clj (set! *warn-on-reflection* true))

(defn- subjects-chan
  "Returns chan of subjects in chunks per index-leaf
  that can be pulled as needed based on the selection criteria of a where clause."
  [{:keys [conn novelty t] :as db} error-ch vars {:keys [p o p-ref?] :as _where-clause}]
  (let [o*          (if-some [v (:value o)]
                      v
                      (when-let [variable (:variable o)]
                        (get vars variable)))
        p*          (:value p)
        idx*        (where/idx-for nil p* o*)
        o-dt        (:datatype o)
        [fflake lflake] (case idx*
                          :post [(flake/create nil p* o* o-dt nil nil util/min-integer)
                                 (flake/create nil p* o* o-dt nil nil util/max-integer)]
                          :psot [(flake/create nil p* nil nil nil nil util/min-integer)
                                 (flake/create nil p* nil nil nil nil util/max-integer)])
        filter-fn   (cond
                      (and o* (= :psot idx*))
                      #(= o* (flake/o %))

                      (:filter o)
                      (let [f (filter/extract-combined-filter (:filter o))]
                        #(-> % flake/o f)))
        return-chan (async/chan 10 (comp (map flake/s)
                                         (dedupe)))]
    (let [idx-root  (get db idx*)
          cmp       (:comparator idx-root)
          range-set (flake/sorted-set-by cmp fflake lflake)
          in-range? (fn [node]
                      (query-range/intersects-range? node range-set))
          query-xf  (where-subj-xf {:start-test  >=
                                    :start-flake fflake
                                    :end-test    <=
                                    :end-flake   lflake
                                    ;; if looking for pred + obj, but pred is not indexed, then need to use :psot and filter for 'o' values
                                    :xf          (when filter-fn
                                                   (map (fn [flakes]
                                                          (filter filter-fn flakes))))})
          resolver  (index/->CachedTRangeResolver conn (get novelty idx*) t t (:lru-cache-atom conn))
          tree-chan (index/tree-chan resolver idx-root in-range? query-range/resolved-leaf? 1 query-xf error-ch)]
      (async/go-loop []
        (let [next-chunk (<! tree-chan)]
          (if (nil? next-chunk)
            (async/close! return-chan)
            (let [more? (loop [vs (seq next-chunk)
                               i  0]
                          (if vs
                            (if (>! return-chan (first vs))
                              (recur (next vs) (inc i))
                              false)
                            true))]
              (if more?
                (recur)
                (async/close! return-chan)))))))
    return-chan))


(defn flakes-xf
  [{:keys [db fuel-vol max-fuel error-ch vars filter-map permissioned?] :as _opts}]
  (fn [sid port]
    (async/go
      (try*
        ;; TODO: Right now we enforce permissions after the index-range call, but
        ;; TODO: in some circumstances we can know user can see no subject flakes
        ;; TODO: and if detected, could avoid index-range call entirely.
        (let [flakes (cond->> (<? (query-range/index-range db :spot = [sid]))
                              filter-map (filter-subject vars filter-map)
                              permissioned? (filter-subject-flakes db)
                              permissioned? <?)]
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
        (let [sid (async/<! (dbproto/-subid db _id-val))]
          (cond (util/exception? sid)
                (async/put! error-ch sid)

                (some? sid)
                (async/put! return-ch sid))))
      (async/close! return-ch))
    return-ch))

(defn resolve-refs
  "If the where clause has a ref, we need to resolve it to a subject id."
  [db vars {:keys [p-ref? o] :as where-clause}]
  (go-try
    (if p-ref?
      (let [v   (if-some [v (:value o)]
                  v
                  (when-let [variable (:variable o)]
                    (get vars variable)))
            sid (when v
                  (or (<? (dbproto/-subid db v)) 0))]
        (if sid
          (assoc where-clause :o {:value sid})
          (assoc where-clause :o {:value nil})))
      where-clause)))

(defn resolve-o-ident
  "If the predicate is a ref? type with an 'o' value, it must be resolved into a subject id."
  [db {:keys [o] :as where-clause}]
  (go-try
    (let [_id (or (<? (dbproto/-subid db (:ident o))) 0)]
      (assoc where-clause :o {:value _id}))))


(defn subj-crawl
  [{:keys [db error-ch f-where limit offset parallelism vars ident-vars
           finish-fn] :as opts}]
  (go-try
    (log/trace "subj-crawl opts:" opts)
    (let [{:keys [o p-ref?]} f-where
          vars*     (if ident-vars
                      (<? (resolve-ident-vars db vars ident-vars))
                      vars)
          opts*     (assoc opts :vars vars*)
          f-where*  (if (and p-ref? (:ident o))
                      (<? (resolve-o-ident db f-where))
                      f-where)
          sid-ch    (if (#{:_id :iri} (:type f-where*))
                      (subjects-id-chan db error-ch vars* f-where*)
                      (subjects-chan db error-ch vars* f-where*))
          flakes-af (flakes-xf opts*)
          flakes-ch (->> (async/chan 32)
                         (drop-offset f-where)
                         (take-limit f-where))
          result-ch (async/chan)]

      (async/pipeline-async parallelism flakes-ch flakes-af sid-ch)
      (async/pipeline-async parallelism result-ch (result-af opts*) flakes-ch)

      (loop [acc []]
        (let [[next-res ch] (async/alts! [error-ch result-ch])]
          (cond
            (= ch error-ch)
            (do (async/close! sid-ch)
                (async/close! flakes-ch)
                (async/close! result-ch)
                (throw next-res))

            (nil? next-res)
            (do (async/close! sid-ch)
                (async/close! flakes-ch)
                (async/close! result-ch)
                (finish-fn acc))

            :else
            (recur (conj acc next-res))))))))
