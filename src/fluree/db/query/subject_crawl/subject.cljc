(ns fluree.db.query.subject-crawl.subject
  (:require #?(:clj  [clojure.core.async :refer [go <! >!] :as async]
               :cljs [cljs.core.async :refer [go <! >!] :as async])
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.index :as index]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.query.subject-crawl.common :refer [where-subj-xf result-af subj-perm-filter-fn filter-subject]]
            [fluree.db.dbproto :as dbproto]))

#?(:clj (set! *warn-on-reflection* true))

(defn- subjects-chan
  "Returns chan of subjects in chunks per index-leaf
  that can be pulled as needed based on the selection criteria of a where clause."
  [{:keys [conn novelty t] :as db} error-ch vars {:keys [p o idx] :as _where-clause}]
  (let [o*          (if-some [v (:value o)]
                      v
                      (when-let [variable (:variable o)]
                        (get vars variable)))
        [fflake lflake] (case idx
                          :post [(flake/->Flake nil p o* nil nil util/min-integer)
                                 (flake/->Flake nil p o* nil nil util/max-integer)]
                          :psot [(flake/->Flake nil p nil nil nil util/min-integer)
                                 (flake/->Flake nil p nil nil nil util/max-integer)])
        filter-fn   (cond
                      (and o* (= :psot idx))
                      #(= o* (flake/o %))

                      (:filter o)
                      (let [f (get-in o [:filter :function])]
                        #(-> % flake/o f)))
        idx-root    (get db idx)
        cmp         (:comparator idx-root)
        range-set   (flake/sorted-set-by cmp fflake lflake)
        in-range?   (fn [node]
                      (query-range/intersects-range? node range-set))
        query-xf    (where-subj-xf {:start-test  >=
                                    :start-flake fflake
                                    :end-test    <=
                                    :end-flake   lflake
                                    ;; if looking for pred + obj, but pred is not indexed, then need to use :psot and filter for 'o' values
                                    :xf          (when filter-fn
                                                   (map (fn [flakes]
                                                          (filter filter-fn flakes))))})
        resolver    (index/->CachedTRangeResolver conn (get novelty idx) t t (:async-cache conn))
        tree-chan   (index/tree-chan resolver idx-root in-range? query-range/resolved-leaf? 1 query-xf error-ch)
        return-chan (async/chan 10 (comp (map flake/s)
                                         (dedupe)))]
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
              (async/close! return-chan))))))
    return-chan))


(defn flakes-xf
  [{:keys [db fuel-vol max-fuel error-ch vars filter-map permissioned?] :as _opts}]
  (let [permissions (when permissioned?
                      (subj-perm-filter-fn db))]
    (fn [sid port]
      (async/go
        (try*
          ;; TODO: Right now we enforce permissions after the index-range call, but
          ;; TODO: in some circumstances we can know user can see no subject flakes
          ;; TODO: and if detected, could avoid index-range call entirely.
          (let [flakes (cond->> (<? (query-range/index-range db :spot = [sid]))
                                filter-map (filter-subject vars filter-map)
                                permissioned? permissions
                                permissioned? <?)]
            (when (seq flakes)
              (async/put! port flakes))

            (async/close! port))
          (catch* e (async/put! error-ch e) (async/close! port) nil))))))


(defn subjects-id-chan
  "For queries that specify _id as the predicate, we will have a
  single subject as a value."
  [db error-ch vars {:keys [o] :as f-where}]
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
          (if (util/exception? sid)
            (async/put! error-ch sid)
            (async/put! return-ch sid))))
      (async/close! return-ch))
    return-ch))


(defn subj-crawl
  [{:keys [db error-ch f-where limit offset parallelism vars finish-fn] :as opts}]
  (go-try
    (let [sid-ch    (if (= :_id (:type f-where))
                      (subjects-id-chan db error-ch vars f-where)
                      (subjects-chan db error-ch vars f-where))
          flakes-af (flakes-xf opts)
          flakes-ch (async/chan 32 (comp (drop offset) (take limit)))
          result-ch (async/chan)]

      (async/pipeline-async parallelism flakes-ch flakes-af sid-ch)
      (async/pipeline-async parallelism result-ch (result-af opts) flakes-ch)

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
