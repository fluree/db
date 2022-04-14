(ns fluree.db.query.subject-crawl.rdf-type
  (:require #?(:clj  [clojure.core.async :refer [go <! >!] :as async]
               :cljs [cljs.core.async :refer [go <! >!] :as async])
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.index :as index]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.query.subject-crawl.common :refer [where-subj-xf result-af subj-perm-filter-fn filter-subject order-results]]))

#?(:clj (set! *warn-on-reflection* true))


(defn flakes-xf
  [{:keys [db fuel-vol max-fuel error-ch vars filter-map permissioned?] :as _opts}]
  (let [permissions (when permissioned?
                      (subj-perm-filter-fn db))]
    (fn [flakes port]
      (go
        (try*
          ;; TODO: Right now we enforce permissions after the index-range call, but
          ;; TODO: in some circumstances we can know user can see no subject flakes
          ;; TODO: and if detected, could avoid index-range call entirely.
          (let [flakes* (cond->> flakes
                                 filter-map (filter-subject vars filter-map)
                                 permissioned? permissions
                                 permissioned? <?)]
            (when (seq flakes*)
              (async/put! port flakes*))

            (async/close! port))
          (catch* e (async/put! error-ch e) (async/close! port) nil))))))

(defn subj-flakes-chan
  "Returns a channel that has a stream of flakes grouped by subject id.
  Always uses :spot index."
  [{:keys [conn novelty t spot] :as db} error-ch vars {:keys [o] :as _where-clause}]
  (let [rdf-type    (or (:value o)
                        (get vars (:variable o)))
        cid         (or (dbproto/-c-prop db :id rdf-type)
                        (throw (ex-info (str "Invalid data type: " rdf-type)
                                        {:status 400 :error :db/invalid-query})))
        fflake      (flake/->Flake (flake/max-subject-id cid) -1 nil nil nil util/min-integer)
        lflake      (flake/->Flake (flake/min-subject-id cid) util/max-integer nil nil nil util/max-integer)
        cmp         (:comparator spot)
        range-set   (flake/sorted-set-by cmp fflake lflake)
        in-range?   (fn [node]
                      (query-range/intersects-range? node range-set))
        query-xf    (where-subj-xf {:start-test  >=
                                    :start-flake fflake
                                    :end-test    <=
                                    :end-flake   lflake
                                    :return-type :flake-by-sid})
        resolver    (index/->CachedTRangeResolver conn (:spot novelty) t t (:async-cache conn))
        tree-chan   (index/tree-chan resolver spot in-range? query-range/resolved-leaf? 1 query-xf error-ch)
        return-chan (async/chan 10 (partition-by flake/s))]
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

(defn rdf-type-crawl
  [{:keys [db error-ch f-where limit offset parallelism finish-fn vars] :as opts}]
  (go-try
    (let [subj-ch   (subj-flakes-chan db error-ch vars f-where)
          flakes-af (flakes-xf opts)
          flakes-ch (async/chan 32 (comp (drop offset) (take limit)))
          result-ch (async/chan)]

      (async/pipeline-async parallelism flakes-ch flakes-af subj-ch)
      (async/pipeline-async parallelism result-ch (result-af opts) flakes-ch)

      (loop [acc []]
        (let [[next-res ch] (async/alts! [error-ch result-ch])]
          (cond
            (= ch error-ch)
            (do (async/close! subj-ch)
                (async/close! flakes-ch)
                (async/close! result-ch)
                (throw next-res))

            (nil? next-res)
            (do (async/close! subj-ch)
                (async/close! flakes-ch)
                (async/close! result-ch)
                (finish-fn acc))


            :else
            (recur (conj acc next-res))))))))