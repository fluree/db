(ns fluree.db.query.block
  (:require [clojure.set :as set]
            [fluree.db.query.range :refer [index-range]]
            [fluree.db.query.fql :as fql]
            #?(:clj [fluree.db.util.async :refer [<? go-try]])
            #?(:clj [fluree.db.permissions-validate :as perm-validate])
            #?(:clj  [clojure.core.async :refer [>! <! >!! <!! go chan buffer close! thread
                                                 alts! alts!! timeout] :as async]
               :cljs [cljs.core.async :refer [go chan <!] :as async])
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [<?]]))

(defn subj->block-map
  [subj-map]
  (-> subj-map
      (set/rename-keys {:_id :t
                        "_block/hash" :hash
                        "_block/instant" :instant
                        "_block/sigs" :sigs
                        "_block/transactions" :txns
                        "_block/number" :block})
      (update :txns (partial map #(get % "_id")))))

(defn block-query
  [db block]
  (let [out (chan 1 (comp cat
                          (map subj->block-map)))]
    (-> db
        (fql/query {:select {"?t" ["_block/number"
                                   "_block/hash"
                                   "_block/instant"
                                   "_block/sigs"
                                   "_block/transactions"]}
                    :where [["?t" "_block/number" block]]})
        (async/pipe out))))

(defn block-flakes
  [db min-t max-t]
  (index-range db :tspo >= [min-t] <= [max-t]))

(defn query-block
  [db block]
  (go
    (let [res (<! (block-query db block))
          [min-t max-t] (apply (juxt max min) (:txns res)) ; reverse max and min because t decrements)
          flakes (<! (block-flakes db min-t max-t))]
      (assoc res :flakes flakes))))

(defn block-range
  [db start end _opts]
  (let [reverse?   (when end (< end start))
        last-block (or end start)]
    (go
      (loop [current-block start
             blocks []]
        (if (> current-block last-block)
          blocks
          (let [res (<? (query-block db current-block))
                blocks* (conj blocks res)
                next-block (if reverse?
                             (dec current-block)
                             (inc current-block))]
            (recur next-block blocks*)))))))
