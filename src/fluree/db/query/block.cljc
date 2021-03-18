(ns fluree.db.query.block
  (:require [clojure.set :as set]
            [fluree.db.constants :as const]
            [fluree.db.query.range :refer [index-range index-flake-stream]]
            [fluree.db.query.fql :as fql]
            #?(:clj [fluree.db.util.async :refer [<? go-try]])
            #?(:clj [fluree.db.permissions-validate :as perm-validate])
            #?(:clj  [clojure.core.async :refer [>! <! >!! <!! go chan buffer close! thread
                                                 alts! alts!! timeout] :as async]
               :cljs [cljs.core.async :refer [go chan <!] :as async])
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [<?]])
  (:import fluree.db.flake.Flake))

(defn block-pred->meta-key
  [pred]
  (get {const/$_block:hash         :hash
        const/$_block:transactions :txns
        const/$_block:instant      :instant
        const/$_block:number       :block
        const/$_block:sigs         :sigs}
       pred))

(defn lookup-block-t
  [db block-num]
  (let [out (chan 1 (map (fn [^Flake f]
                           (.-s f))))]
    (-> db
        (index-flake-stream :post = [const/$_block:number block-num]
                            {:limit 1, :flake-limit 1})
        (async/pipe out))))

(defn include-block-meta
  [m ^Flake f]
  (let [p (.-p f)
        o (.-o f)]
    (if-let [meta-key (block-pred->meta-key p)]
      (if (= meta-key :txns)
        (update m meta-key conj o)
        (assoc m meta-key o))
      m)))

(defn lookup-block-meta
  [db block-num]
  (let [out (chan)]
    (go
      (let [block-t (<! (lookup-block-t db block-num))]
        (-> db
            (index-flake-stream :spot = [block-t])
            (->> (async/reduce include-block-meta {:t block-t}))
            (async/pipe out))))
    out))

(defn block-flakes
  [db min-t max-t]
  (index-range db :tspo >= [min-t] <= [max-t]))

(defn query-block
  [db block]
  (go
    (let [meta (<! (lookup-block-meta db block))
          [min-t max-t] (apply (juxt max min) (:txns meta)) ; reverse max and min because t decrements)
          flakes (<! (block-flakes db min-t max-t))]
      (assoc meta :flakes flakes))))

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
