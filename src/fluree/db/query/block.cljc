(ns fluree.db.query.block
  (:require [fluree.db.constants :as const]
            [fluree.db.query.range :refer [index-range index-flake-stream]]
            #?(:clj  [clojure.core.async :refer [>! <! go chan] :as async]
               :cljs [cljs.core.async :refer [>! <! go chan] :as async])
            [fluree.db.util.async :refer [<?]])
  (:import fluree.db.flake.Flake))

(defn lookup-block-t
  [db block-num]
  (let [out (chan 1 (map #(.-s ^Flake %)))]
    (-> db
        (index-flake-stream :post = [const/$_block:number block-num]
                            {:limit 1, :flake-limit 1})
        (async/pipe out))))

(defn block-pred->meta-key
  [pred]
  (get {const/$_block:hash         :hash
        const/$_block:transactions :txns
        const/$_block:instant      :instant
        const/$_block:number       :block
        const/$_block:sigs         :sigs}
       pred))

(defn reduce-meta-flake
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
            (->> (async/reduce reduce-meta-flake {:t block-t}))
            (async/pipe out))))
    out))

(defn block-flakes
  [db min-t max-t]
  (index-range db :tspo >= [min-t] <= [max-t]))

(defn lookup-block
  [db block]
  (go
    (let [meta          (<! (lookup-block-meta db block))
          ;; reverse max and min here because t decrements)
          [min-t max-t] (apply (juxt max min) (:txns meta))
          flakes        (<! (block-flakes db min-t max-t))]
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
          (let [block   (<? (lookup-block db current-block))
                blocks* (conj blocks block)
                next-block (if reverse?
                             (dec current-block)
                             (inc current-block))]
            (recur next-block blocks*)))))))
