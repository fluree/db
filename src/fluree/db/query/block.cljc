(ns fluree.db.query.block
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.query.range :refer [index-range]]
            [fluree.db.util.async :refer [<? go-try]]))

(defn lookup-block-t
  [db block-num]
  (go-try
   (let [flake-set (<? (index-range db :post = [const/$_block:number block-num]
                                    {:limit 1, :flake-limit 1}))]
     (->> flake-set
          first
          flake/s))))

(def block-metadata-mapping
  {const/$_block:hash         :hash
   const/$_block:transactions :txns
   const/$_block:instant      :instant
   const/$_block:number       :block
   const/$_block:sigs         :sigs})

(defn reduce-block-metadata
  [m f]
  (let [p (flake/p f)
        o (flake/o f)]
    (if-let [metadata-key (get block-metadata-mapping p)]
      (if (= metadata-key :txns)
        (update m metadata-key conj o)
        (assoc m metadata-key o))
      m)))

(defn lookup-block-metadata
  [db block-t]
  (go-try
   (let [spot-flakes (<? (index-range db :spot = [block-t]))]
     (reduce reduce-block-metadata {:t block-t} spot-flakes))))

(defn block-flakes
  [db min-t max-t]
  (index-range db :tspo >= [min-t] <= [max-t]))

(defn lookup-block
  [db block]
  (go-try
    (let [block-t  (<? (lookup-block-t db block))
          metadata (<? (lookup-block-metadata db block-t))

          ;; reverse max and min here because t decrements)
          [min-t max-t] (apply (juxt max min) (:txns metadata))
          flakes        (<? (block-flakes db min-t max-t))]
      (assoc metadata :flakes flakes))))

(defn block-range
  [db start end _opts]
  (let [reverse?   (when end (< end start))
        last-block (or end start)]
    (go-try
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
