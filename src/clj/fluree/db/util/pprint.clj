(ns fluree.db.util.pprint
  (:require [clojure.core.async :as async]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]))

(set! *warn-on-reflection* true)


(defn pprint-root
  [conn index count-atom depth pos-idx]
  (let [branch?      (index/branch? index)
        indent-floor 18
        str-vec      (-> (repeat depth "-") vec (conj ">"))
        str-vec      (if branch?
                       (conj str-vec " I")
                       (conj str-vec " D"))
        str-vec      (if (index/leaf? index)
                       (let [node-count (count (:flakes index))]
                         (swap! count-atom + node-count)
                         (conj str-vec (str ":" (:t index) "-" node-count)))
                       str-vec)
        first-flake  (:first index)
        main-str     (apply str str-vec)
        addl-indent  (if (neg? (- indent-floor (count main-str)))
                       " "
                       (apply str (repeat (- indent-floor (count main-str)) " ")))
        str-vec      [main-str addl-indent]
        str-vec      (conj str-vec (str (flake/s first-flake) "-" (flake/p first-flake) " "))
        str-vec      (conj str-vec (pr-str pos-idx))]
    (println (apply str str-vec))
    (when branch?
      (let [children-count (count (:children index))]
        (dotimes [i children-count]
          (let [child (-> index :children (nth i))]
            (pprint-root conn
                         (async/<!! (index/resolve conn child))
                         count-atom
                         (inc depth)
                         (conj pos-idx i))))))))


(defn pprint-index
  [conn index]
  (let [count-atom (atom 0)]
    (pprint-root conn (async/<!! (index/resolve conn index)) count-atom 0 [])
    (println "Total count: " @count-atom)))


(defn pprint-node
  [node & [prefix]]
  (let [index-node?    (index/branch? node)
        node-type      (if index-node? "I" "D")
        children-count (if index-node?
                         (count (:children node))
                         (count (:flakes node)))
        full-str       (str prefix node-type " c-" children-count)]
    (println full-str)))


(defn pprint-path
  "Pretty prints a lookup path."
  [path & [print-data?]]
  (doseq [[p idx] (partition 2 (interleave path (range (count path))))]
    (let [dashes (apply str (repeat idx "-"))]
      (if (number? p)
        (println (str dashes "> " p))
        (do
          (pprint-node p (str dashes "> "))
          (when (and print-data? (index/leaf? p))
            (println (:flakes p))))))))


(defn pprint-db
  [{:keys [conn spot post opst tspo]}]
  (println "spot:")
  (println "-----------")
  (pprint-index conn spot)
  (println "")
  (println "post:")
  (println "-----------")
  (pprint-index conn post)
  (println "")
  (println "opst:")
  (println "-----------")
  (pprint-index conn opst)
  (println "")
  (println "tspo:")
  (println "-----------")
  (pprint-index conn tspo))
