(ns fluree.db.util.pprint
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.index :as index]
            [clojure.core.async :as async])
  (:import (fluree.db.flake Flake)))


(defn pprint-root
  [index count-atom depth pos-idx]
  (let [indent-first-flake 18
        str-vec            (-> (repeat depth "-") vec (conj ">"))
        str-vec            (if (index/index-node? index)
                             (conj str-vec " I")
                             (conj str-vec " D"))
        str-vec            (if (and (index/index-node? index) (not-empty (:buffer index)))
                             (conj str-vec (str "*" (count (:buffer index))))
                             str-vec)
        str-vec            (if (index/data-node? index)
                             (let [node-count (count (:flakes index))]
                               (swap! count-atom + node-count)
                               (conj str-vec (str ":" (.-block index) "-" node-count)))
                             str-vec)
        first-flake        ^Flake (dbproto/-first-flake index)
        main-str           (apply str str-vec)
        addl-indent        (if (neg? (- indent-first-flake (count main-str)))
                             " "
                             (apply str (repeat (- indent-first-flake (count main-str)) " ")))
        str-vec            [main-str addl-indent]
        str-vec            (conj str-vec (str (.-s first-flake) "-" (.-p first-flake) " "))
        str-vec            (conj str-vec (pr-str pos-idx))]
    (println (apply str str-vec))
    (when (index/index-node? index)
      (let [children-count (count (:children index))]
        (dotimes [i children-count]
          (pprint-root (async/<!! (dbproto/-resolve (nth (:children index) i)))
                       count-atom
                       (inc depth)
                       (conj pos-idx i)))))))


(defn pprint-index
  [index]
  (let [count-atom (atom 0)]
    (pprint-root (async/<!! (dbproto/-resolve index)) count-atom 0 [])
    (println "Total count: " @count-atom)))


(defn pprint-node
  [node & [prefix]]
  (let [index-node?    (index/index-node? node)
        node-type      (if index-node? "I" "D")
        children-count (if index-node?
                         (count (:children node))
                         (count (:flakes node)))
        history-count  (if index-node?
                         (count (:buffer node))
                         (count (:history node)))
        full-str       (str prefix node-type " c-" children-count " h-" history-count)]
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
          (when (and print-data? (index/data-node? p))
            (println (:flakes p))))))))


(defn pprint-db
  [db]
  (println "spot:")
  (println "-----------")
  (pprint-index (:spot db))
  (println "")
  (println "psot:")
  (println "-----------")
  (pprint-index (:psot db))
  (println "")
  (println "post:")
  (println "-----------")
  (pprint-index (:post db))
  (println "")
  (println "opst:")
  (println "-----------")
  (pprint-index (:opst db))
  (println "")
  (println "tspo:")
  (println "-----------")
  (pprint-index (:tspo db)))
