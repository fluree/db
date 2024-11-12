(ns fluree.db.virtual-graph.bm25.index
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.virtual-graph.bm25.stemmer :as stm]
            [fluree.db.virtual-graph.bm25.stopwords :as stopwords]
            [fluree.db.util.log :as log])
  (:refer-clojure :exclude [assert]))

;; TODO - VG - add 'lang' property to pull that out - right now everything is english
(defn idx-flakes->opts-map
  [index-flakes]
  (reduce
   (fn [acc idx-flake]
     (cond
       (= (flake/p idx-flake) const/$fluree:index-b)
       (let [b (flake/o idx-flake)]
         (if (and (number? b) (<= 0 b) (<= b 1))
           (assoc acc :b b)
           (throw (ex-info (str "Invalid B value provided for Bm25 index, must be a number between 0 and 1, but found: " b)
                           {:status 400
                            :error  :db/invalid-index}))))

       (= (flake/p idx-flake) const/$fluree:index-k1)
       (let [k1 (flake/o idx-flake)]
         (if (and (number? k1) (<= 0 k1))
           (assoc acc :k1 k1)
           (throw (ex-info (str "Invalid K1 value provided for Bm25 index, must be a number greater than 0, but found: " k1)
                           {:status 400
                            :error  :db/invalid-index}))))

       :else acc))
   ;; TODO - once protocol is established, can remove :vg-type key
   {:b    0.75
    :k1   1.2
    :lang "en"}
   index-flakes))

(def initialized-index
  ;; there is always a 'ready to use' current index
  {:index          {:vectors    {}
                    :dimensions 0
                    :item-count 0
                    :avg-length 0
                    :terms      {}}
   ;; pending-ch will contain a promise-chan of index state as of immutable db-t value
   ;; once complete, will replace ':current' index with finished pending one
   :pending-ch     nil
   ;; pending-status will contain two-tuple of [items-complete total-items] which can be divided for % complete
   :pending-status nil})

(defn add-stemmer
  [{:keys [lang] :as opts}]
  (assoc opts :stemmer (stm/initialize lang)))

(defn add-stopwords
  [{:keys [lang] :as opts}]
  (assoc opts :stopwords (stopwords/initialize lang)))

(defn idx-flakes->opts
  [index-flakes]
  (-> index-flakes
      (idx-flakes->opts-map)
      (add-stemmer)
      (add-stopwords)))