(ns fluree.db.query.json-ld.select
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.parse.aggregate :refer [parse-aggregate safe-read-fn]]
            [fluree.db.util.log :as log :include-macros true]))

;; parses select statement for JSON-LD queries

#?(:clj (set! *warn-on-reflection* true))

(defn q-var->symbol
  "Returns a query variable as a symbol, else nil if not a query variable."
  [x]
  (when (or (and (string? x)
                 (= \? (first x)))
            (and (or (symbol? x) (keyword? x))
                 (= \? (first (name x)))))
    (symbol x)))

(defn aggregate?
  "Aggregate as positioned in a :select statement"
  [x]
  (or (and (string? x)
           (re-matches #"^\(.+\)$" x))
      (and (list? x)
           (symbol? (first x)))))


(defn parse-map
  [select-map depth]
  (let [[var selection] (first select-map)
        var-as-symbol (q-var->symbol var)]
    (when (or (not= 1 (count select-map))
              (nil? var-as-symbol))
      (throw (ex-info (str "Invalid select statement, maps must have only one key/val. Provided: " select-map)
                      {:status 400 :error :db/invalid-query})))
    {:variable  var-as-symbol
     :selection selection
     :depth     depth}))

(defn expand-selection
  [context depth selection]
  (reduce
    (fn [acc select-item]
      (cond
        (map? select-item)
        (let [[k v] (first select-item)
              iri    (json-ld/expand-iri k context)
              spec   {:iri iri}
              depth* (if (zero? depth)
                       0
                       (dec depth))
              reverse? (boolean (get-in context [k :reverse]))
              spec* (-> spec
                        (assoc :spec (expand-selection context depth* v)
                               :as k))]
          (if reverse?
            (assoc-in acc [:reverse iri] spec*)
            (assoc acc iri spec*)))

        (#{"*" :* '*} select-item)
        (assoc acc :wildcard? true)

        (#{"_id" :_id} select-item)
        (assoc acc :_id? true)

        :else
        (let [iri      (json-ld/expand-iri select-item context)
              spec     {:iri iri}
              reverse? (boolean (get-in context [select-item :reverse]))]
          (if reverse?
            (assoc-in acc [:reverse iri] (assoc spec :as select-item))
            (assoc acc iri (assoc spec :as select-item))))))
    {:depth depth} selection))

(defn parse-subselection
  [context select-map depth]
  (let [{:keys [variable selection depth]} (parse-map select-map depth)
        spec                               (expand-selection context depth selection)]
    {:variable  variable
     :selection selection
     :depth     depth
     :spec      spec}))
