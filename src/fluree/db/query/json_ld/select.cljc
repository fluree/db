(ns fluree.db.query.json-ld.select
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.parse.aggregate :refer [parse-aggregate safe-read-fn]]))

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
  (and (string? x)
       (re-matches #"^\(.+\)$" x)))


(defn parse-map
  [select-map]
  (let [[var selection] (first select-map)
        var-as-symbol (q-var->symbol var)]
    (when (or (not= 1 (count select-map))
              (nil? var-as-symbol))
      (throw (ex-info (str "Invalid select statement, maps must have only one key/val. Provided: " select-map)
                      {:status 400 :error :db/invalid-query})))
    {:variable  var-as-symbol
     :selection selection}))

(defn parse-select
  [select-smt]
  (let [_ (or (every? #(or (string? %) (map? %)) select-smt)
              (throw (ex-info (str "Invalid select statement. Every selection must be a string or map. Provided: " select-smt)
                              {:status 400 :error :db/invalid-query})))]
    (map (fn [select]
           (let [var-symbol (q-var->symbol select)]
             (cond
               var-symbol
               {:variable var-symbol}

               (aggregate? select)
               (parse-aggregate select)

               (map? select)
               (parse-map select)

               :else
               (throw (ex-info (str "Invalid select in statement, provided: " select)
                               {:status 400 :error :db/invalid-query})))))
         select-smt)))


(defn expand-selection
  [{:keys [schema] :as db} context selection]
  (reduce
    (fn [acc select-item]
      (cond
        (map? select-item)
        (let [[k v] (first select-item)
              iri  (json-ld/expand-iri k context)
              spec (get-in schema [:pred iri])
              pid  (:id spec)]
          (assoc acc pid (-> spec
                             (assoc :spec (expand-selection db context v)
                                    :as k))))

        (#{"*" :* '*} select-item)
        (assoc acc :wildcard? true)

        :else
        (let [iri  (json-ld/expand-iri select-item context)
              spec (get-in schema [:pred iri])
              pid  (:id spec)]
          (assoc acc pid (assoc spec :as select-item)))))
    {} selection))


(defn expand-spec
  [db context parsed-select]
  (reduce
    (fn [acc select-item]
      (if-let [selection (:selection select-item)]
        (conj acc (assoc select-item :spec (expand-selection db context selection))
              (conj acc select-item))))
    []
    parsed-select))


(defn parse
  [db
   {:keys [limit pretty-print context] :as parsed-query}
   {:keys [selectOne select selectDistinct selectReduced] :as _query-map'}]
  (let [select-smt    (or selectOne select selectDistinct selectReduced)
        selectOne?    (boolean selectOne)
        limit*        (if selectOne? 1 limit)
        inVector?     (sequential? select-smt)
        select-smt    (if inVector? select-smt [select-smt])
        parsed-select (parse-select select-smt)
        aggregates    (filter #(contains? % :function) parsed-select)
        expandMap?    (some #(contains? % :selection) parsed-select)
        spec          (if expandMap?
                        (expand-spec db context parsed-select)
                        parsed-select)]
    (assoc parsed-query :limit limit*
                        :selectOne? selectOne?
                        :select {:spec            spec
                                 :aggregates      (not-empty aggregates)
                                 :expandMaps?     expandMap?
                                 :selectOne?      selectOne?
                                 :selectDistinct? (boolean (or selectDistinct selectReduced))
                                 :inVector?       inVector?
                                 :prettyPrint     pretty-print})))