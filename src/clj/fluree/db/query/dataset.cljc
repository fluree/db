(ns fluree.db.query.dataset
  (:require [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.exec.select.subject :as subject]
            [clojure.core.async :as async]))

(defrecord DataSet [named default active])

(defn dataset?
  [ds]
  (instance? DataSet ds))

(defn activate
  [ds alias]
  (when (-> ds :named (contains? alias))
    (assoc ds :active alias)))

(defn get-active-graph
  [ds]
  (let [active-graph (:active ds)]
    (if (#{::default} active-graph)
      (:default ds)
      (-> ds :named (get active-graph)))))

(defn names
  [ds]
  (-> ds :named keys))

(defn all
  [ds]
  (->> (:default ds)
       (concat (-> ds :named vals))
       (into [] (distinct))))

(defn merge-objects
  [obj1 obj2]
  (if (sequential? obj1)
    (if (sequential? obj2)
      (into obj1 obj2)
      (conj obj1 obj2))
    (if (sequential? obj2)
      (into [obj1] obj2)
      [obj1 obj2])))

(defn merge-subgraphs
  [sg1 sg2]
  (merge-with merge-objects sg1 sg2))

(extend-type DataSet
  where/Matcher
  (-match-id [ds fuel-tracker solution s-mch error-ch]
    (if-let [active-graph (get-active-graph ds)]
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [db]
                    (where/-match-id db fuel-tracker solution s-mch error-ch)))
             async/merge)
        (where/-match-id active-graph fuel-tracker solution s-mch error-ch))
      where/nil-channel))

  (-match-triple [ds fuel-tracker solution triple error-ch]
    (if-let [active-graph (get-active-graph ds)]
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [db]
                    (where/-match-triple db fuel-tracker solution triple error-ch)))
             async/merge)
        (where/-match-triple active-graph fuel-tracker solution triple error-ch))
      where/nil-channel))

  (-match-class [ds fuel-tracker solution triple error-ch]
    (if-let [active-graph (get-active-graph ds)]
      (if (sequential? active-graph)
        (->> active-graph
             (map (fn [db]
                    (where/-match-class db fuel-tracker solution triple error-ch)))
             async/merge)
        (where/-match-class active-graph fuel-tracker solution triple error-ch))
      where/nil-channel))

  (-activate-alias [ds alias]
    (activate ds alias))

  (-aliases [ds]
    (names ds))


  subject/SubjectFormatter
  (-forward-properties [ds iri select-spec context compact-fn cache fuel-tracker error-ch]
    (let [db-ch   (->> ds all async/to-chan!)
          prop-ch (async/chan)]
      (async/pipeline-async 4
                            prop-ch
                            (fn [db ch]
                              (-> (subject/-forward-properties db iri select-spec context compact-fn cache fuel-tracker error-ch)
                                  (async/pipe ch)))
                            db-ch)
      (async/reduce merge-subgraphs {} prop-ch)))

  (-reverse-property [ds iri reverse-spec compact-fn cache fuel-tracker error-ch]
    (let [db-ch   (->> ds all async/to-chan!)
          prop-ch (async/chan)]
      (async/pipeline-async 2
                            prop-ch
                            (fn [db ch]
                              (-> (subject/-reverse-property db iri reverse-spec compact-fn cache fuel-tracker error-ch)
                                  (async/pipe ch)))
                            db-ch)
      (async/reduce (fn [combined-prop db-prop]
                      (let [[as results] combined-prop]
                        (if results
                          (let [[_as next-result] db-prop]
                            [as (merge-objects results next-result)])
                          db-prop)))
                    []
                    prop-ch)))

  (-iri-visible? [ds iri]
    (go-try
      (some? (loop [[db & r] (all ds)]
               (if db
                 (if (<? (subject/-iri-visible? db iri))
                   db
                   (recur r))
                 nil))))))


(defn combine
  [named-map defaults]
  (let [default-graph (some->> defaults util/sequential)]
    (->DataSet named-map default-graph ::default)))

(defn dataset
  [named-graphs default-aliases]
  (let [default-coll (some->> default-aliases
                              util/sequential
                              (select-keys named-graphs)
                              vals)]
    (combine named-graphs default-coll)))
