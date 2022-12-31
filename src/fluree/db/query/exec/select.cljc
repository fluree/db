(ns fluree.db.query.exec.select
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(defmulti display
  (fn [match db select-cache compact]
    (::where/datatype match)))

(defmethod display :default
  [match _ _ _]
  (go (::where/val match)))

(defmethod display const/$xsd:anyURI
  [match db select-cache compact]
  (go-try
   (let [v (::where/val match)]
     (if-let [cached (get @select-cache v)]
       cached
       (let [iri (<? (dbproto/-iri db (::where/val match) compact))]
         (vswap! select-cache assoc v iri)
         iri)))))

(defmulti format
  (fn [selector db select-cache compact solution]
    (if (map? selector)
      (::selector selector)
      :var)))

(defmethod format :var
  [variable db select-cache compact solution]
  (-> solution
      (get variable)
      (display db select-cache compact)))

(defn ->aggregate-selector
  [variable function]
  {::selector :aggregate
   ::variable variable
   ::function function})

(defmethod format :aggregate
  [{::keys [variable function]} db select-cache compact solution]
  (go-try
   (let [group (<? (format variable db select-cache compact solution))]
     (function group))))

(defn ->subgraph-selector
  [variable selection spec depth]
  {::selector  :subgraph
   ::variable  variable
   ::selection selection
   ::depth     depth
   ::spec      spec})

(defmethod format :subgraph
  [{::keys [variable selection depth spec]} db select-cache compact solution]
  (go-try
   (let [sid    (-> solution
                    (get variable)
                    ::where/val)
         flakes (<? (query-range/index-range db :spot = [sid]))]
     ;; TODO: Replace these nils with fuel values when we turn fuel back on
     (<? (json-ld-resp/flakes->res db select-cache compact nil nil spec 0 flakes)))))

(defn select-values
  [db select-cache compact solution selectors]
  (go-try
   (if (sequential? selectors)
     (loop [selectors selectors
            values     []]
       (if-let [selector (first selectors)]
         (let [value (<? (format selector db select-cache compact solution))]
           (recur (rest selectors)
                  (conj values value)))
         values))
     (let [selector selectors]
       (<? (format selector db select-cache compact solution))))))

(defn select
  [db q solution-ch]
  (let [compact      (->> q :context json-ld/compact-fn)
        selectors    (or (:select q)
                         (:selectOne q))
        select-cache (volatile! {})
        select-ch    (async/chan)]
    (async/pipeline-async 1
                          select-ch
                          (fn [solution ch]
                            (-> (select-values db select-cache compact solution selectors)
                                (async/pipe ch)))
                          solution-ch)
    select-ch))
