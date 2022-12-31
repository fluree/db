(ns fluree.db.query.exec.select
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.json-ld.response :as json-ld-resp]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! chan go go-loop]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(defmulti display
  (fn [match db iri-cache compact error-ch]
    (::where/datatype match)))

(defmethod display :default
  [match _ _ _ _]
  (go (::where/val match)))

(defmethod display const/$xsd:anyURI
  [match db iri-cache compact error-ch]
  (go
    (let [v (::where/val match)]
      (if-let [cached (get @iri-cache v)]
        cached
        (try* (let [iri (<? (dbproto/-iri db v compact))]
                (vswap! iri-cache assoc v iri)
                iri)
              (catch* e
                      (log/error e "Error displaying iri:" v)
                      (>! error-ch e)))))))

(defmulti format
  (fn [selector db iri-cache compact error-ch solution]
    (if (map? selector)
      (::selector selector)
      :var)))

(defmethod format :var
  [variable db iri-cache compact error-ch solution]
  (-> solution
      (get variable)
      (display db iri-cache compact error-ch)))

(defn ->aggregate-selector
  [variable function]
  {::selector :aggregate
   ::variable variable
   ::function function})

(defmethod format :aggregate
  [{::keys [variable function]} db iri-cache compact error-ch solution]
  (let [agg-ch (chan 1 (map function))]
    (-> (format variable db iri-cache compact error-ch solution)
        (async/pipe agg-ch))))

(defn ->subgraph-selector
  [variable selection spec depth]
  {::selector  :subgraph
   ::variable  variable
   ::selection selection
   ::depth     depth
   ::spec      spec})

(defmethod format :subgraph
  [{::keys [variable selection depth spec]} db iri-cache compact error-ch solution]
  (go
    (let [sid (-> solution
                  (get variable)
                  ::where/val)]
      (try*
       (let [flakes (<? (query-range/index-range db :spot = [sid]))]
         ;; TODO: Replace these nils with fuel values when we turn fuel back on
         (<? (json-ld-resp/flakes->res db iri-cache compact nil nil spec 0 flakes)))
       (catch* e
               (log/error e "Error formatting subgraph for subject:" sid)
               (>! error-ch e))))))

(defn select-values
  [solution db iri-cache compact error-ch select-clause]
  (if (sequential? select-clause)
    (go-loop [selectors  select-clause
              values     []]
      (if-let [selector (first selectors)]
        (let [value (<! (format selector db iri-cache compact error-ch solution))]
          (recur (rest selectors)
                 (conj values value)))
        values))
    (format select-clause db iri-cache compact error-ch solution)))

(defn select
  [db q error-ch solution-ch]
  (let [compact   (->> q :context json-ld/compact-fn)
        clause    (or (:select q)
                      (:selectOne q))
        iri-cache (volatile! {})
        select-ch (chan)]
    (async/pipeline-async 1
                          select-ch
                          (fn [solution ch]
                            (-> solution
                                (select-values db iri-cache compact error-ch clause)
                                (async/pipe ch)))
                          solution-ch)
    select-ch))
