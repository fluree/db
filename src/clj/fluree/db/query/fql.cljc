(ns fluree.db.query.fql
  (:refer-clojure :exclude [var? vswap!])
  (:require [clojure.core.async :as async]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.exec :as exec])
  #?(:cljs (:require-macros [clojure.core])))

#?(:clj (set! *warn-on-reflection* true))

(defn query
  "Returns core async channel with results or exception"
  ([ds query-map]
   (query ds nil query-map))
  ([ds fuel-tracker query-map]
   (let [q (try*
               (parse/parse-query query-map)
               (catch* e e))]
     (if (util/exception? q)
       (async/to-chan! [q])
       (exec/query ds fuel-tracker q)))))
