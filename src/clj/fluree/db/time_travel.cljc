(ns fluree.db.time-travel
  (:require [clojure.core.async :as async]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol TimeTravel
  (datetime->t [db datetime])
  (-as-of [db t]))

(defn as-of
  "Gets database as of a specific moment. Resolves 't' value provided to internal Fluree indexing
  negative 't' long integer value."
  [db t]
  (let [pc (async/promise-chan)]
    (async/go
      (try*
        (let [t* (cond
                   (string? t)  (<? (datetime->t db t)) ; ISO-8601 datetime
                   (pos-int? t) t
                   :else        (throw (ex-info (str "Time travel to t value of: " t " not yet supported.")
                                                {:status 400 :error :db/invalid-query})))]
          (log/debug "as-of t:" t*)
          (async/put! pc (-as-of db t*)))
        (catch* e
          ;; return exception into promise-chan
          (async/put! pc e))))
    pc))
