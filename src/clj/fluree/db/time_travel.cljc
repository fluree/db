(ns fluree.db.time-travel
  (:require [clojure.core.async :as async]
            [fluree.db.db.json-ld :as db]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn datetime->t
  "Takes an ISO-8601 datetime string and returns a core.async channel with the
  latest 't' value that is not more recent than that datetime."
  [db datetime]
  (go-try
    (log/debug "datetime->t db:" (pr-str db))
    (let [epoch-datetime (util/str->epoch-ms datetime)
          current-time (util/current-time-millis)
          [start end] (if (< epoch-datetime current-time)
                        [epoch-datetime current-time]
                        [current-time epoch-datetime])
          flakes         (-> db
                             db/root-db
                             (query-range/index-range
                               :post
                               > [const/$_commit:time start]
                               < [const/$_commit:time end])
                             <?)]
      (log/debug "datetime->t index-range:" (pr-str flakes))
      (if (empty? flakes)
        (:t db)
        (let [t (-> flakes first flake/t flake/prev-t)]
          (if (zero? t)
            (throw (ex-info (str "There is no data as of " datetime)
                            {:status 400, :error :db/invalid-query}))
            t))))))

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
          (async/put! pc (assoc db :t t*)))
        (catch* e
          ;; return exception into promise-chan
          (async/put! pc e))))
    pc))
