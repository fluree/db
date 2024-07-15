(ns fluree.db.query.history
  (:require [clojure.core.async :as async]
            [fluree.db.query.history.parse :as parse]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? go-try]]))

(defn find-t-endpoints
  [db {:keys [from to at] :as _t}]
  (go-try
    (if at
      (let [t (cond (= :latest at) (:t db)
                    (string? at)   (<? (time-travel/datetime->t db at))
                    (number? at)   at)]
        [t t])
      ;; either (:from or :to)
      [(cond (= :latest from) (time-travel/latest-t db)
             (string? from)   (<? (time-travel/datetime->t db from))
             (number? from)   from
             (nil? from)      1)
       (cond (= :latest to) (time-travel/latest-t db)
             (string? to)   (<? (time-travel/datetime->t db to))
             (number? to)   to
             (nil? to)      (:t db))])))

(defprotocol AuditLog
  (-history [db context from-t to-t commit-details? include error-ch history-q])
  (-commits [db context from-t to-t include error-ch]))

(defn query
  [db context q]
  (go-try
    (let [{:keys [history t commit-details] :as parsed-query}
          (parse/parse-history-query q)
          ;; from and to are positive ints, need to convert to negative or fill in default values
          [from-t to-t] (<? (find-t-endpoints db t))
          error-ch      (async/chan)
          include       (not-empty (select-keys parsed-query [:commit :data :txn]))
          result-ch     (if history
                          (-history db context from-t to-t commit-details include error-ch history)
                          (-commits db context from-t to-t include error-ch))]
      (async/alt! result-ch ([result] result)
                  error-ch  ([e] e)))))
