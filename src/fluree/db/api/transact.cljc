(ns fluree.db.api.transact
  (:require [fluree.db.fuel :as fuel]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :as async-util :refer [<? go-try]]
            [fluree.db.json-ld.transact :as tx]
            [fluree.db.dbproto :as dbproto]))

(defn stage
  [db json-ld opts]
  (go-try
    (if (:meta opts)
      (let [start-time   #?(:clj  (System/nanoTime)
                            :cljs (util/current-time-millis))
            fuel-tracker (fuel/tracker)]
        (try* (let [result (<? (dbproto/-stage db fuel-tracker json-ld opts))]
                {:status 200
                 :result result
                 :time   (util/response-time-formatted start-time)
                 :fuel   (fuel/tally fuel-tracker)})
              (catch* e
                      (throw (ex-info "Error staging database"
                                      (-> e
                                          ex-data
                                          (assoc :time (util/response-time-formatted start-time)
                                                 :fuel (fuel/tally fuel-tracker))))))))
      (<? (dbproto/-stage db json-ld opts)))))

(defn transact!
  [ledger json-ld opts]
  (go-try
    (if (:meta opts)
      (let [start-time   #?(:clj  (System/nanoTime)
                            :cljs (util/current-time-millis))
            fuel-tracker (fuel/tracker)]
        (try* (let [tx-result (<? (tx/transact! ledger fuel-tracker json-ld opts))]
                {:status 200
                 :result tx-result
                 :time   (util/response-time-formatted start-time)
                 :fuel   (fuel/tally fuel-tracker)})
              (catch* e
                      (throw (ex-info "Error updating ledger"
                                      (-> e
                                          ex-data
                                          (assoc :time (util/response-time-formatted start-time)
                                                 :fuel (fuel/tally fuel-tracker))))))))
      (<? (tx/transact! ledger json-ld opts)))))
