(ns fluree.db.api.transact
  (:require [clojure.walk :refer [keywordize-keys]]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.transact :as tx]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

(defn stage
  [db json-ld opts]
  (go-try
    (let [opts*    (util/parse-opts opts)
          max-fuel (:max-fuel opts*)]
      (if (::util/track-fuel? opts*)
        (let [start-time   #?(:clj (System/nanoTime)
                              :cljs (util/current-time-millis))
              fuel-tracker (fuel/tracker max-fuel)]
          (try* (let [result (<? (dbproto/-stage db fuel-tracker json-ld opts*))]
                  {:status 200
                   :result result
                   :time   (util/response-time-formatted start-time)
                   :fuel   (fuel/tally fuel-tracker)})
                (catch* e
                  (throw (ex-info "Error staging database"
                                  {:time (util/response-time-formatted start-time)
                                   :fuel (fuel/tally fuel-tracker)}
                                  e)))))
        (<? (dbproto/-stage db json-ld opts*))))))

(defn parse-json-ld-txn
  "Expands top-level keys and parses any opts in json-ld transaction document.
  Throws if required keys @id or @graph are absent."
  [conn context-type json-ld]
  (let [conn-default-ctx (conn-proto/-default-context conn context-type)
        context-key      (cond
                           (contains? json-ld "@context") "@context"
                           (contains? json-ld :context) :context)
        txn-context      (get json-ld context-key)
        _                (log/trace "parse-json-ld-txn txn-context:" txn-context)
        parsed-context   (if (or (nil? txn-context)
                                 (and (sequential? txn-context)
                                      (= "" (first txn-context))))
                           (json-ld/parse-context
                            (cons conn-default-ctx (rest txn-context)))
                           (json-ld/parse-context txn-context))
        _                (log/trace "parse-json-ld-txn parsed-context:" parsed-context)

        {ledger-id const/iri-ledger graph "@graph" :as parsed-txn}
        (into {}
              (map (fn [[k v]]
                     (let [k* (if (= context-key k)
                                "@context"
                                (json-ld/expand-iri k parsed-context))
                           v* (if (= const/iri-opts k*)
                                (keywordize-keys v)
                                v)]
                       [k* v*])))
              json-ld)]
    (if-not (and ledger-id graph)
      (throw (ex-info (str "Invalid transaction, missing required keys:"
                           (when (nil? ledger-id)
                             (str " " (json-ld/compact const/iri-ledger
                                                       parsed-context)))
                           (when (nil? graph)
                             " @graph")
                           ".")
                      {:status 400 :error :db/invalid-transaction}))
      parsed-txn)))

(defn ledger-transact!
  [ledger txn opts]
  (go-try
    (let [opts*    (util/parse-opts opts)
          max-fuel (:max-fuel opts*)]
      (if (::util/track-fuel? opts*)
        (let [start-time   #?(:clj  (System/nanoTime)
                              :cljs (util/current-time-millis))
              fuel-tracker (fuel/tracker max-fuel)]
          (try*
            (let [tx-result (<? (tx/transact! ledger fuel-tracker txn opts*))]
              {:status 200
               :result tx-result
               :time   (util/response-time-formatted start-time)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
              (throw
               (ex-info "Error updating ledger"
                        (-> e
                            ex-data
                            (assoc :time (util/response-time-formatted start-time)
                                   :fuel (fuel/tally fuel-tracker))))))))
        (<? (tx/transact! ledger txn opts*))))))

(defn transact!
  [conn parsed-json-ld opts]
  (go-try
    (let [{txn-context     "@context"
           txn             "@graph"
           ledger-id       const/iri-ledger
           txn-opts        const/iri-opts
           default-context const/iri-default-context} parsed-json-ld
           address         (<? (nameservice/primary-address conn ledger-id nil))]
      (if-not (<? (nameservice/exists? conn address))
        (throw (ex-info "Ledger does not exist" {:ledger address}))
        (let [ledger (<? (jld-ledger/load conn address))
              opts   (cond-> opts
                       txn-opts (merge txn-opts)
                       txn-context (assoc :txn-context txn-context)
                       default-context (assoc :defaultContext default-context))]
          (<? (ledger-transact! ledger txn opts)))))))
