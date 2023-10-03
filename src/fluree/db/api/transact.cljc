(ns fluree.db.api.transact
  (:require [clojure.walk :refer [keywordize-keys]]
            [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :as async-util :refer [<? go-try]]
            [fluree.db.json-ld.transact :as tx]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.json-ld :as json-ld]))

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

(defn- parse-json-ld-txn
  "Expands top-level keys and parses any opts in json-ld transaction document,
  for use by `transact!`.

  Throws if required keys @id or @graph are absent."
  [json-ld]
  (let [context-key (cond
                      (contains? json-ld "@context") "@context"
                      (contains? json-ld :context) :context)
        context (get json-ld context-key)]
    (let [parsed-context (json-ld/parse-context context)
          {id "@id" graph "@graph" :as parsed-txn}
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
      (if-not (and id graph)
        (throw (ex-info (str "Invalid transaction, missing required keys:"
                             (when (nil? id)
                               " @id")
                             (when (nil? graph)
                               " @graph")
                             ".")
                        {:status 400 :error :db/invalid-transaction}))
        parsed-txn))))

(defn transact!
  [conn json-ld opts]
  (go-try
    (let [{txn-context "@context"
           txn "@graph"
           ledger-id "@id"
           txn-opts const/iri-opts
           default-context const/iri-default-context} (parse-json-ld-txn json-ld)
          address  (<? (nameservice/primary-address conn ledger-id nil))]
      (if-not (<? (nameservice/exists? conn address))
        (throw (ex-info "Ledger does not exist" {:ledger address}))
        (let [ledger (<? (jld-ledger/load conn address))
              opts* (cond-> opts
                      txn-opts        (merge txn-opts)
                      txn-context     (assoc :txn-context txn-context)
                      default-context (assoc :defaultContext default-context))]
          (if (:meta opts*)
            (let [start-time   #?(:clj  (System/nanoTime)
                                  :cljs (util/current-time-millis))
                  fuel-tracker (fuel/tracker)]
              (try* (let [tx-result (<? (tx/transact! ledger fuel-tracker txn opts*))]
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
            (<? (tx/transact! ledger txn opts*))))))))
