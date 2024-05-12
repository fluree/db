(ns fluree.db.api.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.transact :as tx]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.ledger :as ledger]))

(defn parse-opts
  [parsed-opts opts]
  (reduce (fn [opts* [k v]] (assoc opts* (keyword k) v))
          parsed-opts
          opts))

(defn stage
  [db txn {:keys [raw-txn] :as opts}]
  (go-try
    (let [{txn* :subject did :did} (or (<? (cred/verify txn))
                                       {:subject txn})
          txn-context              (or (:context opts)
                                       (ctx-util/txn-context txn*))

          expanded            (json-ld/expand (ctx-util/use-fluree-context txn*))
          txn-opts            (util/get-first-value expanded const/iri-opts)
          {:keys [maxFuel meta]
           :as   parsed-opts} (cond-> opts
                                (not raw-txn) (assoc :raw-txn txn)
                                did           (assoc :did did)
                                txn-context   (assoc :context txn-context)
                                true          (parse-opts txn-opts))]
      (if (or maxFuel meta)
        (let [start-time   #?(:clj  (System/nanoTime)
                              :cljs (util/current-time-millis))
              fuel-tracker (fuel/tracker maxFuel)]
          (try*
            (let [result (<? (tx/stage db fuel-tracker expanded parsed-opts))]
              {:status 200
               :result result
               :time   (util/response-time-formatted start-time)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
              (throw (ex-info "Error staging database"
                              {:time (util/response-time-formatted start-time)
                               :fuel (fuel/tally fuel-tracker)}
                              e)))))
        (<? (tx/stage db expanded parsed-opts))))))

(defn transact!
  [conn txn]
  (go-try
    (let [{txn* :subject did :did} (or (<? (cred/verify txn))
                                       {:subject txn})

          txn-context (ctx-util/txn-context txn*)
          expanded    (json-ld/expand (ctx-util/use-fluree-context txn*))
          ledger-id   (util/get-first-value expanded const/iri-ledger)
          _ (when-not ledger-id
              (throw (ex-info "Invalid transaction, missing required key: ledger."
                              {:status 400 :error :db/invalid-transaction})))
          address     (<? (nameservice/primary-address conn ledger-id nil))

          opts (cond-> (util/get-first-value expanded const/iri-opts)
                 did         (assoc :did did)
                 txn-context (assoc :context txn-context))

          parsed-opts (parse-opts {:raw-txn txn} opts)]
      (if-not (<? (nameservice/exists? conn address))
        (throw (ex-info "Ledger does not exist" {:ledger address}))
        (let [ledger (<? (jld-ledger/load conn address))
              db     (<? (ledger/-db ledger))
              staged (<? (stage db txn* parsed-opts))]
          (<? (ledger/-commit! ledger staged)))))))

(defn create-with-txn
  [conn txn]
  (go-try
    (let [{txn* :subject did :did} (or (<? (cred/verify txn))
                                       {:subject txn})

          txn-context (ctx-util/txn-context txn*)
          expanded    (json-ld/expand (ctx-util/use-fluree-context txn*))
          ledger-id   (util/get-first-value expanded const/iri-ledger)
          _ (when-not ledger-id
              (throw (ex-info "Invalid transaction, missing required key: ledger."
                              {:status 400 :error :db/invalid-transaction})))
          address     (<? (nameservice/primary-address conn ledger-id nil))

          opts (cond-> (util/get-first-value expanded const/iri-opts)
                 did         (assoc :did did)
                 txn-context (assoc :context txn-context))

          parsed-opts (parse-opts {:raw-txn txn} opts)]
      (if (<? (nameservice/exists? conn address))
        (throw (ex-info (str "Ledger " ledger-id " already exists")
                        {:status 409 :error :db/ledger-exists}))
        (let [ledger (<? (jld-ledger/create conn ledger-id parsed-opts))
              db     (<? (ledger/-db ledger))
              staged (<? (stage db txn* parsed-opts))]
          (<? (ledger/-commit! ledger staged)))))))
