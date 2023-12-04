(ns fluree.db.api.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.transact :as tx]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.json-ld.policy :as perm]))

(defn parse-opts
  [parsed-opts opts]
  (reduce (fn [opts* [k v]] (assoc opts* (keyword k) v))
          parsed-opts
          opts))

(defn stage
  [db txn parsed-opts]
  (go-try
    (let [{txn :subject did :did} (or (<? (cred/verify txn))
                                      {:subject txn})
          txn-context             (or (:context parsed-opts)
                                      (ctx-util/txn-context txn))
          expanded                (json-ld/expand (ctx-util/use-fluree-context txn))
          opts                    (util/get-first-value expanded const/iri-opts)

          parsed-opts             (cond-> parsed-opts
                                    did (assoc :did did)
                                    txn-context (assoc :context txn-context))

          {:keys [maxFuel meta] :as parsed-opts*} (parse-opts parsed-opts opts)

          s-ctx (some-> txn ctx-util/extract-supplied-context json-ld/parse-context)
          db*   (if-let [policy-identity (perm/parse-policy-identity parsed-opts* s-ctx)]
                  (<? (perm/wrap-policy db policy-identity))
                  db)
          txn-context   (dbproto/-context db (:context parsed-opts))]
      (if (or maxFuel meta)
        (let [start-time   #?(:clj  (System/nanoTime)
                              :cljs (util/current-time-millis))
              fuel-tracker (fuel/tracker maxFuel)]
          (try*
            (let [result (<? (tx/stage db* txn-context fuel-tracker expanded parsed-opts*))]
              {:status 200
               :result result
               :time   (util/response-time-formatted start-time)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
              (throw (ex-info "Error staging database"
                              {:time (util/response-time-formatted start-time)
                               :fuel (fuel/tally fuel-tracker)}
                              e)))))
        (<? (tx/stage db* txn-context expanded parsed-opts*))))))

(defn transact!
  [conn txn]
  (go-try
    (let [{txn :subject did :did} (or (<? (cred/verify txn))
                                      {:subject txn})

          txn-context (ctx-util/txn-context txn)
          expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
          ledger-id   (util/get-first-value expanded const/iri-ledger)
          _ (when-not ledger-id
              (throw (ex-info "Invalid transaction, missing required key: ledger."
                              {:status 400 :error :db/invalid-transaction})))
          address     (<? (nameservice/primary-address conn ledger-id nil))

          opts (cond-> (util/get-first-value expanded const/iri-opts)
                 did         (assoc :did did)
                 txn-context (assoc :context txn-context))

          parsed-opts (parse-opts {} opts)]
      (if-not (<? (nameservice/exists? conn address))
        (throw (ex-info "Ledger does not exist" {:ledger address}))
        (let [ledger (<? (jld-ledger/load conn address))
              db     (<? (stage (ledger-proto/-db ledger) txn parsed-opts))]
          (<? (ledger-proto/-commit! ledger db)))))))

(defn create-with-txn
  [conn txn]
  (go-try
    (let [{txn :subject did :did} (or (<? (cred/verify txn))
                                      {:subject txn})

          txn-context (ctx-util/txn-context txn)
          expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
          ledger-id   (util/get-first-value expanded const/iri-ledger)
          _ (when-not ledger-id
              (throw (ex-info "Invalid transaction, missing required key: ledger."
                              {:status 400 :error :db/invalid-transaction})))
          address     (<? (nameservice/primary-address conn ledger-id nil))

          opts (cond-> (util/get-first-value expanded const/iri-opts)
                 did         (assoc :did did)
                 txn-context (assoc :context txn-context))

          parsed-opts (parse-opts {} opts)]
      (if (<? (nameservice/exists? conn address))
        (throw (ex-info (str "Ledger " ledger-id " already exists")
                        {:status 409 :error :db/ledger-exists}))
        (let [ledger (<? (jld-ledger/create conn ledger-id parsed-opts))
              db     (<? (stage (ledger-proto/-db ledger) txn parsed-opts))]
          (<? (ledger-proto/-commit! ledger db)))))))
