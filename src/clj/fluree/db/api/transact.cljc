(ns fluree.db.api.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.transact :as tx]
            [fluree.db.ledger.json-ld :as jld-ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.ledger :as ledger]
            [fluree.db.query.fql.syntax :as syntax]))

(defn parse-opts
  [expanded-txn override-opts txn-context]
  (let [txn-opts (some-> (util/get-first-value expanded-txn const/iri-opts)
                         (syntax/coerce-txn-opts))
        opts     (merge txn-opts (some-> override-opts syntax/coerce-txn-opts))]
    (-> opts
        (assoc :context txn-context)
        (update :identity #(or % (:did opts)))
        (dissoc :did))))

(defn track-fuel?
  [parsed-opts]
  (or (:max-fuel parsed-opts)
      (:meta parsed-opts)))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn parsed-opts]
  (go-try
    (let [identity    (:identity parsed-opts)
          policy-db   (if (policy/policy-enforced-opts? parsed-opts)
                        (let [parsed-context (:context parsed-opts)]
                          (<? (policy/policy-enforce-db db parsed-context parsed-opts)))
                        db)]
      (if (track-fuel? parsed-opts)
        (let [start-time #?(:clj (System/nanoTime)
                            :cljs (util/current-time-millis))
              fuel-tracker       (fuel/tracker (:max-fuel parsed-opts))]
          (try*
            (let [result (<? (tx/stage policy-db fuel-tracker identity parsed-txn parsed-opts))]
              {:status 200
               :result result
               :time   (util/response-time-formatted start-time)
               :fuel   (fuel/tally fuel-tracker)})
            (catch* e
                    (throw (ex-info "Error staging database"
                                    {:time (util/response-time-formatted start-time)
                                     :fuel (fuel/tally fuel-tracker)}
                                    e)))))
        (<? (tx/stage policy-db identity parsed-txn parsed-opts))))))

(defn stage
  [db txn opts]
  (go-try
   (let [txn-context (or (ctx-util/txn-context txn)
                         (:context opts))
         expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
         parsed-opts (parse-opts expanded opts txn-context)
         parsed-txn  (q-parse/parse-txn expanded txn-context)]
     (<? (stage-triples db parsed-txn parsed-opts)))))

(defn extract-ledger-id
  "Extracts ledger-id from expanded json-ld transaction"
  [expanded-json-ld]
  (if-let [ledger-id (util/get-first-value expanded-json-ld const/iri-ledger)]
    ledger-id
    (throw (ex-info "Invalid transaction, missing required key: ledger."
                    {:status 400 :error :db/invalid-transaction}))))

(defn transact-ledger!
  [ledger txn {:keys [expanded? context triples?] :as opts}]
  (go-try
    (let [expanded    (if expanded?
                        txn
                        (json-ld/expand (ctx-util/use-fluree-context txn)))
          txn-context (if expanded?
                        context
                        (or (ctx-util/txn-context txn)
                            context)) ;; parent context might come from a Verifiable Credential's context
          triples     (if triples?
                        txn
                        (q-parse/parse-txn expanded txn-context))
          parsed-opts (parse-opts expanded opts txn-context)
          staged      (<? (stage-triples (ledger/-db ledger) triples parsed-opts))

          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          ;; possible keys at f.d.ledger.json-ld/enrich-commit-opts
          cmt-opts (dissoc parsed-opts :context :identity)]
      (if (track-fuel? parsed-opts)
        (assoc staged :result (<? (ledger/-commit! ledger (:result staged) cmt-opts)))
        (<? (ledger/-commit! ledger staged cmt-opts))))))

(defn transact!
  ([conn txn] (transact! conn txn nil))
  ([conn txn override-opts]
   (go-try
     (let [expanded  (json-ld/expand (ctx-util/use-fluree-context txn))
           ledger-id (extract-ledger-id expanded)
           opts*     (assoc override-opts :expanded? true
                                 :context (or (ctx-util/txn-context txn)
                                              ;; parent context might come from a Verifiable Credential's context
                                              (:context override-opts)))]
       (<? (transact! conn ledger-id expanded opts*)))))
  ([conn ledger-id txn override-opts]
   (go-try
     (let [address (<? (nameservice/primary-address conn ledger-id nil))]
       (if-not (<? (nameservice/exists? conn address))
         (throw (ex-info "Ledger does not exist" {:ledger address}))
         (let [ledger (<? (jld-ledger/load conn address))]
           (<? (transact-ledger! ledger txn override-opts))))))))

(defn credential-transact!
  "Like transact!, but use when leveraging a Verifiable Credential or signed JWS.

  Will throw if signature cannot be extracted."
  [conn txn opts]
  (go-try
   (let [{txn* :subject identity :did} (<? (cred/verify txn))
         parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                          (ctx-util/txn-context txn))]
     (<? (transact! conn txn* (assoc opts :raw-txn txn
                                          :identity identity
                                          :context parent-context))))))

(defn create-with-txn
  ([conn txn] (create-with-txn conn txn nil))
  ([conn txn {:keys [context] :as override-opts}]
   (go-try
    (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
          txn-context (or (ctx-util/txn-context txn)
                          context) ;; parent context from credential if present
          ledger-id   (extract-ledger-id expanded)
          address     (<? (nameservice/primary-address conn ledger-id nil))
          parsed-opts (parse-opts expanded override-opts txn-context)]
      (if (<? (nameservice/exists? conn address))
        (throw (ex-info (str "Ledger " ledger-id " already exists")
                        {:status 409 :error :db/ledger-exists}))
        (let [ledger (<? (jld-ledger/create conn ledger-id parsed-opts))]
          (<? (transact-ledger! ledger expanded (assoc parsed-opts :expanded? true)))))))))

(defn credential-create-with-txn!
  [conn txn]
  (let [{txn* :subject identity :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :identity identity :context parent-context})))
