(ns fluree.db.api.transact
  (:require [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.transact :as tx]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.credential :as cred]))

(defn parse-opts
  [expanded-txn opts txn-context]
  (let [txn-opts (some-> (util/get-first-value expanded-txn const/iri-opts)
                         util/keywordize-keys)
        opts*    (merge txn-opts (util/keywordize-keys opts))]
    (assoc opts* :context txn-context)))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn parsed-opts]
  (go-try
    (let [track-fuel? (or (:maxFuel parsed-opts)
                          (:meta parsed-opts))
          identity    (:did parsed-opts)
          policy-db   (if (policy/policy-enforced-opts? parsed-opts)
                        (let [parsed-context (:context parsed-opts)]
                          (<? (policy/policy-enforce-db db parsed-context parsed-opts)))
                        db)]
      (if track-fuel?
        (let [start-time #?(:clj (System/nanoTime)
                            :cljs (util/current-time-millis))
              fuel-tracker       (fuel/tracker (:maxFuel parsed-opts))]
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

(defn transact!
  ([conn txn]
   (transact! conn txn nil))
  ([conn txn opts]
   (go-try
     (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
           context     (or (ctx-util/txn-context txn)
                           ;; parent context might come from a Verifiable
                           ;; Credential's context
                           (:context opts))
           ledger-id   (extract-ledger-id expanded)
           expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
           triples     (q-parse/parse-txn expanded context)
           parsed-opts (parse-opts expanded opts context)]
       (<? (connection/transact! conn ledger-id triples parsed-opts))))))

(defn credential-transact!
  "Like transact!, but use when leveraging a Verifiable Credential or signed JWS.

  Will throw if signature cannot be extracted."
  [conn txn opts]
  (go-try
   (let [{txn* :subject did :did} (<? (cred/verify txn))
         parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                          (ctx-util/txn-context txn))]
     (<? (transact! conn txn* (assoc opts :raw-txn txn
                                          :did did
                                          :context parent-context))))))

(defn create-with-txn
  ([conn txn]
   (create-with-txn conn txn nil))
  ([conn txn {:keys [context] :as opts}]
   (go-try
     (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
           ledger-id   (extract-ledger-id expanded)
           address     (<? (connection/primary-address conn ledger-id))
           txn-context (or (ctx-util/txn-context txn)
                           context) ;; parent context from credential if present
           parsed-opts (parse-opts expanded opts txn-context)]
      (if (<? (connection/ledger-exists? conn address))
        (throw (ex-info (str "Ledger " ledger-id " already exists")
                        {:status 409 :error :db/ledger-exists}))
        (let [ledger  (<? (connection/create-ledger conn ledger-id parsed-opts))
              triples (q-parse/parse-txn expanded txn-context)

              ;; commit API takes a did-map and parsed context as opts
              ;; whereas stage API takes a did IRI and unparsed context.
              ;; Dissoc them until deciding at a later point if they can carry through.
              cmt-opts (dissoc parsed-opts :context :did)]
          (<? (connection/transact-ledger! conn ledger triples cmt-opts))))))))

(defn credential-create-with-txn!
  [conn txn]
  (let [{txn* :subject did :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :did did :context parent-context})))
