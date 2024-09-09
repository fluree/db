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
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.ledger :as ledger]))

(defn parse-opts
  [expanded-txn opts txn-context]
  (-> (util/get-first-value expanded-txn const/iri-opts)
      (merge opts) ;; override opts 'did', 'raw-txn' with credential's if present
      (util/keywordize-keys)
      (assoc :context txn-context)))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn parsed-opts]
  (go-try
   (let [track-fuel? (or (:maxFuel parsed-opts)
                         (:meta parsed-opts))
         identity    (:did parsed-opts)
         policy-db   (if identity
                       (<? (policy/wrap-identity-policy db identity true nil))
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

(defn transact-ledger!
  [ledger txn {:keys [expanded? context triples?] :as opts}]
  (go-try
   (let [txn-context (or (ctx-util/txn-context txn)
                         context) ;; parent context might come from a Verifiable Credential's context
         expanded    (if expanded?
                       txn
                       (json-ld/expand (ctx-util/use-fluree-context txn)))
         triples     (if triples?
                       txn
                       (q-parse/parse-txn expanded txn-context))
         parsed-opts (parse-opts expanded opts txn-context)
         db          (<? (stage-triples (ledger/-db ledger) triples parsed-opts))
         ;; commit API takes a did-map and parsed context as opts
         ;; whereas stage API takes a did IRI and unparsed context.
         ;; Dissoc them until deciding at a later point if they can carry through.
         cmt-opts    (dissoc parsed-opts :context :did)] ;; possible keys at f.d.ledger.json-ld/enrich-commit-opts
     (<? (ledger/-commit! ledger db cmt-opts)))))

(defn transact!
  ([conn txn]
   (transact! conn txn {:raw-txn txn}))
  ([conn txn opts]
   (go-try
     (let [expanded  (json-ld/expand (ctx-util/use-fluree-context txn))
           ledger-id (extract-ledger-id expanded)]
      (<? (transact! conn ledger-id txn opts)))))
  ([conn ledger-id txn opts]
   (go-try
     (let [address (<? (connection/primary-address conn ledger-id))]
       (if-not (<? (connection/ledger-exists? conn address))
         (throw (ex-info "Ledger does not exist" {:ledger address}))
         (let [ledger (<? (connection/load-ledger conn address))]
           (<? (transact-ledger! ledger txn opts))))))))

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
  ([conn txn] (create-with-txn conn txn nil))
  ([conn txn {:keys [context] :as opts}]
   (go-try
     (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
           txn-context (or (ctx-util/txn-context txn)
                           context) ;; parent context from credential if present
           ledger-id   (extract-ledger-id expanded)
           address     (<? (connection/primary-address conn ledger-id))
           parsed-opts (parse-opts expanded opts txn-context)]
      (if (<? (connection/ledger-exists? conn address))
        (throw (ex-info (str "Ledger " ledger-id " already exists")
                        {:status 409 :error :db/ledger-exists}))
        (let [ledger (<? (connection/create-ledger conn ledger-id parsed-opts))]
          (<? (transact-ledger! ledger expanded (assoc parsed-opts :expanded? true)))))))))

(defn credential-create-with-txn!
  [conn txn]
  (let [{txn* :subject did :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :did did :context parent-context})))
