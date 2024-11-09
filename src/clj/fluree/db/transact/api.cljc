(ns fluree.db.transact.api
  (:require [fluree.db.constants :as const]
            [fluree.db.transact :as transact]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.context :as ctx-util]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.credential :as cred]
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

(defn stage
  [db txn opts]
  (go-try
   (let [txn-context (or (ctx-util/txn-context txn)
                         (:context opts))
         expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
         parsed-opts (parse-opts expanded opts txn-context)
         parsed-txn  (q-parse/parse-txn expanded txn-context)]
     (<? (transact/stage-triples db parsed-txn parsed-opts)))))

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
  ([conn txn override-opts]
   (go-try
     (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
           context     (or (ctx-util/txn-context txn)
                           ;; parent context might come from a Verifiable
                           ;; Credential's context
                           (:context override-opts))
           ledger-id   (extract-ledger-id expanded)
           triples     (q-parse/parse-txn expanded context)
           parsed-opts (parse-opts expanded override-opts context)]
       (<? (connection/transact! conn ledger-id triples parsed-opts))))))

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
  ([conn txn]
   (create-with-txn conn txn nil))
  ([conn txn {:keys [context] :as override-opts}]
   (go-try
    (let [expanded    (json-ld/expand (ctx-util/use-fluree-context txn))
          txn-context (or (ctx-util/txn-context txn)
                          context) ;; parent context from credential if present
          ledger-id   (extract-ledger-id expanded)
          address     (<? (connection/primary-address conn ledger-id))
          parsed-opts (parse-opts expanded override-opts txn-context)]
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
  (let [{txn* :subject identity :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :identity identity :context parent-context})))
