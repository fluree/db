(ns fluree.db.api.transact
  (:require [fluree.db.connection :as connection]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as ctx-util]))

(defn parse-opts
  [txn override-opts txn-context]
  (let [txn-opts (some-> (q-parse/get-named txn "opts")
                         (syntax/coerce-txn-opts))
        opts     (merge txn-opts (some-> override-opts syntax/coerce-txn-opts))]
    (-> opts
        (assoc :context txn-context)
        (update :identity #(or % (:did opts)))
        (dissoc :did))))

(defn format-txn
  [txn override-opts]
  (if (sparql/sparql-format? override-opts)
    (sparql/->fql txn)
    txn))

(defn stage
  [db txn override-opts]
  (go-try
    (let [txn*       (format-txn txn override-opts)
          parsed-txn (q-parse/parse-txn txn* override-opts)]
      (<? (connection/stage-triples db parsed-txn)))))

(defn extract-ledger-id
  "Extracts ledger-id from expanded json-ld transaction"
  [txn]
  (or (q-parse/get-named txn "ledger")
      (throw (ex-info "Invalid transaction, missing required key: ledger."
                      {:status 400 :error :db/invalid-transaction}))))

(defn transact!
  ([conn txn]
   (transact! conn txn nil))
  ([conn txn override-opts]
   (go-try
     (let [txn*           (format-txn txn override-opts)
           override-opts* (assoc override-opts :format :fql)
           ledger-id      (extract-ledger-id txn*)
           parsed-txn     (q-parse/parse-txn txn* override-opts*)
           context        (or (ctx-util/txn-context txn*)
                              ;; parent context might come from a Verifiable
                              ;; Credential's context
                              (:context override-opts*))
           parsed-opts    (parse-opts txn override-opts* context)]
       (<? (connection/transact! conn ledger-id parsed-txn parsed-opts))))))

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
     (let [txn-context (or (ctx-util/txn-context txn)
                           context) ;; parent context from credential if present
           ledger-id   (extract-ledger-id txn)
           address     (<? (connection/primary-address conn ledger-id))
           parsed-opts (-> (parse-opts txn override-opts txn-context)
                           (syntax/coerce-ledger-opts))]
       (if (<? (connection/ledger-exists? conn address))
         (throw (ex-info (str "Ledger " ledger-id " already exists")
                         {:status 409 :error :db/ledger-exists}))
         (let [ledger     (<? (connection/create-ledger conn ledger-id parsed-opts))
               parsed-txn (q-parse/parse-txn txn override-opts)

               ;; commit API takes a did-map and parsed context as opts
               ;; whereas stage API takes a did IRI and unparsed context.
               ;; Dissoc them until deciding at a later point if they can carry through.
               cmt-opts (dissoc parsed-opts :context :did)]
           (<? (connection/transact-ledger! conn ledger parsed-txn cmt-opts))))))))

(defn credential-create-with-txn!
  [conn txn]
  (let [{txn* :subject identity :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :identity identity :context parent-context})))
