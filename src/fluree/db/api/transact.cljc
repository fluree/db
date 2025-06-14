(ns fluree.db.api.transact
  (:refer-clojure :exclude [update])
  (:require [clojure.core.async :as async]
            [fluree.db.connection :as connection]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.query.fql.syntax :as syntax]
            [fluree.db.query.sparql :as sparql]
            [fluree.db.query.turtle.parse :as turtle]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as ctx-util]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]))

(defn format-txn
  [txn override-opts]
  (if (sparql/sparql-format? override-opts)
    (sparql/->fql txn)
    txn))

(defn insert
  [db rdf {:keys [context format] :as _opts}]
  (go-try
    (let [context* (json-ld/parse-context context)
          parsed-triples (if (= :turtle format)
                           (turtle/parse rdf)
                           (parse/jld->parsed-triples rdf nil context*))
          parsed-txn {:insert parsed-triples}]
      (<? (transact/stage-triples db parsed-txn)))))

(defn upsert
  [db rdf opts]
  (go-try
    (let [opts* (assoc opts :context (json-ld/parse-context (:context opts)))
          parsed-txn (parse/parse-upsert-txn rdf opts*)]
      (<? (transact/stage-triples db parsed-txn)))))

(defn update
  [db txn override-opts]
  (go-try
    (let [parsed-txn (-> txn
                         (format-txn override-opts)
                         (parse/parse-stage-txn override-opts))]
      (<? (transact/stage-triples db parsed-txn)))))

(defn- not-found?
  [e]
  (-> e ex-data :status (= 404)))

(defn transact!
  ([conn txn]
   (transact! conn txn nil))
  ([conn txn override-opts]
   (go-try
     (let [parsed-txn (-> txn
                          (format-txn override-opts)
                          (parse/parse-ledger-txn override-opts))
           ledger-id (:ledger-id parsed-txn)
           ledger (async/<! (connection/load-ledger conn ledger-id))]
       (if (util/exception? ledger)
         (if (not-found? ledger)
           (throw (ex-info (str "Ledger " ledger-id " does not exist")
                           {:status 409 :error :db/ledger-not-exists}
                           ledger))
           (throw ledger))
         (<? (transact/transact-ledger! ledger parsed-txn)))))))

(defn credential-transact!
  "Like transact!, but use when leveraging a Verifiable Credential or signed JWS.

  Will throw if signature cannot be extracted."
  [conn txn opts]
  (go-try
    (let [{txn* :subject identity :did} (<? (cred/verify txn))
          parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                           (ctx-util/txn-context txn))]
      (<? (transact! conn txn* (assoc opts
                                      :raw-txn txn
                                      :identity identity
                                      :context parent-context))))))

(defn create-with-txn
  ([conn txn]
   (create-with-txn conn txn nil))
  ([conn txn override-opts]
   (go-try
     (let [;; commit API takes a did-map and parsed context as opts
           ;; whereas stage API takes a did IRI and unparsed context.
           ;; Dissoc them until deciding at a later point if they can carry through.
           {:keys [ledger-id] :as parsed-txn}
           (-> txn
               (format-txn override-opts)
               (parse/parse-ledger-txn override-opts)
               (clojure.core/update :opts dissoc :context :did :identity)) ; Using an
                                                                           ; identity option
                                                                           ; with an empty
                                                                           ; ledger will
                                                                           ; always fail
                                                                           ; policy checks
                                                                           ; because there
                                                                           ; are no policies
                                                                           ; to check.
           ledger-opts (-> parsed-txn :opts syntax/coerce-ledger-opts)
           ledger      (<? (connection/create-ledger conn ledger-id ledger-opts))]
       (<? (transact/transact-ledger! ledger parsed-txn))))))

(defn credential-create-with-txn!
  [conn txn]
  (let [{txn* :subject identity :did} (<? (cred/verify txn))
        parent-context (when (map? txn) ;; parent-context only relevant for verifiable credential
                         (ctx-util/txn-context txn))]
    (create-with-txn conn txn* {:raw-txn txn, :identity identity :context parent-context})))
