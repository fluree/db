(ns fluree.db.json-ld.migrate.sid
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.async-db :as async-db]
            [fluree.db.constants :as const]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.ledger :as ledger]
            [fluree.db.connection :as connection]
            [fluree.db.query.update :as update]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util :refer [get-first get-id get-first-id get-first-value]]
            [fluree.db.util.log :as log]
            [fluree.db.commit.storage :as commit-storage]))

(defrecord NamespaceMapping [mapping]
  iri/IRICodec
  (encode-iri [_ iri]
    (update/generate-sid! mapping iri))
  (decode-sid [_ sid]
    (iri/sid->iri sid (:namespace-codes @mapping))))

(defn db->namespace-mapping
  "Take only the parts of a db necessary to generate SIDs correctly."
  [db]
  (-> db
      (select-keys [:namespaces :namespace-codes])
      volatile!
      NamespaceMapping.))

(defn set-namespaces
  "Take a NamespaceMapping and a db and integrate the state."
  [db ns-mapping]
  (let [{:keys [namespaces namespace-codes]} @(:mapping ns-mapping)]
    (assoc db :namespaces namespaces, :namespace-codes namespace-codes)))

(defn migrate-commit
  "Turns the data from the commit into flakes and re-generates the commit to include all
  the necessary information."
  [ledger db [commit _proof db-data]]
  (go-try
    (let [commit-id          (get-id commit)
          t-new              (flake-db/db-t db-data)
          _ (log/info "Migrating commit " commit-id " at t " t-new)

          ;; the ns-mapping has all the parts of the db necessary for create-flakes to encode iris properly
          ns-mapping         (db->namespace-mapping db)

          assert             (flake-db/db-assert db-data)
          asserted-flakes    (flake-db/create-flakes true ns-mapping t-new assert)
          retract            (flake-db/db-retract db-data)
          retracted-flakes   (flake-db/create-flakes false ns-mapping t-new retract)

          {:keys [previous issuer message data] :as commit-metadata}
          (commit-data/json-ld->map commit db)

          commit-id          (:id commit-metadata)
          commit-sid         (iri/encode-iri ns-mapping commit-id)

          db-sid             (iri/encode-iri ns-mapping (:id data))
          metadata-flakes    (commit-data/commit-metadata-flakes commit-metadata
                                                                 t-new commit-sid db-sid)

          previous-id        (when previous (:id previous))
          prev-commit-flakes (when previous-id
                               (commit-data/prev-commit-flakes db t-new commit-sid
                                                               previous-id))
          prev-data-id       (get-first-id previous const/iri-data)
          prev-db-flakes     (when prev-data-id
                               (commit-data/prev-data-flakes db db-sid t-new
                                                             prev-data-id))
          issuer-flakes      (when-let [issuer-iri (:id issuer)]
                               (commit-data/issuer-flakes db t-new commit-sid issuer-iri))
          message-flakes     (when message
                               (commit-data/message-flakes t-new commit-sid message))
          all-flakes         (-> db
                                 (get-in [:novelty :spot])
                                 empty
                                 (into metadata-flakes)
                                 (into retracted-flakes)
                                 (into asserted-flakes)
                                 (cond-> prev-commit-flakes (into prev-commit-flakes)
                                         prev-db-flakes (into prev-db-flakes)
                                         issuer-flakes (into issuer-flakes)
                                         message-flakes (into message-flakes)))
          tx-state           (flake.transact/->tx-state
                               :db db
                               :txn (get-first-value commit const/iri-txn)
                               :author (let [author (get-first-value commit const/iri-author)]
                                         (when-not (str/blank? author) author))
                               :annotation (get-first-value commit const/iri-annotation))
          staged-db          (-> (<? (flake.transact/final-db db all-flakes tx-state))
                                 :db-after
                                 (set-namespaces ns-mapping))
          committed-db       (<? (connection/commit! ledger staged-db
                                                     {:time (get-first-value commit const/iri-time)}))]
      (if (async-db/db? committed-db)
        (<? (async-db/deref-async committed-db))
        committed-db))))

(defn migrate-commits
  "Reduce over each commmit and integrate its data into the ledger's db."
  [ledger branch tuples-chans]
  (go-try
    (loop [[[commit-tuple ch] & r] tuples-chans
           db (let [current-db (ledger/current-db ledger)]
                (if (async-db/db? current-db)
                  (<? (async-db/deref-async current-db))
                  current-db))]
      (if commit-tuple
        (recur r (<? (migrate-commit ledger db commit-tuple)))
        db))))

(defn migrate
  "Migrate the ledger at the designated address. changes-ch, if provided, will return a
  stream of updated index nodes.

  Old commits are lacking the f:namespaces key in the commit data file, and also lack a
  link to the genesis commit from t1. Also, the flakes stored in the index files are not
  compact SIDs. This migration traverses the commit chain and holds them all in memory,
  then processes each one, properly generating the necessary namespace codes for SIDs
  along the way and rewriting the commit chain to use the newer commit structure."
  ([conn ledger-alias indexing-opts]
   (migrate conn ledger-alias indexing-opts false nil))
  ([{:keys [store] :as conn} ledger-alias indexing-opts force changes-ch]
   (go-try
     (let [ledger-address       (<? (connection/primary-address conn ledger-alias))
           last-commit-addr     (<? (connection/lookup-commit conn ledger-address))
           last-verified-commit (<? (commit-storage/read-commit-jsonld store last-commit-addr))
           last-commit          (first last-verified-commit)
           version              (get-first-value last-commit const/iri-v)]
       (if (and (= version commit-data/commit-version) (not force))
         (log/info :migrate/sid "ledger" ledger-alias "already migrated. Version:" version)
         (let [last-data-stats   (-> last-commit
                                     (get-first const/iri-data)
                                     (update-keys {const/iri-t :t const/iri-size :size const/iri-flakes :flakes})
                                     (select-keys [:t :size :flakes])
                                     (update-vals (comp :value first)))
               all-commit-tuples (<? (commit-storage/trace-commits store last-commit 1))
               first-commit      (ffirst all-commit-tuples)
               branch            (or (keyword (get-first-value first-commit const/iri-branch))
                                     :main)
               ledger            (<? (ledger/create conn {:alias    ledger-alias
                                                          :did      nil
                                                          :branch   branch
                                                          :indexing indexing-opts
                                                          ::time    (get-first-value first-commit const/iri-time)}))
               tuples-chans      (map (fn [commit-tuple]
                                        [commit-tuple (when changes-ch (async/chan))])
                                      all-commit-tuples)
               _                 (log/info :migrate/sid "ledger" ledger-alias "before stats:" last-data-stats)
               indexed-db        (<? (migrate-commits ledger branch tuples-chans))]
           (log/info :migrate/sid "ledger" ledger-alias "after stats:" (:stats indexed-db))
           (when changes-ch
             (-> (map second tuples-chans)
                 async/merge
                 (async/pipe changes-ch)))
           ledger))))))
