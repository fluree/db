(ns fluree.db.json-ld.commit
  (:require [fluree.json-ld :as json-ld]
            [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.util.core :as util]
            [fluree.db.util.context :as context]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn- did-from-private
  [private-key]
  (let [acct-id (crypto/account-id-from-private private-key)]
    (str "did:fluree:" acct-id)))

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn- enrich-commit-opts
  [ledger {:keys [context did private message tag file-data? index-files-ch] :as _opts}]
  (let [context*      (parse-commit-context context)
        private*      (or private
                          (:private did)
                          (:private (ledger/-did ledger)))
        did*          (or (some-> private*
                                  did-from-private)
                          did
                          (ledger/-did ledger))
        ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)]
    {:message        message
     :tag            tag
     :file-data?     file-data? ;; if instead of returning just a db from commit, return also the written files (for consensus)
     :context        context*
     :private        private*
     :did            did*
     :ctx-used-atom  ctx-used-atom
     :compact-fn     compact-fn
     :compact        (fn [iri] (json-ld/compact iri compact-fn))
     :id-key         (json-ld/compact "@id" compact-fn)
     :type-key       (json-ld/compact "@type" compact-fn)
     :index-files-ch index-files-ch})) ;; optional async chan passed in which will stream out all new index files created (for consensus)

(defn new-t?
  [ledger-commit db-commit]
  (let [ledger-t (commit-data/t ledger-commit)]
    (or (nil? ledger-t)
        (flake/t-after? (commit-data/t db-commit)
                        ledger-t))))

(defn write-commit
  [conn alias {:keys [did private]} commit]
  (go-try
    (let [[commit* jld-commit] (commit-data/commit->jsonld commit)
          signed-commit        (if did
                                 (<? (cred/generate jld-commit private (:id did)))
                                 jld-commit)
          commit-res           (<? (connection/-c-write conn alias signed-commit))
          commit**             (commit-data/update-commit-address commit* (:address commit-res))]
      {:commit-map    commit**
       :commit-jsonld jld-commit
       :write-result  commit-res})))

(defn push-commit
  [conn {:keys [state] :as _ledger} {:keys [commit-map commit-jsonld write-result]}]
  (nameservice/push! conn (assoc commit-map
                                 :meta write-result
                                 :json-ld commit-jsonld
                                 :ledger-state state)))

(defn do-commit+push
  "Writes commit and pushes, kicks off indexing if necessary."
  [{:keys [conn alias] :as ledger} {:keys [commit branch] :as db} keypair]
  (go-try
    (let [ledger-commit (:commit (ledger/-status ledger branch))
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          _             (log/debug "do-commit+push new-commit:" new-commit)

          {:keys [commit-map write-result] :as commit-write-map}
          (<? (write-commit conn alias keypair new-commit))

          db*   (assoc db :commit commit-map)
          db**  (if (new-t? ledger-commit commit)
                  (commit-data/add-commit-flakes (:prev-commit db) db*)
                  db*)
          db*** (ledger/-commit-update! ledger branch (dissoc db** :txns))
          push-res      (<? (push-commit conn ledger commit-write-map))]
      {:commit-res write-result
       :push-res   push-res
       :db         db***})))

(defn newer-commit?
  [db commit]
  (flake/t-after? (commit-data/t (:commit db))
                  (commit-data/t commit)))

(defn update-commit-fn
  "Returns a fn that receives a newly indexed db as its only argument.
  Will updated the provided committed-db with the new index, then create
  a new commit and push to the name service(s) if configured to do so."
  [ledger committed-db commit-opts]
  (fn [indexed-db]
    (let [indexed-commit (:commit indexed-db)
          new-db         (if (newer-commit? committed-db indexed-commit)
                           (dbproto/-index-update committed-db (:index indexed-commit))
                           indexed-db)]
      (do-commit+push ledger new-db commit-opts))))

(defn run-index
  "Runs indexer. Will update the latest commit file with new index point
  once completed.

  If optional changes-ch is provided, will stream indexing updates to it
  so it can be replicated via consensus to other servers as needed."
  ([ledger db commit-opts]
   (run-index ledger db commit-opts nil))
  ([{:keys [indexer] :as ledger} db commit-opts changes-ch]
   (let [update-fn (update-commit-fn ledger db commit-opts)]
     ;; call indexing process with update-commit-fn to push out an updated commit once complete
     (indexer/-index indexer db {:update-commit update-fn
                                 :changes-ch    changes-ch}))))

(defn write-transactions!
  [conn {:keys [alias] :as _ledger} staged]
  (go-try
    (loop [[[txn author-did annotation] & r] staged
           results                []]
      (if txn
        (let [{txn-id :address} (<? (connection/-txn-write conn alias txn))]
          (recur r (conj results [txn-id author-did annotation])))
        results))))

(defn commit
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  [{:keys [alias conn] :as ledger} {:keys [t stats commit] :as db} opts]
  (go-try
    (let [{:keys [did message tag file-data? index-files-ch] :as opts*}
          (enrich-commit-opts ledger opts)

          {:keys [dbid db-jsonld staged-txns]}
          (jld-db/db->jsonld db opts*)

          [[txn-id author annotation] :as txns]
          (<? (write-transactions! conn ledger staged-txns))

          ledger-update-res (<? (connection/-c-write conn alias db-jsonld)) ; write commit data
          db-address        (:address ledger-update-res) ; may not have address (e.g. IPFS) until after writing file

          commit-time (util/current-time-iso)
          _           (log/debug "Committing t" t "at" commit-time)

          base-commit-map {:old-commit commit
                           :issuer     did
                           :message    message
                           :tag        tag
                           :dbid       dbid
                           :t          t
                           :time       commit-time
                           :db-address db-address
                           :author     (or author "")
                           :annotation annotation
                           :txn-id     (if (= 1 (count txns)) txn-id "")
                           :flakes     (:flakes stats)
                           :size       (:size stats)}
          new-commit      (commit-data/new-db-commit-map base-commit-map)
          db*             (-> db
                              (update :staged empty)
                              (assoc :commit new-commit
                                     :prev-commit commit))
          keypair         (select-keys opts* [:did :private])

          {db**             :db
           commit-file-meta :commit-res}
          (<? (do-commit+push ledger db* keypair))]

      (run-index ledger db** opts* index-files-ch)

      (if file-data?
        {:data-file-meta   ledger-update-res
         :commit-file-meta commit-file-meta
         :db               db**}
        db**))))
