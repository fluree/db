(ns fluree.db.ledger
  (:require [clojure.string :as str]
            [fluree.db.branch :as branch]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.did :as did]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.index.metadata :as index.meta]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.track :as track]
            [fluree.db.transact :as transact]
            [fluree.db.util :as util :refer [get-first get-first-value]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as context]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn get-branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

(defn indexing-enabled?
  [ledger branch]
  (-> ledger (get-branch-meta branch) branch/indexing-enabled?))

(defn available-branches
  [{:keys [state] :as _ledger}]
  (-> @state :branches keys))

;; TODO - no time travel, only latest db on a branch thus far
(defn current-db
  ([ledger]
   (current-db ledger nil))
  ([ledger branch]
   (if-let [branch-meta (get-branch-meta ledger branch)] ; if branch is nil, will return default
     (branch/current-db branch-meta)
     (throw (ex-info (str "Invalid branch: " branch " is not one of:"
                          (available-branches ledger))
                     {:status 400, :error :db/invalid-branch})))))

(defn update-commit!
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  ([ledger branch-name db]
   (update-commit! ledger branch-name db nil))
  ([{:keys [state] :as ledger} branch-name db index-files-ch]
   (log/debug "Attempting to update ledger:" (:alias ledger)
              "and branch:" branch-name "with new commit to t" (:t db))
   (if-let [branch-meta (get-branch-meta ledger branch-name)]
     (-> branch-meta
         (branch/update-commit! db index-files-ch)
         branch/current-db)
     (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                          "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                     {:status 400, :error :db/invalid-branch})))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  ([ledger]
   (status ledger const/default-branch-name))
  ([{:keys [address alias] :as ledger} requested-branch]
   (let [branch-data (get-branch-meta ledger requested-branch)
         current-db  (branch/current-db branch-data)
         {:keys [commit stats t]} current-db
         {:keys [size flakes]} stats
         branch (or requested-branch (:branch @(:state ledger)))]
     {:address address
      :alias   alias
      :branch  branch
      :t       t
      :size    size
      :flakes  flakes
      :commit  commit})))

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [ledger expanded-commit expanded-data]
  (go-try
    (let [branch    (get-first-value expanded-commit const/iri-branch)
          commit-t  (-> expanded-commit
                        (get-first const/iri-data)
                        (get-first-value const/iri-fluree-t))
          db        (current-db ledger branch)
          current-t (:t db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger)
                 "at t value:" commit-t "where current cached db t value is:"
                 current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond
        (= commit-t (flake/next-t current-t))
        (let [updated-db (<? (transact/-merge-commit db expanded-commit expanded-data))]
          (update-commit! ledger branch updated-db)
          ::updated)

        ;; missing some updates, dump in-memory ledger forcing a reload
        (flake/t-after? commit-t (flake/next-t current-t))
        (do
          (log/warn "Received commit update that is more than 1 ahead of current ledger state. "
                    "Will dump in-memory ledger and force a reload: " (:alias ledger))
          ::stale)

        (= commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger)
                    " at t value: " commit-t " however we already have this commit so not applying: "
                    current-t)
          ::current)

        (flake/t-before? commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger)
                    " at t value: " commit-t " however, latest t is more current: "
                    current-t)
          ::newer)))))

(defrecord Ledger [id address alias did state cache commit-catalog
                   index-catalog reasoner primary-publisher secondary-publishers indexing-opts])

(defn initial-state
  [branches current-branch]
  {:branches branches
   :branch   current-branch
   :graphs   {}})

(defn instantiate
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [alias ledger-address commit-catalog index-catalog primary-publisher secondary-publishers
   indexing-opts did latest-commit]
  (let [alias*     (util.ledger/ensure-ledger-branch alias)
        branch     (util.ledger/ledger-branch alias*)
        publishers (cons primary-publisher secondary-publishers)
        branches   {branch (branch/state-map alias* branch commit-catalog index-catalog
                                             publishers latest-commit indexing-opts)}]
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                alias*  ;; Full alias including branch
                  :address              ledger-address
                  :commit-catalog       commit-catalog
                  :index-catalog        index-catalog
                  :primary-publisher    primary-publisher
                  :secondary-publishers secondary-publishers
                  :cache                (atom {})
                  :reasoner             #{}
                  :indexing-opts        indexing-opts})))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [{:keys [alias primary-address publish-addresses commit-catalog index-catalog
           primary-publisher secondary-publishers]}
   {:keys [did indexing] :as _opts}]
  (go-try
    (let [;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (util/current-time-iso)
          genesis-commit (<? (commit-storage/write-genesis-commit
                              commit-catalog alias publish-addresses init-time))
          ;; Publish genesis commit to nameservice - convert expanded to compact format first
          _              (when primary-publisher
                           (let [;; Convert expanded genesis commit to compact JSON-ld format
                                 commit-map (commit-data/json-ld->map genesis-commit nil)
                                 compact-commit (commit-data/->json-ld commit-map)]
                             (<? (nameservice/publish primary-publisher compact-commit))))]
      (instantiate alias primary-address commit-catalog index-catalog
                   primary-publisher secondary-publishers indexing did genesis-commit))))

(defn trigger-index!
  "Manually triggers indexing for a ledger on the specified branch.
   Uses the current db for that branch. Returns a channel that will receive
   the result when indexing completes.

   Options:
   - branch: Branch name (defaults to main branch if not specified)"
  ([ledger]
   (trigger-index! ledger nil))
  ([ledger branch]
   (let [branch-meta (get-branch-meta ledger branch)]
     (branch/trigger-index! branch-meta))))

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn parse-keypair
  [ledger {:keys [did private] :as opts}]
  (let [private* (or private
                     (:private did)
                     (-> ledger :did :private))
        did*     (or (some-> private* did/private->did)
                     did
                     (-> ledger :did :id))]
    (assoc opts :did did*, :private private*)))

(defn parse-data-helpers
  [{:keys [context] :as opts}]
  (let [ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context ctx-used-atom)]
    (assoc opts
           :commit-data-opts {:compact-fn    compact-fn
                              :compact       (fn [iri] (json-ld/compact iri compact-fn))
                              :id-key        (json-ld/compact const/iri-id compact-fn)
                              :type-key      (json-ld/compact const/iri-type compact-fn)
                              :ctx-used-atom ctx-used-atom})))

(defn parse-commit-opts
  [ledger opts]
  (-> opts
      (update :context parse-commit-context)
      (->> (parse-keypair ledger))
      parse-data-helpers))

(defn save-txn!
  ([{:keys [commit-catalog alias] :as _ledger} txn]
   (let [ledger-name (util.ledger/ledger-base-name alias)]
     (save-txn! commit-catalog ledger-name txn)))
  ([commit-catalog ledger-name txn]
   (let [path (str/join "/" [ledger-name "txn"])]
     (storage/content-write-json commit-catalog path txn))))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don't think the db should be handling any of it
(defn write-transaction!
  [ledger ledger-name staged]
  (go-try
    (let [{:keys [txn author annotation]} staged
          {:keys [commit-catalog]} ledger]
      (if txn
        (let [{txn-id :address} (<? (save-txn! commit-catalog ledger-name txn))]
          {:txn-id     txn-id
           :author     author
           :annotation annotation})
        staged))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn update-commit-id
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-hash]
  (let [commit-id (commit-data/hash->commit-id commit-hash)]
    [(assoc commit-map :id commit-id)
     (assoc commit-jsonld "id" commit-id)]))

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [commit-jsonld (-> (commit-data/->json-ld commit)
                            (dissoc "index"))
          ;; For credential/generate, we need a DID map with public key
          did-map (when (and did private)
                    (if (map? did)
                      did
                      (did/private->did-map private)))
          signed-commit (if did-map
                          (<? (credential/generate commit-jsonld private did-map))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (-> [commit commit-jsonld]
              (update-commit-id (:hash commit-res))
              (update-commit-address (:address commit-res)))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger."
  [{:keys [primary-publisher secondary-publishers] :as _ledger} commit-jsonld]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (nameservice/publish-to-all commit-jsonld secondary-publishers)
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (assoc :commit new-commit
               :staged nil
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes))))

(defn indexing-needed?
  [novelty-size min-size]
  (>= novelty-size min-size))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger db]
   (commit! ledger db {}))
  ([{ledger-alias :alias :as ledger}
    {:keys [branch t stats commit] :as staged-db}
    opts]
   (log/debug "commit!: write-transaction start" {:ledger ledger-alias})
   (go-try
     (let [{:keys [commit-catalog]} ledger
           ledger-name (util.ledger/ledger-base-name ledger-alias)

           {:keys [tag time message did private commit-data-opts index-files-ch]
            :or   {time (util/current-time-iso)}}
           (parse-commit-opts ledger opts)

           {:keys [db-jsonld staged-txn]}
           (commit-data/db->jsonld staged-db commit-data-opts)

           {:keys [txn-id author annotation]}
           (<? (write-transaction! ledger ledger-name staged-txn))

           _ (log/debug "commit!: write-jsonld(db) start" {:ledger ledger-alias})

           data-write-result (<? (commit-storage/write-jsonld commit-catalog ledger-name db-jsonld))

           _ (log/debug "commit!: write-jsonld(db) done" {:ledger ledger-alias :db-address (:address data-write-result)})
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file
           dbid              (commit-data/hash->db-id (:hash data-write-result))
           keypair           {:did did, :private private}

           new-commit (commit-data/new-db-commit-map {:old-commit commit
                                                      :issuer     did
                                                      :message    message
                                                      :tag        tag
                                                      :dbid       dbid
                                                      :t          t
                                                      :time       time
                                                      :db-address db-address
                                                      :author     author
                                                      :annotation annotation
                                                      :txn-id     txn-id
                                                      :flakes     (:flakes stats)
                                                      :size       (:size stats)})

           _ (log/debug "commit!: write-commit start" {:ledger ledger-alias})

           {:keys [commit-map commit-jsonld write-result]}
           (<? (write-commit commit-catalog ledger-name keypair new-commit))

           _ (log/debug "commit!: write-commit done" {:ledger ledger-alias :commit-address (:address write-result)})

           db  (formalize-commit staged-db commit-map)

           _ (log/debug "commit!: ledger/update-commit! start" {:ledger ledger-alias :t t})

           db* (update-commit! ledger branch db index-files-ch)]

       (log/debug "commit!: ledger/update-commit! done, publish-commit start" {:ledger ledger-alias :t t :at time})

       (<? (publish-commit ledger commit-jsonld))

       (log/debug "commit!: publish-commit done" {:ledger ledger-alias})

       (if (track/track-txn? opts)
         (let [index-t (index.meta/index-t commit-map)
               novelty-size (get-in db* [:novelty :size] 0)
               ;; Always read threshold from realized FlakeDB; db* may be AsyncDB
               reindex-min-bytes (or (:reindex-min-bytes db) 1000000)]
           (-> write-result
               (select-keys [:address :hash :size])
               (assoc :ledger-id ledger-alias
                      :t t
                      :db db*
                      :indexing-needed (indexing-needed? novelty-size reindex-min-bytes)
                      :index-t index-t
                      :indexing-enabled (indexing-enabled? ledger branch)
                      :novelty-size novelty-size)))
         db*)))))

(defn transact!
  [ledger parsed-txn]
  (go-try
    (let [{:keys [branch] :as parsed-opts,
           :or   {branch const/default-branch-name}}
          (:opts parsed-txn)

          db       (current-db ledger branch)
          staged   (<? (transact/stage-triples db parsed-txn))
          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          cmt-opts (dissoc parsed-opts :context :identity)]
      (if (track/track-txn? parsed-opts)
        (let [staged-db     (:db staged)
              commit-result (<? (commit! ledger staged-db cmt-opts))]
          (merge staged commit-result))
        (<? (commit! ledger staged cmt-opts))))))
