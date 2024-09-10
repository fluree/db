(ns fluree.db.ledger
  (:require [fluree.db.storage :as storage]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.transact :as transact]
            [fluree.db.did :as did]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.context :as context]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.constants :as const]
            [fluree.db.commit.storage :as commit-storage]
            [clojure.string :as str]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]))

#?(:clj (set! *warn-on-reflection* true))

(defn get-branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

;; TODO - no time travel, only latest db on a branch thus far
(defn current-db
  ([ledger]
   (current-db ledger nil))
  ([ledger branch]
   (let [branch-meta (get-branch-meta ledger branch)]
     ;; if branch is nil, will return default
     (when-not branch-meta
       (throw (ex-info (str "Invalid branch: " branch ".")
                       {:status 400 :error :db/invalid-branch})))
     (branch/current-db branch-meta))))

(defn update-commit!
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  ([ledger branch-name db]
   (update-commit! ledger branch-name db nil))
  ([{:keys [state] :as ledger} branch-name db index-files-ch]
   (log/debug "Attempting to update ledger:" (:alias ledger)
              "and branch:" branch-name "with new commit to t" (:t db))
   (let [branch-meta (get-branch-meta ledger branch-name)]
     (when-not branch-meta
       (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                            "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                       {:status 400 :error :db/invalid-branch})))
     (-> branch-meta
         (branch/update-commit! db index-files-ch)
         branch/current-db))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  ([ledger]
   (status ledger "main"))
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

(defn parse-commit-options
  "Parses the commit options and removes non-public opts."
  [opts]
  (if (string? opts)
    {:message opts}
    (select-keys opts [:context :did :private :message :tag :file-data? :index-files-ch])))

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
  [ledger {:keys [context did private message tag file-data? index-files-ch time] :as _opts}]
  (let [context*      (parse-commit-context context)
        private*      (or private
                          (:private did)
                          (-> ledger :did :private))
        did*          (or (some-> private* did/private->did)
                          did
                          (:did ledger))
        ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context* ctx-used-atom)]
    {:commit-opts
     {:message message
      :tag tag
      :time (or time (util/current-time-iso))
      :file-data? file-data? ;; if true, return the db as well as the written files (for consensus)
      :context context*
      :private private*
      :did did*}

     :commit-data-helpers
     {:compact-fn compact-fn
      :compact (fn [iri] (json-ld/compact iri compact-fn))
      :id-key (json-ld/compact "@id" compact-fn)
      :type-key (json-ld/compact "@type" compact-fn)
      :ctx-used-atom ctx-used-atom}

     ;; optional async chan passed in which will stream out all new index files created (for consensus)
     :index-files-ch index-files-ch}))

(defn write-transaction
  [storage ledger-alias txn]
  (let [path (str/join "/" [ledger-alias "txn"])]
    (storage/content-write-json storage path txn)))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don' think the db should be handling any of it
(defn write-transactions!
  [storage {:keys [alias] :as _ledger} staged]
  (go-try
   (loop [[next-staged & r] staged
          results []]
     (if next-staged
       (let [[txn author-did annotation] next-staged
             results* (if txn
                        (let [{txn-id :address} (<? (write-transaction storage alias txn))]
                          (conj results [txn-id author-did annotation]))
                        (conj results next-staged))]
         (recur r results*))
       results))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [[_ commit-jsonld :as commit-pair]
          (commit-data/commit->jsonld commit)

          signed-commit (if did
                          (<? (cred/generate commit-jsonld private (:id did)))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (update-commit-address commit-pair (:address commit-res))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger."
  [{:keys [primary-publisher secondary-publishers] :as _ledger}
   {:keys [commit-jsonld] :as _write-result}]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (dorun (map (fn [ns]
                    (nameservice/publish ns commit-jsonld)))
             secondary-publishers)
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (update :staged empty)
        (assoc :commit new-commit
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes prev-commit))))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger staged-db]
   (commit! ledger staged-db nil))
  ([{:keys [alias commit-store] :as ledger}
    {:keys [branch t stats commit] :as staged-db} opts]
   (go-try
     (let [{index-files-ch :index-files-ch
            commit-data-opts :commit-data-helpers
            {:keys [did message private tag file-data? time]} :commit-opts}
           (enrich-commit-opts ledger opts)

           {:keys [dbid db-jsonld staged-txns]}
           (flake-db/db->jsonld staged-db commit-data-opts)

           ;; TODO - we do not support multiple "transactions" in a single commit (although other code assumes we do which needs cleaning)
           [[txn-id author annotation] :as txns]
           (<? (write-transactions! commit-store ledger staged-txns))

           data-write-result (<? (commit-storage/write-jsonld commit-store alias db-jsonld)) ; write commit data
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file

           base-commit-map {:old-commit commit
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
                            :size       (:size stats)}
           new-commit      (commit-data/new-db-commit-map base-commit-map)
           keypair         {:did did :private private}

           {:keys [commit-map write-result] :as commit-write-map}
           (<? (write-commit commit-store alias keypair new-commit))

           db  (formalize-commit staged-db commit-map)
           db* (update-commit! ledger branch db index-files-ch)]

       (log/debug "Committing t" t "at" time)

       (<? (publish-commit ledger commit-write-map))

       (if file-data?
         {:data-file-meta   data-write-result
          :commit-file-meta write-result
          :db               db*}
         db*)))))

(defn close-ledger
  "Shuts down ledger and resources."
  [{:keys [cache state] :as _ledger}]
  (reset! state {:closed? true})
  (reset! cache {})
  #_(release-ledger conn alias)) ;; remove ledger from conn cache

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [ledger expanded-commit]
  (go-try
    (let [[commit-jsonld _proof] (commit-storage/verify-commit expanded-commit)

          branch     (-> expanded-commit
                         (get-first-value const/iri-branch)
                         keyword)
          commit-t   (-> expanded-commit
                         (get-first const/iri-data)
                         (get-first-value const/iri-fluree-t))
          current-db (current-db ledger branch)
          current-t  (:t current-db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond

        (= commit-t (flake/next-t current-t))
        (let [db-address     (-> commit-jsonld
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-address))
              commit-storage (-> ledger :conn :store)
              db-data-jsonld (<? (commit-storage/read-commit-jsonld commit-storage db-address))
              updated-db     (<? (transact/-merge-commit current-db commit-jsonld db-data-jsonld))]
          (update-commit! ledger branch updated-db))

        ;; missing some updates, dump in-memory ledger forcing a reload
        (flake/t-after? commit-t (flake/next-t current-t))
        (do
          (log/debug "Received commit update that is more than 1 ahead of current ledger state. "
                     "Will dump in-memory ledger and force a reload: " (:alias ledger))
          (close-ledger ledger)
          false)

        (= commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however we already have this commit so not applying: " current-t)
          false)

        (flake/t-before? commit-t current-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however, latest t is more current: " current-t)
          false)))))

(defrecord Ledger [id address alias did state cache primary-publisher
                   secondary-publishers commit-storage index-storage reasoner])

(defn initial-state
  [branches current-branch]
  {:closed?  false
   :branches branches
   :branch   current-branch
   :graphs   {}})

(defn instantiate
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [ledger-alias ledger-address primary-publisher secondary-publishers branch
   commit-store index-store indexing-opts did latest-commit]
  (let [branches {branch (branch/state-map ledger-alias branch commit-store index-store
                                           latest-commit indexing-opts)}]
    (map->Ledger {:id                   (random-uuid)
                  :did                  did
                  :state                (atom (initial-state branches branch))
                  :alias                ledger-alias
                  :address              ledger-address
                  :primary-publisher    primary-publisher
                  :secondary-publishers secondary-publishers
                  :commit-store         commit-store
                  :index-store          index-store
                  :cache                (atom {})
                  :reasoner             #{}})))

(defn normalize-alias
  "For a ledger alias, removes any preceding '/' or '#' if exists."
  [ledger-alias]
  (if (or (str/starts-with? ledger-alias "/")
          (str/starts-with? ledger-alias "#"))
    (subs ledger-alias 1)
    ledger-alias))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default
  context."
  [{:keys [alias primary-address ns-addresses primary-publisher
           secondary-publishers subscribers commit-store index-store]}
   {:keys [did branch indexing] :as opts}]
  (go-try
    (let [ledger-alias*  (normalize-alias alias)
          ;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (or (:fluree.db.json-ld.migrate.sid/time opts)
                             (util/current-time-iso))
          genesis-commit (<? (commit-storage/write-genesis-commit
                               commit-store alias branch ns-addresses init-time))]
      (instantiate ledger-alias* primary-address primary-publisher secondary-publishers
                   branch commit-store index-store indexing did genesis-commit))))
