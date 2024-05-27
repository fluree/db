(ns fluree.db.ledger.json-ld
  (:require [clojure.core.async :as async :refer [<!]]
            [fluree.db.ledger :as ledger]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.did :as did]
            [fluree.db.util.context :as context]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.reify :as jld-reify]
            [clojure.string :as str]
            [fluree.db.indexer :as indexer]
            [fluree.db.indexer.default :as idx-default]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.connection :as connection :refer [register-ledger release-ledger]]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake])
  (:refer-clojure :exclude [load]))

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

(defn current-commit
  ([ledger]
   (current-commit ledger nil))
  ([ledger branch]
   (let [branch-meta (get-branch-meta ledger branch)]
     ;; if branch is nil, will return default
     (when-not branch-meta
       (throw (ex-info (str "Invalid branch: " branch ".")
                       {:status 400 :error :db/invalid-branch})))
     (branch/current-commit branch-meta))))

(defn current-t
  ([ledger]
   (current-t ledger nil))
  ([ledger branch]
   (-> ledger
       (current-commit branch)
       commit-data/t)))

(defn commit-update
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  [{:keys [state] :as ledger} branch-name db]
  (log/debug "Attempting to update ledger:" (:alias ledger)
             "and branch:" branch-name "with new commit to t" (:t db))
  (let [branch-meta (get-branch-meta ledger branch-name)]
    (when-not branch-meta
      (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                           "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                      {:status 400 :error :db/invalid-branch})))
    (branch/update-commit! branch-meta db)
    (branch/current-db branch-meta)))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [address alias] :as ledger} requested-branch]
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
     :commit  commit}))

(defn normalize-opts
  "Normalizes commit options"
  [opts]
  (if (string? opts)
    {:message opts}
    opts))

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
                          (-> ledger :did :private))
        did*          (or (some-> private* did/private->did)
                          did
                          (:did ledger))
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
    (let [ledger-commit (current-commit ledger branch)
          new-commit    (commit-data/use-latest-index commit ledger-commit)
          _             (log/debug "do-commit+push new-commit:" new-commit)

          {:keys [commit-map write-result] :as commit-write-map}
          (<? (write-commit conn alias keypair new-commit))

          db*   (assoc db :commit commit-map)
          db**  (if (new-t? ledger-commit commit)
                  (commit-data/add-commit-flakes (:prev-commit db) db*)
                  db*)
          db*** (commit-update ledger branch (dissoc db** :txns))
          push-res      (<? (push-commit conn ledger commit-write-map))]
      {:commit-res write-result
       :push-res   push-res
       :db         db***})))

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

      (if file-data?
        {:data-file-meta   ledger-update-res
         :commit-file-meta commit-file-meta
         :db               db**}
        db**))))

(defn commit!
  [ledger db opts]
  (let [{:keys [branch] :as opts*}
        (normalize-opts opts)
        {:keys [t] :as db*} (or db (current-db ledger branch))
        committed-t         (current-t ledger branch)]
    (if (= t (flake/next-t committed-t))
      (commit ledger db* opts*)
      (throw (ex-info (str "Cannot commit db, as committed 't' value of: " committed-t
                           " is no longer consistent with staged db 't' value of: " t ".")
                      {:status 400 :error :db/invalid-commit})))))

(defn close-ledger
  "Shuts down ledger and resources."
  [{:keys [indexer cache state conn alias] :as _ledger}]
  (indexer/-close indexer)
  (reset! state {:closed? true})
  (reset! cache {})
  (release-ledger conn alias)) ;; remove ledger from conn cache

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [{:keys [conn] :as ledger} expanded-commit]
  (go-try
    (let [[commit proof] (jld-reify/verify-commit expanded-commit)

          branch     (-> expanded-commit
                         (get-first-value const/iri-branch)
                         keyword)
          commit-t   (-> expanded-commit
                         (get-first const/iri-data)
                         (get-first-value const/iri-t))
          current-db (current-db ledger branch)
          current-t  (:t current-db)]
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" current-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = current-t
      (cond

        (= commit-t (flake/next-t current-t))
        (let [updated-db  (<? (jld-reify/merge-commit conn current-db [commit proof]))]
          (commit-update ledger branch updated-db))

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

(defrecord JsonLDLedger [id address alias did indexer state cache conn reasoner]
  ledger/iCommit
  (-commit! [ledger db] (commit! ledger db nil))
  (-commit! [ledger db opts] (commit! ledger db opts))
  (-notify [ledger expanded-commit] (notify ledger expanded-commit))

  ledger/iLedger
  (-db [ledger] (current-db ledger))
  (-status [ledger] (status ledger nil))
  (-status [ledger branch] (status ledger branch))
  (-close [ledger] (close-ledger ledger)))


(defn normalize-alias
  "For a ledger alias, removes any preceding '/' or '#' if exists."
  [ledger-alias]
  (if (or (str/starts-with? ledger-alias "/")
          (str/starts-with? ledger-alias "#"))
    (subs ledger-alias 1)
    ledger-alias))

(defn write-genesis-commit
  [conn ledger-alias branch ns-addresses]
  (go-try
    (let [genesis-commit            (commit-data/blank-commit ledger-alias branch ns-addresses)
          initial-context           (get genesis-commit "@context")
          initial-db-data           (-> genesis-commit
                                        (get "data")
                                        (assoc "@context" initial-context))
          {db-address :address}     (<? (connection/-c-write conn ledger-alias initial-db-data))
          genesis-commit*           (assoc-in genesis-commit ["data" "address"] db-address)
          {commit-address :address} (<? (connection/-c-write conn ledger-alias genesis-commit*))]
      (assoc genesis-commit* "address" commit-address))))

(defn initial-state
  [branches current-branch]
  {:closed?  false
   :branches branches
   :branch   current-branch
   :graphs   {}
   :push     {:complete {:t   0
                         :dag nil}
              :pending  {:t   0
                         :dag nil}}})

(defn validate-indexer
  [indexer reindex-min-bytes reindex-max-bytes]
  (cond
    (satisfies? indexer/iIndex indexer)
    indexer

    indexer
    (throw (ex-info (str "Ledger indexer provided, but doesn't implement iIndex protocol. "
                         "Provided: " indexer)
                    {:status 400 :error :db/invalid-indexer}))

    :else
    (idx-default/create
      (util/without-nils
        {:reindex-min-bytes reindex-min-bytes
         :reindex-max-bytes reindex-max-bytes}))))

(defn parse-did
  [conn did]
  (if did
    (if (map? did)
      did
      {:id did})
    (connection/-did conn)))

(defn parse-ledger-options
  [conn {:keys [did branch indexer reindex-min-bytes reindex-max-bytes]
         :or   {branch :main}}]
  (let [did*    (parse-did conn did)
        indexer (validate-indexer indexer reindex-min-bytes reindex-max-bytes)]
    {:did     did*
     :branch  branch
     :indexer indexer}))

(defn create*
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn ledger-alias opts]
  (go-try
    (let [{:keys [did branch indexer]}
          (parse-ledger-options conn opts)

          ledger-alias*  (normalize-alias ledger-alias)
          address        (<? (nameservice/primary-address conn ledger-alias* (assoc opts :branch branch)))
          ns-addresses   (<? (nameservice/addresses conn ledger-alias* (assoc opts :branch branch)))
          genesis-commit (json-ld/expand
                           (<? (write-genesis-commit conn ledger-alias branch ns-addresses)))
          ;; map of all branches and where they are branched from
          branches       {branch (branch/state-map conn ledger-alias* branch genesis-commit)}]
      (map->JsonLDLedger
        {:id       (random-uuid)
         :did      did
         :state    (atom (initial-state branches branch))
         :alias    ledger-alias*
         :address  address
         :cache    (atom {})
         :indexer  indexer
         :reasoner #{}
         :conn     conn}))))

(defn create
  [conn ledger-alias opts]
  (go-try
    (let [[not-cached? ledger-chan] (register-ledger conn ledger-alias)] ;; holds final cached ledger in a promise-chan avoid race conditions
      (if not-cached?
        (let [ledger (<! (create* conn ledger-alias opts))]
          (when (util/exception? ledger)
            (release-ledger conn ledger-alias))
          (async/put! ledger-chan ledger)
          ledger)
        (throw (ex-info (str "Unable to create new ledger, one already exists for: " ledger-alias)
                        {:status 400
                         :error  :db/ledger-exists}))))))

(defn commit->ledger-alias
  "Returns ledger alias from commit map, if present. If not present
  then tries to resolve the ledger alias from the nameservice."
  [conn db-alias commit-map]
  (or (get-first-value commit-map const/iri-alias)
      (->> (connection/-nameservices conn)
           (some #(ns-proto/-alias % db-alias)))))

;; TODO - once we have a different delimiter than `/` for branch/t-value this can simplified
(defn address->alias
  [address]
  (when-let [path (->> address
                       (re-matches #"^fluree:[^:]+://(.*)$")
                       (second))]
    (if (str/ends-with? path "/main/head")
      (subs path 0 (- (count path) 10))
      path)))

(defn load*
  [conn ledger-chan address]
  (go-try
    (let [commit-addr  (<? (nameservice/lookup-commit conn address))
          _            (log/debug "Attempting to load from address:" address
                                  "with commit address:" commit-addr)
          _            (when-not commit-addr
                         (throw (ex-info (str "Unable to load. No record of ledger exists: " address)
                                         {:status 400 :error :db/invalid-commit-address})))
          [commit _]   (<? (jld-reify/read-commit conn commit-addr))
          _            (when-not commit
                         (throw (ex-info (str "Unable to load. Commit file for ledger: " address
                                              " at location: " commit-addr " is not found.")
                                         {:status 400 :error :db/invalid-db})))
          _            (log/debug "load commit:" commit)
          ledger-alias (commit->ledger-alias conn address commit)
          branch       (keyword (get-first-value commit const/iri-branch))

          {:keys [did branch indexer]} (parse-ledger-options conn {:branch branch})

          branches {branch (branch/state-map conn ledger-alias branch commit)}
          ledger   (map->JsonLDLedger
                     {:id       (random-uuid)
                      :did      did
                      :state    (atom (initial-state branches branch))
                      :alias    ledger-alias
                      :address  address
                      :cache    (atom {})
                      :indexer  indexer
                      :reasoner #{}
                      :conn     conn})]
      (nameservice/subscribe-ledger conn ledger-alias) ; async in background, elect to receive update notifications
      (async/put! ledger-chan ledger)
      ledger)))

(def fluree-address-prefix
  "fluree:")

(defn fluree-address?
  [x]
  (str/starts-with? x fluree-address-prefix))

(defn load-address
  [conn address]
  (let [alias (address->alias address)
        [not-cached? ledger-chan] (register-ledger conn alias)]
    (if not-cached?
      (load* conn ledger-chan address)
      ledger-chan)))

(defn load-alias
  [conn alias]
  (go-try
    (let [[not-cached? ledger-chan] (register-ledger conn alias)]
      (if not-cached?
        (let [address (<! (nameservice/primary-address conn alias nil))]
          (if (util/exception? address)
            (do (release-ledger conn alias)
                (async/put! ledger-chan
                            (ex-info (str "Load for " alias " failed due to failed address lookup.")
                                     {:status 400 :error :db/invalid-address}
                                     address)))
            (<? (load* conn ledger-chan address))))
        (<? ledger-chan)))))

(defn load
  [conn alias-or-address]
  (if (fluree-address? alias-or-address)
    (load-address conn alias-or-address)
    (load-alias conn alias-or-address)))
