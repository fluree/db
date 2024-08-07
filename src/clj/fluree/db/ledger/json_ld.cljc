(ns fluree.db.ledger.json-ld
  (:require [clojure.core.async :as async :refer [<!]]
            [fluree.db.ledger :as ledger]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.credential :as cred]
            [fluree.db.transact :as transact]
            [fluree.db.did :as did]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.context :as context]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.reify :as jld-reify]
            [clojure.string :as str]
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

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don' think the db should be handling any of it
(defn write-transactions!
  [conn {:keys [alias] :as _ledger} staged]
  (go-try
   (loop [[next-staged & r] staged
          results []]
     (if next-staged
       (let [[txn author-did annotation] next-staged
             results* (if txn
                        (let [{txn-id :address} (<? (connection/-txn-write conn alias txn))]
                          (conj results [txn-id author-did annotation]))
                        (conj results next-staged))]
         (recur r results*))
       results))))

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
  [conn {:keys [commit-map commit-jsonld]}]
  (let [commit-jsonld* (assoc commit-jsonld "address" (:address commit-map))]
    (nameservice/push! conn commit-jsonld*)))

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
  [{:keys [alias conn] :as ledger} {:keys [branch t stats commit] :as staged-db} opts]
  (go-try
    (let [{index-files-ch :index-files-ch
           commit-data-opts :commit-data-helpers
           {:keys [did message private tag file-data? time]} :commit-opts}
          (enrich-commit-opts ledger opts)

          {:keys [dbid db-jsonld staged-txns]}
          (flake-db/db->jsonld staged-db commit-data-opts)

          ;; TODO - we do not support multiple "transactions" in a single commit (although other code assumes we do which needs cleaning)
          [[txn-id author annotation] :as txns]
          (<? (write-transactions! conn ledger staged-txns))

          data-write-result (<? (connection/-c-write conn alias db-jsonld)) ; write commit data
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
          (<? (write-commit conn alias keypair new-commit))

          db  (formalize-commit staged-db commit-map)
          db* (update-commit! ledger branch db index-files-ch)]

      (log/debug "Committing t" t "at" time)

      (<? (push-commit conn commit-write-map))

      (if file-data?
        {:data-file-meta   data-write-result
         :commit-file-meta write-result
         :db               db*}
        db*))))

(defn close-ledger
  "Shuts down ledger and resources."
  [{:keys [cache state conn alias] :as _ledger}]
  (reset! state {:closed? true})
  (reset! cache {})
  (release-ledger conn alias)) ;; remove ledger from conn cache

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [ledger expanded-commit]
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
        (let [updated-db  (<? (transact/-merge-commit current-db commit proof))]
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

(defrecord JsonLDLedger [id address alias did state cache conn reasoner]
  ledger/iCommit
  (-commit! [ledger db] (commit! ledger db nil))
  (-commit! [ledger db opts] (commit! ledger db (parse-commit-options opts)))
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
  [conn ledger-alias branch ns-addresses init-time]
  (go-try
    (let [genesis-commit            (commit-data/blank-commit ledger-alias branch ns-addresses init-time)
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
   :graphs   {}})

(defn parse-did
  [conn did]
  (if did
    (if (map? did)
      did
      {:id did})
    (connection/-did conn)))

(defn parse-ledger-options
  [conn {:keys [did branch indexing]
         :or   {branch :main}}]
  (let [did*           (parse-did conn did)
        ledger-default (-> conn :ledger-defaults :indexing)
        indexing*      (merge ledger-default indexing)]
    {:did      did*
     :branch   branch
     :indexing indexing*}))

(defn create*
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn ledger-alias {:keys [did branch indexing] :as opts}]
  (go-try
    (let [ledger-alias*  (normalize-alias ledger-alias)
          address        (<? (nameservice/primary-address conn ledger-alias* (assoc opts :branch branch)))
          ns-addresses   (<? (nameservice/addresses conn ledger-alias* (assoc opts :branch branch)))
          ;; internal-only opt used for migrating ledgers without genesis commits
          init-time      (or (:fluree.db.json-ld.migrate.sid/time opts)
                             (util/current-time-iso))
          genesis-commit (json-ld/expand
                           (<? (write-genesis-commit conn ledger-alias branch ns-addresses init-time)))
          ;; map of all branches and where they are branched from
          branches       {branch (branch/state-map conn ledger-alias* branch genesis-commit indexing)}]
      (map->JsonLDLedger
        {:id       (random-uuid)
         :did      did
         :state    (atom (initial-state branches branch))
         :alias    ledger-alias*
         :address  address
         :cache    (atom {})
         :reasoner #{}
         :conn     conn}))))

(defn create
  [conn ledger-alias opts]
  (go-try
    (let [[not-cached? ledger-chan] (register-ledger conn ledger-alias)] ;; holds final cached ledger in a promise-chan avoid race conditions
      (if not-cached?
        (let [ledger (<! (create* conn ledger-alias (parse-ledger-options conn opts)))]
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

          {:keys [did branch]} (parse-ledger-options conn {:branch branch})

          branches {branch (branch/state-map conn ledger-alias branch commit)}
          ledger   (map->JsonLDLedger
                     {:id       (random-uuid)
                      :did      did
                      :state    (atom (initial-state branches branch))
                      :alias    ledger-alias
                      :address  address
                      :cache    (atom {})
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
