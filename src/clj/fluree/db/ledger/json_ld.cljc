(ns fluree.db.ledger.json-ld
  (:require [clojure.core.async :as async :refer [<!]]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.json-ld.commit :as jld-commit]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.reify :as jld-reify]
            [clojure.string :as str]
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.nameservice.core :as nameservice]
            [fluree.db.conn.core :refer [register-ledger release-ledger]]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.index :as index]
            [fluree.db.util.log :as log])
  (:refer-clojure :exclude [load]))

#?(:clj (set! *warn-on-reflection* true))

(defn branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

;; TODO - no time travel, only latest db on a branch thus far
(defn db
  [ledger {:keys [branch context-type]}]
  (let [branch-meta (ledger-proto/-branch ledger branch)]
    ;; if branch is nil, will return default
    (when-not branch-meta
      (throw (ex-info (str "Invalid branch: " branch ".")
                      {:status 400 :error :db/invalid-branch})))
    (cond-> (branch/latest-db branch-meta)
            context-type (assoc :context-type context-type))))

(defn db-update
  "Updates db, will throw if not next 't' from current db.
  Returns original db, or if index has since been updated then
  updated db with new index point."
  [{:keys [state] :as _ledger} {:keys [branch] :as db}]
  (-> (swap! state update-in [:branches branch] branch/update-db db)
      (get-in [:branches branch :latest-db])))

(defn commit-update
  "Updates both latest db and commit db. If latest registered index is
  newer than provided db, updates index before storing.

  If index in provided db is newer, updates latest index held in ledger state."
  [{:keys [state] :as ledger} branch-name db force?]
  (log/debug "Attempting to update ledger's db with new commit:"
             (:alias ledger) "branch:" branch-name)
  (when-not (get-in @state [:branches branch-name])
    (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                         "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                    {:status 400 :error :db/invalid-branch})))
  (-> (swap! state update-in [:branches branch-name] branch/update-commit db force?)
      (get-in [:branches branch-name :commit-db])))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [state address alias] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state
        branch-data (if requested-branch
                      (get branches requested-branch)
                      (get branches branch))
        {:keys [latest-db commit]} branch-data
        {:keys [stats t]} latest-db
        {:keys [size flakes]} stats]
    {:address address
     :alias   alias
     :branch  branch
     :t       (when t (- t))
     :size    size
     :flakes  flakes
     :commit  commit}))

(defn normalize-opts
  "Normalizes commit options"
  [opts]
  (if (string? opts)
    {:message opts}
    opts))

(defn commit!
  [ledger db opts]
  (let [opts* (normalize-opts opts)
        db*   (or db (ledger-proto/-db ledger (:branch opts*)))]
    (jld-commit/commit ledger db* opts*)))


(defn close-ledger
  "Shuts down ledger and resources."
  [{:keys [indexer cache state conn alias] :as _ledger}]
  (idx-proto/-close indexer)
  (reset! state {:closed? true})
  (reset! cache {})
  (release-ledger conn alias)) ;; remove ledger from conn cache

;; TODO - finalize in-memory db update along with logic to ensure consistent state
(defn update-local-db
  "Returns true if update was successful, else false or exception
  if unexpected exception occurs."
  [ledger updated-db]

  true)

(defn notify
  "Returns false if provided commit update did not result in an update to the ledger because
  the provided commit was not the next expected commit.

  If commit successful, returns successfully updated db."
  [{:keys [conn] :as ledger} expanded-commit]
  (go-try
    (let [[commit proof] (jld-reify/parse-commit expanded-commit)
          branch    (keyword (get-first-value expanded-commit const/iri-branch))
          commit-t  (-> expanded-commit
                        (get-first const/iri-data)
                        (get-first-value const/iri-t))
          latest-db (ledger-proto/-db ledger {:branch branch})
          latest-t  (- (:t latest-db))]
      (log/debug "notify of new commit for ledger:" (:alias ledger) "at t value:" commit-t
                 "where current cached db t value is:" latest-t)
      ;; note, index updates will have same t value as current one, so still need to check if t = latest-t
      (cond

        (= commit-t (inc latest-t))
        (let [updated-db  (<? (jld-reify/merge-commit conn latest-db false [commit proof]))
              commit-map  (commit-data/json-ld->map commit (select-keys updated-db index/types))
              updated-db* (assoc updated-db :commit commit-map)]
          (commit-update ledger branch updated-db* false))

        ;; missing some updates, dump in-memory ledger forcing a reload
        (> commit-t (inc latest-t))
        (do
          (log/debug "Received commit update that is more than 1 ahead of current ledger state. "
                     "Will dump in-memory ledger and force a reload: " (:alias ledger))
          (close-ledger ledger)
          false)

        (= commit-t latest-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however we already have this commit so not applying: " latest-t)
          false)

        (< commit-t latest-t)
        (do
          (log/info "Received commit update for ledger: " (:alias ledger) " at t value: " commit-t
                    " however, latest-t is more current: " latest-t)
          false)))))

(defrecord JsonLDLedger [id address alias did indexer state cache conn method]
  ledger-proto/iCommit
  (-commit! [ledger db] (commit! ledger db nil))
  (-commit! [ledger db opts] (commit! ledger db opts))
  (-notify [ledger expanded-commit] (notify ledger expanded-commit))

  ledger-proto/iLedger
  (-db [ledger] (db ledger nil))
  (-db [ledger opts] (db ledger opts))
  (-db-update [ledger db] (db-update ledger db))
  (-branch [ledger] (branch-meta ledger nil))
  (-branch [ledger branch] (branch-meta ledger branch))
  (-commit-update [ledger branch db] (commit-update ledger branch db false))
  (-commit-update [ledger branch db force?] (commit-update ledger branch db force?))
  (-status [ledger] (status ledger nil))
  (-status [ledger branch] (status ledger branch))
  (-did [_] did)
  (-alias [_] alias)
  (-address [_] address)
  (-close [ledger] (close-ledger ledger)))


(defn normalize-alias
  "For a ledger alias, removes any preceding '/' or '#' if exists."
  [ledger-alias]
  (if (or (str/starts-with? ledger-alias "/")
          (str/starts-with? ledger-alias "#"))
    (subs ledger-alias 1)
    ledger-alias))

(defn include-dbs
  [conn db include]
  (go-try
    (loop [[commit-address & r] include
           db* db]
      (if commit-address
        (let [commit-tuple (jld-reify/read-commit conn commit-address)
              db**         (<? (jld-reify/load-db db* commit-tuple true))]
          (recur r db**))
        db*))))


(defn create*
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn ledger-alias opts]
  (go-try
    (let [{:keys [did branch indexer include reindex-min-bytes
                  reindex-max-bytes]
           :or   {branch :main}}
          opts

          did*    (if did
                    (if (map? did)
                      did
                      {:id did})
                    (conn-proto/-did conn))
          indexer (cond
                    (satisfies? idx-proto/iIndex indexer)
                    indexer

                    indexer
                    (throw (ex-info (str "Ledger indexer provided, but doesn't implement iIndex protocol. "
                                         "Provided: " indexer)
                                    {:status 400 :error :db/invalid-indexer}))

                    :else
                    (conn-proto/-new-indexer
                      conn (util/without-nils
                             {:reindex-min-bytes reindex-min-bytes
                              :reindex-max-bytes reindex-max-bytes})))
          ledger-alias* (normalize-alias ledger-alias)
          address       (<? (nameservice/primary-address conn ledger-alias* (assoc opts :branch branch)))
          ns-addresses  (<? (nameservice/addresses conn ledger-alias* (assoc opts :branch branch)))
          method-type   (conn-proto/-method conn)
          ;; map of all branches and where they are branched from
          branches      {branch (branch/new-branch-map nil ledger-alias* branch ns-addresses)}
          ledger        (map->JsonLDLedger
                          {:id      (random-uuid)
                           :did     did*
                           :state   (atom {:closed?  false
                                           :branches branches
                                           :branch   branch
                                           :graphs   {}
                                           :push     {:complete {:t   0
                                                                 :dag nil}
                                                      :pending  {:t   0
                                                                 :dag nil}}})
                           :alias   ledger-alias*
                           :address address
                           :method  method-type
                           :cache   (atom {})
                           :indexer indexer
                           :conn    conn})
          db            (jld-db/create ledger)]
      ;; place initial 'blank' DB into ledger.
      (ledger-proto/-db-update ledger db)
      (when include
        ;; includes other ledgers - experimental
        (let [db* (<? (include-dbs conn db include))]
          (ledger-proto/-db-update ledger db*)))
      ledger)))

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
      (->> (conn-proto/-nameservices conn)
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
    (let [commit-addr  (<? (nameservice/lookup-commit conn address nil))
          _            (when-not commit-addr
                         (throw (ex-info (str "Unable to load. No commit exists for: " address)
                                         {:status 400 :error :db/invalid-commit-address})))
          [commit _]   (<? (jld-reify/read-commit conn commit-addr))
          _            (when-not commit
                         (throw (ex-info (str "Unable to load. No commit exists for: " commit-addr)
                                         {:status 400 :error :db/invalid-db})))
          _            (log/debug "load commit:" commit)
          ledger-alias (commit->ledger-alias conn address commit)
          branch       (keyword (get-first-value commit const/iri-branch))
          ledger       (<? (create* conn ledger-alias {:branch         branch
                                                       :id             commit-addr}))
          db           (ledger-proto/-db ledger)
          db*          (<? (jld-reify/load-db-idx db commit commit-addr false))]
      (ledger-proto/-commit-update ledger branch db*)
      (nameservice/subscribe-ledger conn ledger-alias) ; async in background, elect to receive update notifications
      (async/put! ledger-chan ledger) ; note, ledger can be an exception!
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
