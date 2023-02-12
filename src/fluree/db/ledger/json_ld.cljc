(ns fluree.db.ledger.json-ld
  (:require [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.bootstrap :as bootstrap]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.json-ld.commit :as jld-commit]
            [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.reify :as jld-reify]
            [clojure.string :as str]
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log])
  (:refer-clojure :exclude [load]))

#?(:clj (set! *warn-on-reflection* true))

(defn branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state context] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state
        branch      (if requested-branch
                      (get branches requested-branch)
                      ;; default branch
                      (get branches branch))
        context-kw  (json-ld/parse-context context)
        context-str (-> context util/stringify-keys json-ld/parse-context)]
    (-> branch
        (assoc-in [:latest-db :schema :context] context-kw)
        (assoc-in [:latest-db :schema :context-str] context-str))))

;; TODO - no time travel, only latest db on a branch thus far
(defn db
  [ledger {:keys [branch]}]
  (let [branch-meta (ledger-proto/-branch ledger branch)]
    ;; if branch is nil, will return default
    (when-not branch-meta
      (throw (ex-info (str "Invalid branch: " branch ". Branch must exist before transacting.")
                      {:status 400 :error :db/invalid-branch})))
    (branch/latest-db branch-meta)))

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
  [{:keys [state] :as _ledger} branch-name db force?]
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
  [{:keys [indexer cache state] :as _ledger}]
  (idx-proto/-close indexer)
  (reset! state {:closed? true})
  (reset! cache {}))

(defrecord JsonLDLedger [id address alias context did indexer
                         state cache conn method]
  ledger-proto/iCommit
  (-commit! [ledger db] (commit! ledger db nil))
  (-commit! [ledger db opts] (commit! ledger db opts))

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
        (let [base-context {:base commit-address}
              commit-data  (-> (<? (conn-proto/-c-read conn commit-address))
                               (json-ld/expand base-context))
              [commit proof] (jld-reify/parse-commit commit-data)
              _            (when proof
                             (jld-reify/validate-commit db commit proof))
              db**         (<? (jld-reify/load-db db* commit true))]
          (recur r db**))
        db*))))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn ledger-alias opts]
  (go-try
    (let [{:keys [context-type context did branch pub-fn ipns indexer include
                  reindex-min-bytes reindex-max-bytes initial-tx]
           :or   {branch :main}} opts
          did*          (if did
                          (if (map? did)
                            did
                            {:id did})
                          (conn-proto/-did conn))
          indexer       (cond
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
          address       (<? (conn-proto/-address conn ledger-alias* (assoc opts :branch branch)))
          context*      (->> context
                             (util/normalize-context context-type)
                             (merge (conn-proto/-context conn)))
          method-type   (conn-proto/-method conn)
          ;; map of all branches and where they are branched from
          branches      {branch (branch/new-branch-map nil ledger-alias* branch)}
          ledger        (map->JsonLDLedger
                          {:id      (random-uuid)
                           :context context*
                           :did     did*
                           :state   (atom {:closed?  false
                                           :branches branches
                                           :branch   branch
                                           :graphs   {}
                                           :push     {:complete {:t   0
                                                                 :dag nil}
                                                      :pending  {:t   0
                                                                 :dag nil}}})
                           :alias   ledger-alias
                           :address address
                           :method  method-type
                           :cache   (atom {})
                           :indexer indexer
                           :conn    conn})
          blank-db      (jld-db/create ledger)
          bootstrap?    (boolean initial-tx)
          db            (if bootstrap?
                          (<? (bootstrap/bootstrap blank-db initial-tx))
                          (bootstrap/blank-db blank-db))]
      ;; place initial 'blank' DB into ledger.
      (ledger-proto/-db-update ledger db)
      (when include
        ;; includes other ledgers - experimental
        (let [db* (<? (include-dbs conn db include))]
          (ledger-proto/-db-update ledger db*)))
      ledger)))

(defn load
  [conn commit-address]
  (go-try
    (let [base-context {:base commit-address}
          last-commit  (<? (conn-proto/-lookup conn commit-address))
          _            (when-not last-commit
                         (throw (ex-info (str "Unable to load. No commit exists for: " commit-address)
                                         {:status 400 :error :db/invalid-commit-address})))
          commit-data  (<? (conn-proto/-c-read conn last-commit))
          _            (when-not commit-data
                         (throw (ex-info (str "Unable to load. No commit exists for: " last-commit)
                                         {:status 400 :error :db/invalid-db})))
          commit-data* (json-ld/expand commit-data base-context)
          [commit proof] (jld-reify/parse-commit commit-data*)
          _            (when proof
                         (jld-reify/validate-commit db commit proof))
          _            (log/debug "load commit:" commit)
          alias        (or (get-in commit [const/iri-alias :value])
                           (conn-proto/-alias conn commit-address))
          branch       (keyword (get-in commit [const/iri-branch :value]))
          default-ctx  (-> commit
                           (get const/iri-default-context)
                           :value
                           (->> (jld-reify/load-default-context conn))
                           <?)
          ledger       (<? (create conn alias {:branch  branch
                                               :id      last-commit
                                               :context default-ctx}))
          db           (ledger-proto/-db ledger)
          db*          (<? (jld-reify/load-db-idx db commit last-commit false))]
      (ledger-proto/-commit-update ledger branch db*)
      ledger)))


(defn is-ledger?
  "Returns true if map is a ledger object.
  Used to differentiate in cases where ledger or DB could be used."
  [x]
  (satisfies? ledger-proto/iLedger x))
