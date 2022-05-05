(ns fluree.db.ledger.json-ld
  (:require [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.json-ld.bootstrap :as bootstrap]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.commit :as commit]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.commit :as jld-commit]))

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
  [ledger {:keys [branch]}]
  (let [branch-meta (ledger-proto/-branch ledger branch)]
    ;; if branch is nil, will return default
    (when-not branch-meta
      (throw (ex-info (str "Invalid branch: " branch ". Branch must exist before transacting.")
                      {:status 400 :error :db/invalid-branch})))
    (branch/latest-db branch-meta)))


(defn db-update
  "Updates db, will throw if not next 't' from current db."
  [{:keys [state] :as _ledger} {:keys [branch] :as db}]
  (let [branch-name (branch/name branch)]
    (swap! state update-in [:branches branch-name] branch/update-db db)))


(defn commit-update
  [{:keys [state] :as _ledger} branch {:keys [t] :as commit-meta}]
  (let []
    (swap! state update-in [:branches branch]
           (fn [branch-map]
             (if (<= t (:t branch-map))
               (throw (ex-info (str "Error updating commit state. Db's t value is not beyond the "
                                    "latest recorded t value. db's t: " t
                                    " latest registered t: " (:t branch-map))
                               {:status 500
                                :error  :db/ledger-order}))
               (assoc branch-map :commit commit-meta))))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      (get branches branch))))

(defn commit!
  [ledger db opts]
  (let [opts* (commit/normalize-opts opts)]
    (jld-commit/commit ledger db opts*)))


(defrecord JsonLDLedger [name context did
                         state cache conn
                         method reindex-min reindex-max]
  commit/iCommit
  (-commit! [ledger db] (commit! ledger db nil))
  (-commit! [ledger db opts] (commit! ledger db opts))

  ledger-proto/iLedger
  (-db [ledger] (db ledger nil))
  (-db [ledger opts] (db ledger opts))
  (-db-update [ledger db] (db-update ledger db))
  (-branch [ledger] (branch-meta ledger nil))
  (-branch [ledger branch] (branch-meta ledger branch))
  (-commit-update [ledger branch commit-meta] (commit-update ledger branch commit-meta))
  (-status [ledger] (status ledger nil))
  (-status [ledger branch] (status ledger branch))
  (-did [_] did))


(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn name opts]
  (go-try
    (let [{:keys [context did branch pub-fn]
           :or   {branch :main}} opts
          did*         (if did
                         (if (map? did)
                           did
                           {:id did})
                         (conn-proto/-did conn))
          context*     (or context (conn-proto/-context conn))
          method-type  (conn-proto/-method conn)
          default-push (fn [])
          ;; map of all branches and where they are branched from
          branches     {branch (branch/new-branch-map nil branch)}
          ledger       (map->JsonLDLedger
                         {:context     context*
                          :did         did*
                          :state       (atom {:branches branches
                                              :branch   branch
                                              :pub-fn   nil
                                              ;; pub-locs is map of locations to state-map (like latest committed 't' val)
                                              :pub-locs {}})
                          :name        name
                          :method      method-type
                          :cache       (atom {})
                          :reindex-min 100000
                          :reindex-max 1000000
                          :conn        conn})
          blank-db     (jld-db/create ledger)
          db           (if (or context* did*)
                         (->> (bootstrap/bootstrap-tx context* (:id did*))
                              (db-proto/-stage blank-db)
                              <?)
                         blank-db)]
      ;; place initial 'blank' DB into ledger.
      (ledger-proto/-db-update ledger db)
      ledger)))


(defn is-ledger?
  "Returns true if map is a ledger object.
  Used to differentiate in cases where ledger or DB could be used."
  [x]
  (satisfies? ledger-proto/iLedger x))

