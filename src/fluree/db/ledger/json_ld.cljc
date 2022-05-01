(ns fluree.db.ledger.json-ld
  (:require [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.conn.json-ld-proto :as jld-proto]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.json-ld.bootstrap :as bootstrap]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.json-ld-db :as jld-db]
            [fluree.db.util.log :as log]))

(defn branch-meta
  "Retrieves branch metadata from ledger state"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      ;; default branch
      (get branches branch))))

(defn db-latest
  [ledger branch]
  (let [branch-meta (ledger-proto/-branch ledger branch)]
    ;; if branch is nil, will return default
    (when-not branch-meta
      (throw (ex-info (str "Invalid branch: " branch ". Branch must exist before transacting.")
                      {:status 400 :error :db/invalid-branch})))
    (branch/latest-db branch-meta)))

(defn db-update
  "Updates db, will throw if not next 't' from current db."
  [{:keys [state] :as _ledger} {:keys [branch] :as db}]
  (swap! state update-in [:branches branch] branch/update-db db))

(defn stage
  [ledger json-ld {:keys [branch] :as opts}]
  (go-try
    (let [latest-db (db-latest ledger branch)
          opts      nil
          new-db    (<? (jld-transact/stage latest-db json-ld opts))]
      (db-update ledger new-db))))


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

(defn commit
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [state] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state]
    (if requested-branch
      (get branches requested-branch)
      (get branches branch))))


(defrecord JsonLDLedger [name context did
                         state cache conn
                         method reindex-min reindex-max]
  ledger-proto/iLedger
  (-stage [ledger json-ld opts] (stage ledger json-ld opts))
  (-db-latest [ledger] (db-latest ledger nil))
  (-db-latest [ledger branch] (db-latest ledger branch))
  (-db-update [ledger db] (db-update ledger db))
  (-branch [ledger] (branch-meta ledger nil))
  (-branch [ledger branch] (branch-meta ledger branch))
  (-commit! [ledger] :TODO)
  (-commit! [ledger branch] :TODO)
  (-commit! [ledger branch t] :TODO)
  (-commit-update [ledger branch commit-meta] (commit-update ledger branch commit-meta))
  (-commit [ledger] (-> ledger (branch-meta nil) branch/latest-commit))
  (-commit [ledger branch] (-> ledger (branch-meta branch) branch/latest-commit))
  (-did [ledger] did))


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
                         (jld-proto/did conn))
          context*     (or context (jld-proto/context conn))
          method-type  (jld-proto/method conn)
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
          blank-db     (jld-db/create ledger)]
      ;; place initial 'blank' DB into ledger.
      (ledger-proto/-db-update ledger blank-db)
      (when (or context* did*)
        (let [bootstrap-tx (bootstrap/bootstrap-tx context* (:id did*))
              new-db       (<? (ledger-proto/-stage ledger bootstrap-tx nil))]))
      ledger)))


(defn is-ledger?
  "Returns true if map is a ledger object.
  Used to differentiate in cases where ledger or DB could be used."
  [x]
  (satisfies? ledger-proto/iLedger x))

