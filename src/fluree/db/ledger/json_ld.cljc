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
            [fluree.db.method.ipfs.push :as ipfs-push]
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
  "Updates both latest db and commit db."
  [{:keys [state] :as _ledger} branch-name db force?]
  (when-not (get-in @state [:branches branch-name])
    (throw (ex-info (str "Unable to update commit on branch: " branch-name " as it no longer exists in ledger. "
                         "Did it just get deleted? Branches that exist are: " (keys (:branches @state)))
                    {:status 400 :error :db/invalid-branch})))
  (swap! state update-in [:branches branch-name] branch/update-commit db force?))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [state address] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state
        branch-data (if requested-branch
                      (get branches requested-branch)
                      (get branches branch))
        {:keys [latest-db commit commit-meta]} branch-data
        {:keys [stats t]} latest-db
        {:keys [size flakes]} stats]
    {:address address
     :branch  branch
     :t       (when t (- t))
     :size    size
     :flakes  flakes
     :commit  {:t       (when commit (- commit))
               :address (:address commit-meta)
               :db      (:address (:db commit-meta))}}))

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

(defn push!
  [ledger commit-meta]
  (ipfs-push/push! ledger commit-meta))


(defrecord JsonLDLedger [address alias context did
                         state cache conn
                         method reindex-min reindex-max]
  ledger-proto/iCommit
  (-commit! [ledger] (commit! ledger nil nil))
  (-commit! [ledger db-or-opts] (if (jld-db/json-ld-db? db-or-opts)
                                  (commit! ledger db-or-opts nil)
                                  (commit! ledger nil db-or-opts)))
  (-commit! [ledger db opts] (commit! ledger db opts))
  (-push! [ledger commit-meta] (push! ledger commit-meta))

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
  (-address [_] address))

(defn normalize-address
  "Creates a full IRI from a base-address and ledger alias.
  Assumes ledger-alias is already normalized via 'normalize-alias'"
  [base-address ledger-alias]
  (let [base-address* (if (str/ends-with? base-address "/")
                        base-address
                        (str base-address "/"))]
    (str "fluree:ipns://" base-address* ledger-alias)))

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
              db**         (<? (jld-reify/load-db db* commit-data true))]
          (recur r db**))
        db*))))

(defn create
  "Creates a new ledger, optionally bootstraps it as permissioned or with default context."
  [conn ledger-alias opts]
  (go-try
    (let [{:keys [context did branch pub-fn blank? ipns include]
           :or   {branch :main}} opts
          did*          (if did
                          (if (map? did)
                            did
                            {:id did})
                          (conn-proto/-did conn))
          ledger-alias* (normalize-alias ledger-alias)
          base-address  (if-let [ipns-key (:key ipns)]
                          (<? (conn-proto/-address conn ipns-key))
                          (<? (conn-proto/-address conn)))
          address       (normalize-address base-address ledger-alias*)
          context*      (or context (conn-proto/-context conn))
          method-type   (conn-proto/-method conn)
          ;; map of all branches and where they are branched from
          branches      {branch (branch/new-branch-map nil branch)}
          ledger        (map->JsonLDLedger
                          {:context     context*
                           :did         did*
                           :state       (atom {:branches branches
                                               :branch   branch
                                               :graphs   {}
                                               :push     {:complete {:t   0
                                                                     :dag nil}
                                                          :pending  {:t   0
                                                                     :dag nil}}})
                           :alias       ledger-alias
                           :address     address
                           :method      method-type
                           :cache       (atom {})
                           :reindex-min 100000
                           :reindex-max 1000000
                           :conn        conn})
          blank-db      (jld-db/create ledger)
          bootstrap?    (and (not blank?)
                             (or context* did*))
          db            (if bootstrap?
                          (<? (bootstrap/bootstrap blank-db context* (:id did*)))
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
          commit-data  (-> (<? (conn-proto/-c-read conn commit-address))
                           (json-ld/expand base-context))
          [commit proof] (jld-reify/parse-commit commit-data)
          alias        (or (get-in commit [const/iri-alias :value])
                           commit-address)
          branch       (get-in commit [const/iri-branch :value])
          ledger       (<? (create conn alias {:branch branch
                                               :id     commit-address
                                               :blank? true}))
          db           (ledger-proto/-db ledger)
          db*          (<? (jld-reify/load-db db commit-data false))]
      (ledger-proto/-db-update ledger db*)
      ledger)))


(defn is-ledger?
  "Returns true if map is a ledger object.
  Used to differentiate in cases where ledger or DB could be used."
  [x]
  (satisfies? ledger-proto/iLedger x))

