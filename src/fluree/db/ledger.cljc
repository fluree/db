(ns fluree.db.ledger
  (:require [fluree.db.conn.json-ld-proto :as jld-proto]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld])
  (:refer-clojure :exclude [load]))

#?(:clj (set! *warn-on-reflection* true))

;; TODO
(defn load
  "Loads ledger metadata, making it ready to retrieve databases
  and/or process new transactions."
  [conn ledger-iri]
  (let []
    {:conn conn
     :id   ledger-iri})
  )


(defn current-branch-map
  "Returns current working branch-map for ledger"
  [ledger]
  (-> ledger
      :state
      deref
      :branch))

(defn current-branch-name
  [ledger]
  (:name (current-branch-map ledger)))

;; TODO - if you branch from an uncommitted branch, and then commit, commit the current-branch too
(defn- new-branch-map
  "Returns a new branch name for specified branch name off of
  supplied current-branch."
  [current-branch branch-name]
  (let [{:keys [t commit idx dbs]
         :or   {t 0, commit 0, dbs (list)}} current-branch
        ;; is current branch uncommitted? If so, when committing new branch we must commit current-branch too
        uncommitted? (> t commit)]
    {:name      branch-name
     :t         t
     :dbs       dbs ; copy any uncommitted dbs from prior branch
     :commit    commit
     :idx       idx
     :latest-db nil
     :from      (-> current-branch
                    (select-keys [:name :t])
                    (assoc :uncommitted? uncommitted?))}))


(defn last-commit
  "Returns three-tuple of branch-name, t, and commit id of the last commit on ledger"
  [{:keys [state] :as ledger}]
  (let [{:keys [branch branches]} @state
        {:keys [name t commit]} (get branches branch)]
    [name t commit]))

(defn current-branch
  "Returns current branch name."
  [ledger]
  (-> ledger
      :state
      deref
      :branch))

(defn branch-meta
  "Returns branch map data for current branch, or specified branch"
  ([ledger] (branch-meta ledger (current-branch ledger)))
  ([ledger branch]
   (-> ledger
       :state
       deref
       :branches
       (get branch))))

;; TODO
(defn branch
  "Creates, or changes, a ledger's branch"
  [ledger branch]
  (let [{:keys [state]} ledger
        {:keys [branches branch]} @state
        [branch-t [branch-current branch-commit]] branch
        branch*     (util/str->keyword branch)
        new?        (contains? branches branch*)
        is-current? (= branch)]

    )

  )

(defn did
  "Returns current ledger did map for signing/authenticating"
  [ledger]
  (:did ledger))


(defn publish
  "Publishes commit for ledger.
  Returns async chan that will contain eventual response."
  [ledger commit]

  )



;; TODO - I think persisting a ledger prior to there being data might be desirable, not possible yet
(defn create
  "Creates a new ledger."
  [conn name opts]
  (let [{:keys [context did branch pub-fn]
         :or   {branch :main}} opts
        default-context (jld-proto/context conn)
        default-did     (jld-proto/did conn)
        method-type     (jld-proto/method conn)
        default-push    (fn [])
        ;; map of all branches and where they are branched from
        branches        {branch (new-branch-map nil branch)}]
    {:context     (if context
                    (json-ld/parse-context context)
                    default-context)
     :did         (or did default-did)
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
     :conn        conn}))


(defn update-commit
  "Updates a commit in the ledger's state with a newly committed db."
  [{:keys [ledger t branch] :as db} commit]
  (let [{:keys [state]} ledger]
    (swap! state update-in [:branches branch]
           (fn [branch-map]
             (if (<= t (:t branch-map))
               (throw (ex-info (str "Error updating commit state. Db's t value is not beyond the "
                                    "latest recorded t value. db's t: " t
                                    " latest registered t: " (:t branch-map))
                               {:status 500
                                :error :db/ledger-order}))
               (assoc branch-map :t t
                                 :commit commit
                                 :latest-db db))))))


(defn is-ledger?
  "Returns true if map is a ledger object.
  Used to differentiate in cases where ledger or DB could be used."
  [ledger-or-db]
  (and (map? ledger-or-db)
       (contains? ledger-or-db :cache)))


;; TODO - move to db space
(defn load-db
  "Loads a specific immutable database.

  A database requires a ledger, which in turn requires a connection."
  [conn db-iri]

  )
