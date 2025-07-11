(ns fluree.db.connection
  (:refer-clojure :exclude [replicate])
  (:require [clojure.core.async :as async :refer [<! go go-loop]]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.indexer.garbage :as garbage]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.sub :as ns-subscribe]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(comment
 ;; state machine looks like this:
  {:ledger        {"ledger-a" {;; map of branches, along with current/default branch
                               :branches {}
                               :branch   {}}}
   :subscriptions {}})

(def blank-state
  "Initial connection state"
  {:ledger        {}
   :subscriptions {}})

(defn printer-map
  "Returns map of important data for print writer"
  [conn]
  (select-keys conn [:id]))

(defrecord Connection [id state parallelism commit-catalog index-catalog primary-publisher
                       secondary-publishers remote-systems serializer cache defaults])

#?(:clj
   (defmethod print-method Connection [^Connection conn, ^Writer w]
     (.write w "#fluree/Connection ")
     (binding [*out* w]
       (pr (printer-map conn))))
   :cljs
   (extend-type Connection
     IPrintWithWriter
     (-pr-writer [conn w _opts]
       (-write w "#fluree/Connection ")
       (-write w (pr (printer-map conn))))))

(defmethod pprint/simple-dispatch Connection [^Connection conn]
  (pr conn))

(defn connection?
  [x]
  (instance? Connection x))

(defn connect
  [{:keys [parallelism commit-catalog index-catalog cache serializer
           primary-publisher secondary-publishers remote-systems defaults]
    :or   {serializer (json-serde)} :as _opts}]
  (let [id    (random-uuid)
        state (atom blank-state)]
    (->Connection id state parallelism commit-catalog index-catalog primary-publisher
                  secondary-publishers remote-systems serializer cache defaults)))

(defn register-ledger
  "Creates a promise-chan and saves it in a cache of ledgers being held
  in-memory on the conn.

  Returns a two-tuple of
  [cached? promise-chan]

  where `cached?` is true if an existing promise-chan was found, false if a new
  promise-chan was created.

  `promise-chan` is a promise channel that must have the final ledger `put!`
  into it assuming `success?` is true, otherwise it will return the existing
  found promise-chan when `success?` is false"
  [{:keys [state] :as _conn} ledger-alias]
  (let [new-p-chan (async/promise-chan)
        p-chan     (-> state
                       (swap! update-in [:ledger ledger-alias]
                              (fn [existing]
                                (or existing new-p-chan)))
                       (get-in [:ledger ledger-alias]))
        cached?    (not= p-chan new-p-chan)]
    (log/debug "Registering ledger: " ledger-alias " cached? " cached?)
    [cached? p-chan]))

(defn notify
  [{:keys [commit-catalog] :as conn} address hash]
  (go-try
    (if-let [expanded-commit (<? (commit-storage/read-commit-jsonld commit-catalog address hash))]
      (if-let [ledger-alias (get-first-value expanded-commit const/iri-alias)]
        (if-let [ledger-ch (ns-subscribe/cached-ledger conn ledger-alias)]
          (do (log/debug "Notification received for ledger" ledger-alias
                         "of new commit:" expanded-commit)
              (let [ledger        (<? ledger-ch)
                    db-address    (-> expanded-commit
                                      (get-first const/iri-data)
                                      (get-first-value const/iri-address))
                    expanded-data (<? (commit-storage/read-data-jsonld commit-catalog db-address))]
                (case (<? (ledger/notify ledger expanded-commit expanded-data))
                  (::ledger/current ::ledger/newer ::ledger/updated)
                  (do (log/debug "Ledger" ledger-alias "is up to date")
                      true)

                  ::ledger/stale
                  (do (log/debug "Dropping state for stale ledger:" ledger-alias)
                      (ns-subscribe/release-ledger conn ledger-alias)))))
          (log/debug "No cached ledger found for commit: " expanded-commit))
        (log/warn "Notify called with a data that does not have a ledger alias."
                  "Are you sure it is a commit?: " expanded-commit))
      (log/warn "No commit found for address:" address))))

(defn publishers
  [{:keys [primary-publisher secondary-publishers] :as _conn}]
  (cons primary-publisher secondary-publishers))

(defn publications
  [conn]
  (:remote-systems conn))

(defn all-nameservices
  [{:keys [remote-systems] :as conn}]
  (concat (publishers conn) remote-systems))

(def fluree-address-prefix
  "fluree:")

(defn fluree-address?
  [x]
  (str/starts-with? x fluree-address-prefix))

(defn relative-ledger-alias?
  [ledger-alias]
  (not (fluree-address? ledger-alias)))

(defn publishing-addresses
  "Retrieve address for each nameservices based on a relative ledger-alias.
  If ledger-alias is not relative, returns only the current ledger alias.

  TODO - if a single non-relative address is used, and the ledger exists,
  we should retrieve all stored ns addresses in the commit if possible and
  try to use all nameservices."
  [conn ledger-alias]
  (go-try
    (if (relative-ledger-alias? ledger-alias)
      (loop [nameservices* (publishers conn)
             addresses     []]
        (let [ns (first nameservices*)]
          (if ns
            (if-let [address (<? (nameservice/publishing-address ns ledger-alias))]
              (recur (rest nameservices*) (conj addresses address))
              (recur (rest nameservices*) addresses))
            addresses)))
      [ledger-alias])))

(defn primary-address
  "From a connection, lookup primary address from nameservice(s) for a given
  ledger alias"
  [{:keys [primary-publisher] :as _conn} ledger-alias]
  (nameservice/publishing-address primary-publisher ledger-alias))

(defn lookup-commit*
  "Returns commit address from first matching nameservice on a conn
   for a given ledger alias and branch"
  [ledger-address nameservices]
  (go-try
    (loop [nses nameservices]
      (when-let [nameservice (first nses)]
        (or (<? (nameservice/lookup nameservice ledger-address))
            (recur (rest nses)))))))

(defn lookup-commit
  [conn ledger-address]
  (lookup-commit* ledger-address (all-nameservices conn)))

(defn read-file-address
  [{:keys [commit-catalog] :as _conn} addr]
  (go-try
    (let [json-data (<? (storage/read-json commit-catalog addr))]
      (assoc json-data "address" addr))))

(defn lookup-publisher-commit
  [conn ledger-address]
  (lookup-commit* ledger-address (publishers conn)))

(defn read-publisher-commit
  [conn ledger-address]
  (go-try
    (or (<? (lookup-publisher-commit conn ledger-address))
        (throw (ex-info (str "No published commits exist for: " ledger-address)
                        {:status 404 :error, :db/commit-not-found})))))

(defn published-addresses
  [conn ledger-alias]
  (go-try
    (loop [[nsv & r] (publishers conn)
           addrs     []]
      (if nsv
        (if (<? (nameservice/published-ledger? nsv ledger-alias))
          (recur r (conj addrs (<? (nameservice/publishing-address nsv ledger-alias))))
          (recur r addrs))
        addrs))))

(defn published-ledger?
  [conn ledger-alias]
  (go-try
    (loop [[nsv & r] (publishers conn)]
      (if nsv
        (or (<? (nameservice/published-ledger? nsv ledger-alias))
            (recur r))
        false))))

(defn known-addresses
  [conn ledger-alias]
  (go-try
    (loop [[nsv & r] (publications conn)
           addrs     []]
      (if nsv
        (recur r (into addrs (<? (nameservice/known-addresses nsv ledger-alias))))
        addrs))))

(defn ledger-exists?
  "Checks nameservices on a connection and returns true if any nameservice
  already has a ledger associated with the given alias."
  [conn ledger-alias]
  (go-try
    (or (<? (published-ledger? conn ledger-alias))
        (boolean (not-empty (<? (known-addresses conn ledger-alias)))))))

(defn current-addresses
  [conn ledger-alias]
  (go-try
    (into (<? (published-addresses conn ledger-alias))
          (<? (known-addresses conn ledger-alias)))))

(defn parse-identity
  [conn identity]
  (if identity
    (if (map? identity)
      identity
      {:id identity})
    (-> conn :defaults :identity)))

(defn parse-ledger-options
  [conn {:keys [did branch indexing]
         :or   {branch commit-data/default-branch}}]
  (let [did*           (parse-identity conn did)
        ledger-default (-> conn :defaults :indexing)
        indexing*      (merge ledger-default indexing)]
    {:did      did*
     :branch   branch
     :indexing indexing*}))

(defn throw-ledger-exists
  [ledger-alias]
  (throw (ex-info (str "Unable to create new ledger, one already exists for: " ledger-alias)
                  {:status 409, :error :db/ledger-exists})))

(defn create-ledger
  [{:keys [commit-catalog index-catalog primary-publisher secondary-publishers] :as conn} ledger-alias opts]
  (go-try
    (if (<? (ledger-exists? conn ledger-alias))
      (throw-ledger-exists ledger-alias)
      (let [[cached? ledger-chan] (register-ledger conn ledger-alias)]
        (if  cached?
          (throw-ledger-exists ledger-alias)
          (let [addr          (<? (primary-address conn ledger-alias))
                publish-addrs (<? (publishing-addresses conn ledger-alias))
                ledger-opts   (parse-ledger-options conn opts)
                ledger        (<! (ledger/create {:alias                ledger-alias
                                                  :primary-address      addr
                                                  :publish-addresses    publish-addrs
                                                  :commit-catalog       commit-catalog
                                                  :index-catalog        index-catalog
                                                  :primary-publisher    primary-publisher
                                                  :secondary-publishers secondary-publishers}
                                                 ledger-opts))]
            (when (util/exception? ledger)
              (ns-subscribe/release-ledger conn ledger-alias))
            (async/put! ledger-chan ledger)
            ledger))))))

(defn commit->ledger-alias
  "Returns ledger alias from commit map, if present. If not present
  then tries to resolve the ledger alias from the nameservice."
  [conn db-alias commit-map]
  (or (get-first-value commit-map const/iri-alias)
      (->> (all-nameservices conn)
           (some (fn [ns]
                   (nameservice/alias ns db-alias))))))

(defn throw-missing-branch
  [address ledger-alias]
  (throw (ex-info (str "No committed branches exist for ledger: " ledger-alias
                       " at address: " address)
                  {:status 400, :error :db/missing-branch})))

(defn load-ledger*
  [{:keys [commit-catalog index-catalog primary-publisher secondary-publishers] :as conn}
   ledger-chan address]
  (go-try
    (if-let [commit (<? (lookup-commit conn address))]
      (do (log/debug "Attempting to load from address:" address
                     "with commit:" commit)
          (let [expanded-commit (json-ld/expand commit)
                ledger-alias    (commit->ledger-alias conn address expanded-commit)
                branch          (-> expanded-commit
                                    (get-first-value const/iri-branch)
                                    (or (throw-missing-branch address ledger-alias)))

                {:keys [did branch indexing]} (parse-ledger-options conn {:branch branch})
                ledger (ledger/instantiate ledger-alias address branch commit-catalog index-catalog
                                           primary-publisher secondary-publishers indexing did expanded-commit)]
            (ns-subscribe/subscribe-ledger conn ledger-alias)
            (async/put! ledger-chan ledger)
            ledger))
      (throw (ex-info (str "Unable to load. No record of ledger at address: " address " exists.")
                      {:status 404, :error :db/unkown-address})))))

(defn load-ledger-address
  [conn address]
  (let [alias (nameservice/address-path address)
        [cached? ledger-chan] (register-ledger conn alias)]
    (if cached?
      ledger-chan
      (load-ledger* conn ledger-chan address))))

(defn try-load-address
  [conn ledger-chan alias addr]
  (go
    (try* (<? (load-ledger* conn ledger-chan addr))
          (catch* e
            (log/debug e "Unable to load ledger alias" alias "at address:" addr)))))

(defn load-ledger-alias
  [conn alias]
  (go-try
    (let [[cached? ledger-chan] (register-ledger conn alias)]
      (if cached?
        (<? ledger-chan)
        (loop [[addr & r] (<? (current-addresses conn alias))]
          (if addr
            (or (<? (try-load-address conn ledger-chan alias addr))
                (recur r))
            (do (ns-subscribe/release-ledger conn alias)
                (let [ex (ex-info (str "Load for " alias " failed due to failed address lookup.")
                                  {:status 404, :error :db/unkown-ledger})]
                  (async/put! ledger-chan ex)
                  (throw ex)))))))))

(defn load-ledger
  [conn alias-or-address]
  (if (fluree-address? alias-or-address)
    (load-ledger-address conn alias-or-address)
    (load-ledger-alias conn alias-or-address)))

(defn drop-commit-artifacts
  [{:keys [commit-catalog] :as _conn} latest-commit]
  (let [error-ch  (async/chan)
        commit-ch (commit-storage/trace-commits commit-catalog latest-commit 0 error-ch)]
    (go-loop []
      (when-let [[commit _] (<! commit-ch)]
        (let [txn-address         (util/get-first-value commit const/iri-txn)
              commit-address      (util/get-first-value commit const/iri-address)
              data-address        (-> (util/get-first commit const/iri-data)
                                      (util/get-first-value const/iri-address))]
          (log/debug "Dropping commit" (-> (util/get-first commit const/iri-data)
                                           (util/get-first-value const/iri-fluree-t)))
          (when data-address
            (log/debug "Deleting data" data-address)
            (storage/delete commit-catalog data-address))
          (when commit-address
            (log/debug "Deleting commit" commit-address)
            (storage/delete commit-catalog commit-address))
          (when txn-address
            (log/debug "Deleting txn" txn-address)
            (storage/delete commit-catalog txn-address))
          (recur))))))

(defn drop-index-nodes
  "Build up a list of node addresses in leaf->root order, then delete them."
  [storage node-address]
  (go-try
    (loop [[address & r] [node-address]
           addresses     (list)]
      (if address
        (if-let [children (->> (:children (<? (storage/read-json storage address true)))
                               (mapv :id))]
          (recur (into r children) (conj addresses address))
          (recur r (conj addresses address)))

        (doseq [address addresses]
          (log/debug "Dropping node" address)
          (storage/delete storage address))))
    :nodes-dropped))

(defn drop-index-artifacts
  [{:keys [index-catalog] :as _conn} latest-commit]
  (go-try
    (let [storage       (:storage index-catalog)
          index-address (some-> (util/get-first latest-commit const/iri-index)
                                (util/get-first-value const/iri-address))]
      (when index-address
        (log/debug "Dropping index" index-address)
        (let [{:keys [spot opst post tspo]} (<? (storage/read-json storage index-address true))

              garbage-ch (garbage/clean-garbage* index-catalog index-address 0)
              spot-ch    (drop-index-nodes storage (:id spot))
              post-ch    (drop-index-nodes storage (:id post))
              tspo-ch    (drop-index-nodes storage (:id tspo))
              opst-ch    (drop-index-nodes storage (:id opst))]
          (<? garbage-ch)
          (<? spot-ch)
          (<? post-ch)
          (<? tspo-ch)
          (<? opst-ch)
          (<? (storage/delete storage index-address))))
      :index-dropped)))

(defn drop-ledger
  [conn alias]
  (go
    (try*
      (let [alias (if (fluree-address? alias)
                    (nameservice/address-path alias)
                    alias)]
        (loop [[publisher & r] (publishers conn)]
          (when publisher
            (let [ledger-addr   (<? (nameservice/publishing-address publisher alias))
                  latest-commit (-> (<? (nameservice/lookup publisher ledger-addr))
                                    json-ld/expand)]
              (log/warn "Dropping ledger" ledger-addr)
              (drop-index-artifacts conn latest-commit)
              (drop-commit-artifacts conn latest-commit)
              (<? (nameservice/retract publisher alias))
              (recur r))))
        (log/warn "Dropped ledger" alias)
        :dropped)
      (catch* e (log/debug e "Failed to complete ledger deletion")))))

(defn resolve-txn
  "Reads a transaction from the commit catalog by address.
   
   Used by fluree/server in consensus/events."
  [{:keys [commit-catalog] :as _conn} address]
  (storage/read-json commit-catalog address))

(defn replicate-index-node
  [conn address data]
  (let [clg (-> conn :index-catalog :storage)]
    (storage/write-catalog-bytes clg address data)))
