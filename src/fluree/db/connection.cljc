(ns fluree.db.connection
  (:refer-clojure :exclude [replicate])
  (:require [clojure.core.async :as async :refer [<! go go-loop]]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.indexer.garbage :as garbage]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(declare notify* plan-ns-update commit->ns-info)

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

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias)
  nil)

(defn all-publications
  [{:keys [remote-systems] :as _conn}]
  remote-systems)

(defn subscribe-all
  [publications ledger-alias]
  (->> publications
       (map (fn [pub]
              (nameservice/subscribe pub ledger-alias)))
       async/merge))

(defn subscribed?
  [current-state ledger-alias]
  (contains? (:subscriptions current-state) ledger-alias))

(defn get-subscription
  [current-state ledger-alias]
  (get-in current-state [:subscriptions ledger-alias]))

(defn add-subscription
  [current-state publications ledger-alias]
  (if-not (subscribed? current-state ledger-alias)
    (let [sub-ch (subscribe-all publications ledger-alias)]
      (assoc-in current-state [:subscriptions ledger-alias] sub-ch))
    current-state))

(defn remove-subscription
  [current-state ledger-alias]
  (update current-state :subscriptions dissoc ledger-alias))

(defn notify
  "Notifies the connection of an update to keep cached db state current.

  Parameters (2-arity overload):
    - conn: Connection
    - update: either
      - a nameservice record map (possibly compacted), or
      - a commit address string (content-addressed)

  Behavior:
    - If `update` is a map, it is treated as a nameservice record and the
      minimal action is applied (index/commit/none), based on plan-ns-update.
    - If `update` is a string, it is treated as a commit address; the commit is
      read and applied to the cached ledger if newer."
  [conn update]
  (go-try
    (if (map? update)
      (let [ns-info (ledger/expand-and-extract-ns update)]
        (<? (notify* conn ns-info nil)))
      (let [{:keys [commit-catalog]} conn
            address update]
        (if-let [expanded-commit (<? (commit-storage/read-commit-jsonld commit-catalog address))]
          (let [ns-info (commit->ns-info expanded-commit)]
            (log/debug "Notification received for ledger" (:ledger-alias ns-info)
                       "of new commit:" expanded-commit)
            (<? (notify* conn ns-info expanded-commit)))
          (log/warn "No commit found for address:" address))))))

(defn plan-ns-update
  "Decides minimal action based on cached db and nameservice info.

  Returns one of:
    :noop   - nothing to do
    :index  - apply index-only update
    :commit - load and apply next commit
    :stale  - cached state behind; drop for reload

  Assumes branch already resolved in ns-info."
  [db {:keys [ns-t index-address commit-address] :as _ns-info}]
  (let [cur-t   (:t db)
        cur-idx (get-in db [:commit :index :address])
        action  (cond
                  (and (= ns-t cur-t)
                       (or (nil? index-address)
                           (= index-address cur-idx)))
                  :noop

                  (= ns-t cur-t)
                  :index

                  (= ns-t (flake/next-t cur-t))
                  (if commit-address
                    :commit
                    :noop)

                  (flake/t-after? ns-t (flake/next-t cur-t))
                  :stale

                  :else
                  :noop)]
    (log/debug "plan-ns-update" {:cur-t cur-t
                                 :cur-idx cur-idx
                                 :ns-t ns-t
                                 :index-address index-address
                                 :commit-address commit-address
                                 :action action})
    action))

(defn- commit->ns-info
  "Builds an ns-info-like map from an expanded commit.
   Used to unify logic between nameservice record and direct commit notifications."
  [expanded-commit]
  {:ledger-alias   (get-first-value expanded-commit const/iri-alias)
   :branch         (get-first-value expanded-commit const/iri-branch)
   :ns-t           (-> expanded-commit
                       (get-first const/iri-data)
                       (get-first-value const/iri-fluree-t))
   :commit-address (get-first-value expanded-commit const/iri-address)
   :index-address  (-> expanded-commit
                       (get-first const/iri-index)
                       (get-first-value const/iri-id))})

(defn- notify*
  "Internal notify logic shared by both forms.
  Takes a connection, ns-info map, and optionally pre-loaded expanded-commit."
  [{:keys [commit-catalog] :as conn} ns-info expanded-commit]
  (go-try
    (let [{:keys [ledger-alias branch commit-address index-address ns-t]} ns-info]
      (log/debug "notify* received ns-info" {:ledger-alias ledger-alias
                                             :branch branch
                                             :ns-t ns-t
                                             :commit-address commit-address
                                             :index-address index-address
                                             :expanded-commit? (boolean expanded-commit)})
      (if-let [ledger-ch (and ledger-alias (cached-ledger conn ledger-alias))]
        (let [ledger  (<? ledger-ch)
              db      (ledger/current-db ledger branch)
              action  (plan-ns-update db ns-info)]
          (log/debug "notify* planned action" {:ledger-alias ledger-alias :action action})
          (case action
            :noop
            (do (log/debug "Ledger" ledger-alias "is already up to date")
                true)

            :index
            (do (log/debug "Applying index-only update" {:ledger-alias ledger-alias
                                                         :branch branch
                                                         :index-address index-address})
                (let [res (try* (<? (ledger/notify-index ledger {:index-address index-address
                                                                 :branch        branch}))
                                (catch* e
                                  (log/warn e "notify-index failed; marking stale to reload"
                                            {:ledger-alias ledger-alias :branch branch
                                             :index-address index-address})
                                  ::ledger/stale))]
                  (log/debug "notify-index result" {:ledger-alias ledger-alias :result res})
                  (when (= res ::ledger/stale)
                    (release-ledger conn ledger-alias))
                  res))

            :commit
            (let [expanded-commit (or expanded-commit
                                      (<? (commit-storage/load-commit-with-metadata
                                           commit-catalog commit-address index-address)))
                  expanded-data   (let [db-address (-> expanded-commit
                                                       (get-first const/iri-data)
                                                       (get-first-value const/iri-address))]
                                    (<? (commit-storage/read-data-jsonld commit-catalog db-address)))]
              (log/debug "Applying commit update" {:ledger-alias ledger-alias :t ns-t})
              (let [res (try* (<? (ledger/notify ledger expanded-commit expanded-data))
                              (catch* e
                                (log/warn e "notify commit failed; marking stale to reload"
                                          {:ledger-alias ledger-alias :t ns-t})
                                ::ledger/stale))]
                (case res
                  (::ledger/current ::ledger/newer ::ledger/updated)
                  (do (log/debug "Ledger" ledger-alias "is up to date after commit path")
                      true)
                  ::ledger/stale
                  (do (log/debug "Dropping state for stale ledger:" ledger-alias)
                      (release-ledger conn ledger-alias)))))

            :stale
            (do (log/debug "Dropping state for stale ledger:" ledger-alias)
                (release-ledger conn ledger-alias))))
        (log/debug "Ledger not currently loaded:" ledger-alias "; skipping notify of changes.")))))

;; TODO; Were subscribing to every remote system for every ledger we load.
;; Perhaps we should ensure that a remote system manages a particular ledger
;; before subscribing
(defn subscribe-ledger
  "Initiates subscription requests for a ledger into all remote systems on a
  connection."
  [{:keys [state] :as conn} ledger-alias]
  (let [pubs                   (all-publications conn)
        [prev-state new-state] (swap-vals! state add-subscription pubs ledger-alias)]
    (when-not (subscribed? prev-state ledger-alias)
      (let [sub-ch (get-subscription new-state ledger-alias)]
        (go-loop []
          (when-let [msg (<! sub-ch)]
            (log/info "Subscribed ledger:" ledger-alias "received subscription message:" msg)
            (let [action (get msg "action")]
              (if (= "new-commit" action)
                (let [{:keys [address]} (get msg "data")]
                  (notify conn address))
                (log/info "New subscrition message with action: " action "received, ignored.")))
            (recur)))
        :subscribed))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [{:keys [state] :as conn} ledger-alias]
  (->> (all-publications conn)
       (map (fn [pub]
              (nameservice/unsubscribe pub ledger-alias)))
       dorun)
  (swap! state remove-subscription ledger-alias))

(defn publishers
  [{:keys [primary-publisher secondary-publishers] :as _conn}]
  (->> (concat [primary-publisher]
               (cond
                 (sequential? secondary-publishers) secondary-publishers
                 (some? secondary-publishers)      [secondary-publishers]
                 :else                              []))
       (remove nil?)))

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
  (->> ledger-alias
       util.ledger/ensure-ledger-branch
       (nameservice/publishing-address primary-publisher)))

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

(defn parse-address-hash
  [{:keys [commit-catalog] :as _conn} addr]
  (storage/get-hash commit-catalog addr))

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
    (log/debug "published-ledger? checking for:" ledger-alias)
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
  [conn {:keys [did indexing]}]
  (let [did*           (parse-identity conn did)
        ledger-default (-> conn :defaults :indexing)
        indexing*      (merge ledger-default indexing)]
    {:did      did*
     :indexing indexing*}))

(defn throw-ledger-exists
  [ledger-alias]
  (throw (ex-info (str "Unable to create new ledger, one already exists for: " ledger-alias)
                  {:status 409, :error :db/ledger-exists})))

(defn commit->ledger-alias
  "Returns ledger alias from commit map, if present. If not present
  then tries to resolve the ledger alias from the nameservice."
  [conn db-alias commit-map]
  (or (get-first-value commit-map const/iri-alias)
      (->> (all-nameservices conn)
           (some (fn [ns]
                   (nameservice/alias ns db-alias))))))

(defn create-ledger
  [{:keys [commit-catalog index-catalog primary-publisher secondary-publishers] :as conn} ledger-alias opts]
  (go-try
    (let [;; Normalize ledger-alias to include branch
          normalized-alias (util.ledger/ensure-ledger-branch ledger-alias)]
      (if (<? (ledger-exists? conn normalized-alias))
        (throw-ledger-exists normalized-alias)
        (let [[cached? ledger-chan] (register-ledger conn normalized-alias)]
          (if  cached?
            (throw-ledger-exists normalized-alias)
            (let [addr          (<? (primary-address conn normalized-alias))
                  publish-addrs (<? (publishing-addresses conn normalized-alias))
                  ledger-opts   (parse-ledger-options conn opts)
                  ledger        (<! (ledger/create {:alias                normalized-alias
                                                    :primary-address      addr
                                                    :publish-addresses    publish-addrs
                                                    :commit-catalog       commit-catalog
                                                    :index-catalog        index-catalog
                                                    :primary-publisher    primary-publisher
                                                    :secondary-publishers secondary-publishers}
                                                   ledger-opts))]
              (when (util/exception? ledger)
                (release-ledger conn normalized-alias))
              (async/put! ledger-chan ledger)
              ledger)))))))

(defn load-ledger*
  [{:keys [commit-catalog index-catalog primary-publisher secondary-publishers] :as conn}
   ledger-chan address]
  (go-try
    (if-let [ns-record (<? (lookup-commit conn address))]
      (let [{:keys [commit-address index-address]} (ledger/expand-and-extract-ns ns-record)

            ;; Load full commit from disk
            _              (log/debug "Attempting to load from address:" address)
            commit         (<? (commit-storage/load-commit-with-metadata commit-catalog
                                                                         commit-address
                                                                         index-address))
            expanded-commit (json-ld/expand commit)
            combined-alias  (commit->ledger-alias conn address expanded-commit)

            {:keys [did indexing]} (parse-ledger-options conn {})
            ledger (ledger/instantiate combined-alias address commit-catalog index-catalog
                                       primary-publisher secondary-publishers indexing did expanded-commit)]
        (subscribe-ledger conn combined-alias)
        (async/put! ledger-chan ledger)
        ledger)
      (throw (ex-info (str "Unable to load. No record of ledger at address: " address " exists.")
                      {:status 404, :error :db/unkown-address})))))

(defn load-ledger-address
  [conn address]
  (let [alias (storage/get-local-path address)
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
    (let [;; Normalize ledger-alias to include branch
          normalized-alias (util.ledger/ensure-ledger-branch alias)
          [cached? ledger-chan] (register-ledger conn normalized-alias)]
      (if cached?
        (<? ledger-chan)
        (let [addresses (<? (current-addresses conn normalized-alias))]
          (log/debug "load-ledger-alias: Looking for" normalized-alias "found addresses:" addresses)
          (loop [[addr & r] addresses]
            (if addr
              (or (<? (try-load-address conn ledger-chan normalized-alias addr))
                  (recur r))
              (do (release-ledger conn normalized-alias)
                  (let [ex (ex-info (str "Load for " normalized-alias " failed due to failed address lookup.")
                                    {:status 404, :error :db/unkown-ledger})]
                    (async/put! ledger-chan ex)
                    (throw ex))))))))))

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
        (let [{:keys [spot psot opst post tspo]} (<? (storage/read-json storage index-address true))

              garbage-ch (garbage/clean-garbage* index-catalog index-address 0)
              spot-ch    (drop-index-nodes storage (:id spot))
              psot-ch    (drop-index-nodes storage (:id psot))
              post-ch    (drop-index-nodes storage (:id post))
              tspo-ch    (drop-index-nodes storage (:id tspo))
              opst-ch    (drop-index-nodes storage (:id opst))]
          (<? garbage-ch)
          (<? spot-ch)
          (<? psot-ch)
          (<? post-ch)
          (<? tspo-ch)
          (<? opst-ch)
          (<? (storage/delete storage index-address))))
      :index-dropped)))

(defn- stop-ledger-indexing
  "Stops background indexing for a cached ledger to prevent race conditions during drop."
  [conn alias*]
  (go
    (when-let [ledger-ch (cached-ledger conn alias*)]
      (try*
        (let [ledger (<? ledger-ch)]
          (log/debug "Stopping indexing for cached ledger before drop" {:alias alias*})
          (doseq [branch-name (ledger/available-branches ledger)]
            (when-let [branch-meta (ledger/get-branch-meta ledger branch-name)]
              (when-let [idx-q (:index-queue branch-meta)]
                (async/close! idx-q)))))
        (catch* e
          (log/debug e "Failed to stop indexing for cached ledger" {:alias alias*}))))))

(defn- read-nameservice-records-for-drop
  "Reads and caches all nameservice records before deletion to ensure cleanup metadata is available."
  [publishers alias*]
  (go-try
    (loop [remaining-pubs publishers
           records []]
      (if-let [publisher (first remaining-pubs)]
        (let [record (try*
                       (let [ledger-addr (<? (nameservice/publishing-address publisher alias*))]
                         (when-let [ns-rec (<? (nameservice/lookup publisher ledger-addr))]
                           {:publisher publisher
                            :ledger-addr ledger-addr
                            :ns-record ns-rec}))
                       (catch* e
                         (log/debug e "Failed to read nameservice record during drop" {:publisher publisher :alias alias*})
                         nil))]
          (recur (rest remaining-pubs)
                 (if record (conj records record) records)))
        records))))

(defn- delete-ledger-artifacts
  "Deletes commit and index artifacts for all cached nameservice records."
  [conn ns-records alias*]
  (go-try
    (doseq [{:keys [ns-record ledger-addr]} ns-records]
      (try*
        (let [commit-address (get-in ns-record ["f:commit" "@id"])
              index-address  (get-in ns-record ["f:index" "@id"])
              latest-commit  (when commit-address
                               (<? (commit-storage/load-commit-with-metadata
                                    (:commit-catalog conn)
                                    commit-address
                                    index-address)))]
          (log/debug "Dropping ledger artifacts" {:ledger-address ledger-addr})
          (when latest-commit
            (<? (drop-index-artifacts conn latest-commit))
            (<? (drop-commit-artifacts conn latest-commit))))
        (catch* e (log/debug e "Failed to drop artifacts for publisher during drop" {:alias alias*}))))))

(defn drop-ledger
  [conn alias]
  (go
    (try*
      (let [alias* (cond-> alias
                     (fluree-address? alias) storage/get-local-path
                     true util.ledger/ensure-ledger-branch)
            pubs   (vec (publishers conn))]

        (<? (stop-ledger-indexing conn alias*))

        (let [ns-records (<? (read-nameservice-records-for-drop pubs alias*))]
          (<? (delete-ledger-artifacts conn ns-records alias*))

          (doseq [publisher pubs]
            (try*
              (<? (nameservice/retract publisher alias*))
              (catch* e (log/debug e "Failed to retract nameservice record during drop" {:publisher publisher :alias alias*}))))

          (release-ledger conn alias*))

        (log/debug "Dropped ledger" alias*)
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

(defn trigger-ledger-index
  "Manually triggers indexing for a ledger/branch and waits for completion.
   Options:
   - :timeout - Max wait time in ms (default 300000 / 5 minutes)

   Returns the indexed database object or throws an exception on failure/timeout."
  [conn ledger-alias opts]
  (go-try
    (let [{:keys [timeout]
           :or {timeout 300000}} opts
          ledger (<? (load-ledger-alias conn ledger-alias))
          complete-ch (ledger/trigger-index! ledger)
          timeout-ch (async/timeout timeout)]
      (async/alt!
        complete-ch ([result] result)
        timeout-ch (ex-info "Indexing wait timeout, but assume indexing is proceeding in the background."
                            {:status 408
                             :error :db/timeout
                             :timeout timeout})))))
