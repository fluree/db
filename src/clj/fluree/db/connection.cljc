(ns fluree.db.connection
  (:require [clojure.core.async :as async :refer [<!]]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util.core :as util :refer [get-first-value]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.json-ld :as json-ld]
            [fluree.db.ledger :as ledger])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iConnection
  (-did [conn] "Returns optional default did map if set at connection level"))

(defprotocol iStorage
  (-c-read [conn commit-key] "Reads a commit from storage")
  (-c-write [conn ledger-alias commit-data] "Writes a commit to storage")
  (-txn-write [conn ledger-alias txn-data] "Writes a transaction to storage and returns the key. Expects string keys."))

(comment
 ;; state machine looks like this:
 {:ledger {"ledger-a" {:event-fn :main-system-event-fn ;; returns async-chan response once complete
                       :subs     {:sub-id :sub-fn} ;; active subscriptions
                       ;; map of branches, along with current/default branch
                       :branches {}
                       :branch   {}}}


  :await  {:msg-id :async-res-ch} ;; map of msg-ids to response chans for messages awaiting responses
  :stats  {}}) ;; any stats about the connection itself

(defn blank-state
  "Returns top-level state for connection"
  []
  (atom
   {:ledger {}
    :await  {}
    :stats  {}}))

(defn printer-map
  "Returns map of important data for print writer"
  [conn]
  {:id    (:id conn)
   :stats (get @(:state conn) :stats)})

(defrecord Connection [id state parallelism store index-store primary-publisher
                       secondary-publishers subscribers serializer cache defaults]
  iStorage
  (-c-read [_ commit-address]
    (storage/read-json store commit-address))
  (-c-write [_ ledger-alias commit-data]
    (let [path (str/join "/" [ledger-alias "commit"])]
      (storage/content-write-json store path commit-data)))
  (-txn-write [_ ledger-alias txn-data]
    (let [path (str/join "/" [ledger-alias "txn"])]
      (storage/content-write-json store path txn-data)))

  iConnection
  (-did [_] (:did defaults)))

#?(:clj
   (defmethod print-method Connection [^Connection conn, ^Writer w]
     (.write w (str "#fluree/Connection "))
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

(defn connect
  [{:keys [parallelism store index-store cache serializer primary-publisher
           secondary-publishers subscribers defaults]
    :or   {serializer (json-serde)} :as _opts}]
  (let [id    (random-uuid)
        state (blank-state)]
    (->Connection id state parallelism store index-store primary-publisher
                  secondary-publishers subscribers serializer cache defaults)))

(defn register-ledger
  "Creates a promise-chan and saves it in a cache of ledgers being held
  in-memory on the conn.

  Returns a two-tuple of
  [not-cached? promise-chan]

  where not-cached? is true if a new promise-chan was created, false if an
  existing promise-chan was found.

  promise-chan is the new promise channel that must have the final ledger `put!` into it
  assuming success? is true, otherwise it will return the existing found promise-chan when
  success? is false"
  [{:keys [state] :as _conn} ledger-alias]
  (let [new-p-chan (async/promise-chan)
        new-state  (swap! state update-in [:ledger ledger-alias]
                           (fn [existing]
                             (or existing new-p-chan)))
        p-chan     (get-in new-state [:ledger ledger-alias])
        cached?    (not= p-chan new-p-chan)]
    (log/debug "Registering ledger: " ledger-alias " cached? " cached?)
    [cached? p-chan]))

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias))

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn notify-ledger
  [conn commit-map]
  (go-try
    (let [expanded-commit (json-ld/expand commit-map)
          ledger-alias    (get-first-value expanded-commit const/iri-alias)]
      (if ledger-alias
        (if-let [ledger-c (cached-ledger conn ledger-alias)]
          (<? (ledger/-notify (<? ledger-c) expanded-commit))
          (log/debug "No cached ledger found for commit: " commit-map))
        (log/warn "Notify called with a data that does not have a ledger alias."
                  "Are you sure it is a commit?: " commit-map)))))

(defn all-nameservices
  [{:keys [primary-publisher secondary-publishers subscribers] :as _conn}]
  (cons primary-publisher (concat secondary-publishers subscribers)))

(def fluree-address-prefix
  "fluree:")

(defn fluree-address?
  [x]
  (str/starts-with? x fluree-address-prefix))

(defn relative-ledger-alias?
  [ledger-alias]
  (not (fluree-address? ledger-alias)))

(defn addresses
  "Retrieve address for each nameservices based on a relative ledger-alias.
  If ledger-alias is not relative, returns only the current ledger alias.

  TODO - if a single non-relative address is used, and the ledger exists,
  we should retrieve all stored ns addresses in the commit if possible and
  try to use all nameservices."
  [conn ledger-alias]
  (go-try
    (if (relative-ledger-alias? ledger-alias)
      (let [nameservices (all-nameservices conn)]
        (loop [nameservices* nameservices
               addresses     []]
          (let [ns (first nameservices*)]
            (if ns
              (if-let [address (<? (nameservice/address ns ledger-alias))]
                (recur (rest nameservices*) (conj addresses address))
                (recur (rest nameservices*) addresses))
              addresses))))
      [ledger-alias])))

(defn primary-address
  "From a connection, lookup primary address from nameservice(s) for a given
  ledger alias"
  ([conn ledger-alias]
   (go-try
     (first (<? (addresses conn ledger-alias))))))

(defn lookup-commit
  "Returns commit address from first matching nameservice on a conn
   for a given ledger alias and branch"
  [conn ledger-address]
  (let [nameservices (all-nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (if-let [commit-address (<? (nameservice/lookup ns ledger-address))]
            commit-address
            (recur (rest nameservices*))))))))

(defn read-latest-commit
  [{:keys [store] :as conn} ledger-address]
  (go-try
    (if-let [commit-addr (<? (lookup-commit conn ledger-address))]
      (let [commit-data (<? (storage/read-json store commit-addr))]
        (assoc commit-data "address" commit-addr))
      (throw (ex-info (str "Unable to load. No commit exists for: " ledger-address)
                      {:status 400 :error :db/invalid-commit-address})))))

(defn file-read?
  [address]
  (str/ends-with? address ".json"))

(defn read-resource
  [{:keys [store] :as conn} resource-address]
  (if (file-read? resource-address)
    (storage/read-json store resource-address)
    (read-latest-commit conn resource-address)))

(defn ledger-exists?
  "Checks nameservices on a connection and returns true if any nameservice
  already has a ledger associated with the given alias."
  [conn ledger-alias]
  (go-try
    (boolean (<? (lookup-commit conn ledger-alias)))))

(defn subscribe-ledger
  "Initiates subscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (all-nameservices conn)
        callback     (fn [msg]
                       (log/info "Subscription message received: " msg)
                       (let [action       (get msg "action")
                             ledger-alias (get msg "ledger")
                             data         (get msg "data")]
                         (if (= "new-commit" action)
                           (notify-ledger conn data)
                           (log/info "New subscritipn message with action: " action "received, ignored."))))]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (nameservice/-subscribe ns ledger-alias callback))
          (recur (rest nameservices*)))))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (all-nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (nameservice/-unsubscribe ns ledger-alias))
          (recur (rest nameservices*)))))))

(defn parse-did
  [conn did]
  (if did
    (if (map? did)
      did
      {:id did})
    (-did conn)))

(defn parse-ledger-options
  [conn {:keys [did branch indexing]
         :or   {branch :main}}]
  (let [did*           (parse-did conn did)
        ledger-default (-> conn :ledger-defaults :indexing)
        indexing*      (merge ledger-default indexing)]
    {:did      did*
     :branch   branch
     :indexing indexing*}))

(defn create-ledger
  [{:keys [primary-publisher secondary-publishers subscribers index-store] commit-store :store,
    :as conn}
   ledger-alias opts]
  (go-try
    (let [[cached? ledger-chan] (register-ledger conn ledger-alias)]
      (if cached?
        (throw (ex-info (str "Unable to create new ledger, one already exists for: " ledger-alias)
                        {:status 400
                         :error  :db/ledger-exists}))
        (let [address      (<? (primary-address conn ledger-alias))
              ns-addresses (<? (addresses conn ledger-alias))
              ledger-opts  (parse-ledger-options conn opts)
              ledger       (<! (ledger/create {:alias                ledger-alias
                                               :address              address
                                               :primary-publisher    primary-publisher
                                               :secondary-publishers secondary-publishers
                                               :subscribers          subscribers
                                               :ns-addresses         ns-addresses
                                               :commit-store         commit-store
                                               :index-store          index-store}
                                              ledger-opts))]
          (when (util/exception? ledger)
            (release-ledger conn ledger-alias))
          (async/put! ledger-chan ledger)
          ledger)))))

(defn commit->ledger-alias
  "Returns ledger alias from commit map, if present. If not present
  then tries to resolve the ledger alias from the nameservice."
  [conn db-alias commit-map]
  (or (get-first-value commit-map const/iri-alias)
      (->> (all-nameservices conn)
           (some (fn [ns]
                   (nameservice/alias ns db-alias))))))

(defn load-ledger*
  [{:keys [store index-store primary-publisher secondary-publishers] :as conn}
   ledger-chan address]
  (go-try
    (let [commit-addr  (<? (lookup-commit conn address))
          _            (log/debug "Attempting to load from address:" address
                                  "with commit address:" commit-addr)
          _            (when-not commit-addr
                         (throw (ex-info (str "Unable to load. No record of ledger exists: " address)
                                         {:status 400 :error :db/invalid-commit-address})))
          [commit _]   (<? (commit-storage/read-commit-jsonld store commit-addr))
          _            (when-not commit
                         (throw (ex-info (str "Unable to load. Commit file for ledger: " address
                                              " at location: " commit-addr " is not found.")
                                         {:status 400 :error :db/invalid-db})))
          _            (log/debug "load commit:" commit)
          ledger-alias (commit->ledger-alias conn address commit)
          branch       (keyword (get-first-value commit const/iri-branch))

          {:keys [did branch indexing]} (parse-ledger-options conn {:branch branch})

          ledger   (ledger/instantiate ledger-alias address primary-publisher secondary-publishers
                                       branch store index-store did indexing commit)]
      (subscribe-ledger conn ledger-alias)
      (async/put! ledger-chan ledger)
      ledger)))

(defn load-ledger-address
  [conn address]
  (let [alias (nameservice/address-path address)
        [cached? ledger-chan] (register-ledger conn alias)]
    (if cached?
      ledger-chan
      (load-ledger* conn ledger-chan address))))

(defn load-ledger-alias
  [conn alias]
  (go-try
    (let [[cached? ledger-chan] (register-ledger conn alias)]
      (if cached?
        (<? ledger-chan)
        (let [address (<! (primary-address conn alias))]
          (if (util/exception? address)
            (do (release-ledger conn alias)
                (async/put! ledger-chan
                            (ex-info (str "Load for " alias " failed due to failed address lookup.")
                                     {:status 400 :error :db/invalid-address}
                                     address)))
            (<? (load-ledger* conn ledger-chan address))))))))

(defn load-ledger
  [conn alias-or-address]
  (if (fluree-address? alias-or-address)
    (load-ledger-address conn alias-or-address)
    (load-ledger-alias conn alias-or-address)))
