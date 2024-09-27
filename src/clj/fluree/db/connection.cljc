(ns fluree.db.connection
  (:refer-clojure :exclude [replicate])
  (:require [clojure.core.async :as async :refer [<!]]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.did :as did]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.transact :as transact]
            [fluree.db.storage :as storage]
            [fluree.db.util.core :as util :refer [get-first-value try* catch*]]
            [fluree.db.util.context :as context]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.json-ld :as json-ld]
            [fluree.db.ledger :as ledger])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

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

(defrecord Connection [id state parallelism commit-catalog index-catalog primary-publisher
                       secondary-publishers remote-systems serializer cache defaults])

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
  [{:keys [parallelism commit-catalog index-catalog cache serializer
           primary-publisher secondary-publishers remote-systems defaults]
    :or   {serializer (json-serde)} :as _opts}]
  (let [id    (random-uuid)
        state (blank-state)]
    (->Connection id state parallelism commit-catalog index-catalog primary-publisher
                  secondary-publishers remote-systems serializer cache defaults)))

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
          (<? (ledger/notify (<? ledger-c) expanded-commit))
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

(defn read-file-address
  [{:keys [commit-catalog] :as _conn} addr]
  (go-try
    (let [json-data (<? (storage/read-catalog-json commit-catalog addr))]
      (assoc json-data "address" addr))))

(defn read-latest-commit
  [conn ledger-address]
  (go-try
    (if-let [commit-addr (<? (lookup-commit conn ledger-address))]
      (<? (read-file-address conn commit-addr))
      (throw (ex-info (str "Unable to load. No commit exists for: " ledger-address)
                      {:status 400 :error :db/invalid-commit-address})))))

(defn ledger-exists?
  "Checks nameservices on a connection and returns true if any nameservice
  already has a ledger associated with the given alias."
  [conn ledger-alias]
  (go-try
    (boolean (<? (lookup-commit conn ledger-alias)))))

(defn subscribe-ledger
  "Initiates subscription requests for a ledger into all subscribers on a connection."
  [conn ledger-alias]
  (let [nameservices (all-nameservices conn)
        callback     (fn [msg]
                       (log/info "Subscription message received: " msg)
                       (let [action       (get msg "action")
                             ledger-alias (get msg "ledger")
                             data         (get msg "data")]
                         (if (= "new-commit" action)
                           (notify-ledger conn data)
                           (log/info "New subscrition message with action: " action "received, ignored."))))]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (nameservice/subscribe ns ledger-alias callback))
          (recur (rest nameservices*)))))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (all-nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (nameservice/unsubscribe ns ledger-alias))
          (recur (rest nameservices*)))))))

(defn parse-did
  [conn did]
  (if did
    (if (map? did)
      did
      {:id did})
    (-> conn :defaults :did)))

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
  [{:keys [primary-publisher secondary-publishers subscribers commit-catalog index-catalog]
    :as   conn}
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
              ledger       (<! (ledger/create {:conn                 conn
                                               :alias                ledger-alias
                                               :address              address
                                               :primary-publisher    primary-publisher
                                               :secondary-publishers secondary-publishers
                                               :subscribers          subscribers
                                               :ns-addresses         ns-addresses
                                               :commit-catalog         commit-catalog
                                               :index-catalog          index-catalog}
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
  [{:keys [commit-catalog index-catalog primary-publisher secondary-publishers] :as conn}
   ledger-chan address]
  (go-try
    (let [commit-addr  (<? (lookup-commit conn address))
          _            (log/debug "Attempting to load from address:" address
                                  "with commit address:" commit-addr)
          _            (when-not commit-addr
                         (throw (ex-info (str "Unable to load. No record of ledger exists: " address)
                                         {:status 400 :error :db/invalid-commit-address})))
          [commit _]   (<? (commit-storage/read-commit-jsonld commit-catalog commit-addr))
          _            (when-not commit
                         (throw (ex-info (str "Unable to load. Commit file for ledger: " address
                                              " at location: " commit-addr " is not found.")
                                         {:status 400 :error :db/invalid-db})))
          _            (log/debug "load commit:" commit)
          ledger-alias (commit->ledger-alias conn address commit)
          branch       (keyword (get-first-value commit const/iri-branch))

          {:keys [did branch indexing]} (parse-ledger-options conn {:branch branch})

          ledger   (ledger/instantiate conn ledger-alias address primary-publisher secondary-publishers
                                       branch commit-catalog index-catalog did indexing commit)]
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

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn enrich-commit-opts
  [ledger {:keys [context did private] :as _opts}]
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
     {:private private*
      :did did*}

     :commit-data-helpers
     {:compact-fn compact-fn
      :compact (fn [iri] (json-ld/compact iri compact-fn))
      :id-key (json-ld/compact "@id" compact-fn)
      :type-key (json-ld/compact "@type" compact-fn)
      :ctx-used-atom ctx-used-atom}}))

(defn write-transaction
  [storage ledger-alias txn]
  (let [path (str/join "/" [ledger-alias "txn"])]
    (storage/content-write-catalog-json storage path txn)))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don' think the db should be handling any of it
(defn write-transactions!
  [storage ledger-alias staged]
  (go-try
   (loop [[next-staged & r] staged
          results []]
     (if next-staged
       (let [[txn author-did annotation] next-staged
             results* (if txn
                        (let [{txn-id :address} (<? (write-transaction storage ledger-alias txn))]
                          (conj results [txn-id author-did annotation]))
                        (conj results next-staged))]
         (recur r results*))
       results))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [[_ commit-jsonld :as commit-pair]
          (commit-data/commit->jsonld commit)

          signed-commit (if did
                          (<? (credential/generate commit-jsonld private (:id did)))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (update-commit-address commit-pair (:address commit-res))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger."
  [{:keys [primary-publisher secondary-publishers] :as _conn} commit-jsonld]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (dorun (map (fn [ns]
                    (nameservice/publish ns commit-jsonld)))
             secondary-publishers)
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (update :staged empty)
        (assoc :commit new-commit
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes prev-commit))))

(defn parse-commit-options
  "Parses the commit options and removes non-public opts."
  [opts]
  (if (string? opts)
    {:message opts}
    (select-keys opts [:context :did :private :message :tag :file-data? :index-files-ch])))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger staged-db]
   (commit! ledger staged-db nil))
  ([{:keys [conn] ledger-alias :alias, :as ledger}
    {:keys [branch t stats commit] :as staged-db}
    opts]
   (go-try
     (let [{:keys [commit-catalog]} conn

           {:keys [tag time message file-data? index-files-ch]
            :or   {time (util/current-time-iso)}}
           (parse-commit-options opts)

           {commit-data-opts      :commit-data-helpers
            {:keys [did private]} :commit-opts}
           (enrich-commit-opts ledger opts)

           {:keys [dbid db-jsonld staged-txns]}
           (flake-db/db->jsonld staged-db commit-data-opts)

           ;; TODO - we do not support multiple "transactions" in a single
           ;; commit (although other code assumes we do which needs cleaning)
           [[txn-id author annotation] :as _txns]
           (<? (write-transactions! commit-catalog ledger-alias staged-txns))

           data-write-result (<? (commit-storage/write-jsonld commit-catalog ledger-alias db-jsonld))
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file
           keypair           {:did did :private private}

           new-commit (commit-data/new-db-commit-map
                        {:old-commit commit
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
                         :size       (:size stats)})

           {:keys [commit-map commit-jsonld write-result]}
           (<? (write-commit commit-catalog ledger-alias keypair new-commit))

           db  (formalize-commit staged-db commit-map)
           db* (ledger/update-commit! ledger branch db index-files-ch)]

       (log/debug "Committing t" t "at" time)

       (<? (publish-commit conn commit-jsonld))

       (if file-data?
         {:data-file-meta   data-write-result
          :commit-file-meta write-result
          :db               db*}
         db*)))))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db triples parsed-opts]
  (go-try
   (let [track-fuel? (or (:maxFuel parsed-opts)
                         (:meta parsed-opts))
         identity    (:did parsed-opts)
         policy-db   (if identity
                       (<? (policy/wrap-identity-policy db identity nil))
                       db)]
     (if track-fuel?
       (let [start-time #?(:clj (System/nanoTime)
                           :cljs (util/current-time-millis))
             fuel-tracker       (fuel/tracker (:maxFuel parsed-opts))]
         (try*
          (let [result (<? (transact/stage policy-db fuel-tracker identity triples parsed-opts))]
            {:status 200
             :result result
             :time   (util/response-time-formatted start-time)
             :fuel   (fuel/tally fuel-tracker)})
          (catch* e
                  (throw (ex-info "Error staging database"
                                  {:time (util/response-time-formatted start-time)
                                   :fuel (fuel/tally fuel-tracker)}
                                  e)))))
       (<? (transact/stage policy-db identity triples parsed-opts))))))

(defn transact-ledger!
  [_conn ledger triples {:keys [branch] :as opts, :or {branch :main}}]
  (go-try
    (let [db       (ledger/current-db ledger branch)
          staged   (<? (stage-triples db triples opts))
          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          cmt-opts (dissoc opts :context :did)]
      (<? (commit! ledger staged cmt-opts)))))

(defn transact!
  [conn ledger-id triples opts]
  (go-try
    (let [ledger (<? (load-ledger conn ledger-id))]
      (<? (transact-ledger! conn ledger triples opts)))))
