(ns fluree.db.connection
  (:refer-clojure :exclude [replicate])
  (:require [clojure.core.async :as async :refer [<! go go-loop]]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [fluree.db.commit.storage :as commit-storage]
            [fluree.db.constants :as const]
            [fluree.db.did :as did]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.json-ld.policy.rules :as policy.rules]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.serde.json :refer [json-serde]]
            [fluree.db.storage :as storage]
            [fluree.db.track :as track]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.context :as context]
            [fluree.db.util.core :as util :refer [get-first get-first-value try* catch*]]
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

(defn release-ledger
  "Opposite of register-ledger. Removes reference to a ledger from conn"
  [{:keys [state] :as _conn} ledger-alias]
  (swap! state update :ledger dissoc ledger-alias)
  nil)

(defn cached-ledger
  "Returns a cached ledger from the connection if it is cached, else nil"
  [{:keys [state] :as _conn} ledger-alias]
  (get-in @state [:ledger ledger-alias]))

(defn notify
  [{:keys [commit-catalog] :as conn} address hash]
  (go-try
    (if-let [expanded-commit (<? (commit-storage/read-commit-jsonld commit-catalog address hash))]
      (if-let [ledger-alias (get-first-value expanded-commit const/iri-alias)]
        (if-let [ledger-ch (cached-ledger conn ledger-alias)]
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
                      (release-ledger conn ledger-alias)))))
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
                (let [{:keys [address hash]} (get msg "data")]
                  (notify conn address hash))
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
  [{:keys [commit-catalog index-catalog] :as conn} ledger-alias opts]
  (go-try
    (if (<? (ledger-exists? conn ledger-alias))
      (throw-ledger-exists ledger-alias)
      (let [[cached? ledger-chan] (register-ledger conn ledger-alias)]
        (if  cached?
          (throw-ledger-exists ledger-alias)
          (let [addr          (<? (primary-address conn ledger-alias))
                publish-addrs (<? (publishing-addresses conn ledger-alias))
                pubs          (publishers conn)
                ledger-opts   (parse-ledger-options conn opts)
                ledger        (<! (ledger/create {:conn              conn
                                                  :alias             ledger-alias
                                                  :address           addr
                                                  :publish-addresses publish-addrs
                                                  :commit-catalog    commit-catalog
                                                  :index-catalog     index-catalog
                                                  :publishers        pubs}
                                                 ledger-opts))]
            (when (util/exception? ledger)
              (release-ledger conn ledger-alias))
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
  [{:keys [commit-catalog index-catalog] :as conn}
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

                pubs   (publishers conn)
                ledger (ledger/instantiate conn ledger-alias address branch commit-catalog
                                           index-catalog pubs indexing did expanded-commit)]
            (subscribe-ledger conn ledger-alias)
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
            (do (release-ledger conn alias)
                (let [ex (ex-info (str "Load for " alias " failed due to failed address lookup.")
                                  {:status 404, :error :db/unkown-ledger})]
                  (async/put! ledger-chan ex)
                  (throw ex)))))))))

(defn load-ledger
  [conn alias-or-address]
  (if (fluree-address? alias-or-address)
    (load-ledger-address conn alias-or-address)
    (load-ledger-alias conn alias-or-address)))

(def f-context {"f" "https://ns.flur.ee/ledger#"})

(defn save-txn!
  [{:keys [commit-catalog] :as _conn} ledger-alias txn]
  (let [path (str/join "/" [ledger-alias "txn"])]
    (storage/content-write-json commit-catalog path txn)))

(defn resolve-txn
  [{:keys [commit-catalog] :as _conn} address]
  (storage/read-json commit-catalog address))

;; TODO - as implemented the db handles 'staged' data as per below (annotation, raw txn)
;; TODO - however this is really a concern of "commit", not staging and I don't think the db should be handling any of it
(defn write-transaction!
  [conn ledger-alias staged]
  (go-try
    (let [{:keys [txn author annotation]} staged]
      (if txn
        (let [{txn-id :address} (<? (save-txn! conn ledger-alias txn))]
          {:txn-id     txn-id
           :author     author
           :annotation annotation})
        staged))))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-address]
  [(assoc commit-map :address commit-address)
   (assoc commit-jsonld "address" commit-address)])

(defn update-commit-id
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [[commit-map commit-jsonld] commit-hash]
  (let [commit-id (commit-data/hash->commit-id commit-hash)]
    [(assoc commit-map :id commit-id)
     (assoc commit-jsonld "id" commit-id)]))

(defn write-commit
  [commit-storage alias {:keys [did private]} commit]
  (go-try
    (let [commit-jsonld (commit-data/->json-ld commit)
          signed-commit (if did
                          (<? (credential/generate commit-jsonld private (:id did)))
                          commit-jsonld)
          commit-res    (<? (commit-storage/write-jsonld commit-storage alias signed-commit))

          [commit* commit-jsonld*]
          (-> [commit commit-jsonld]
              (update-commit-id (:hash commit-res))
              (update-commit-address (:address commit-res)))]
      {:commit-map    commit*
       :commit-jsonld commit-jsonld*
       :write-result  commit-res})))

(defn publish-commit
  "Publishes commit to all nameservices registered with the ledger."
  [{:keys [primary-publisher secondary-publishers] :as _conn} commit-jsonld]
  (go-try
    (let [result (<? (nameservice/publish primary-publisher commit-jsonld))]
      (nameservice/publish-to-all commit-jsonld secondary-publishers)
      result)))

(defn formalize-commit
  [{prev-commit :commit :as staged-db} new-commit]
  (let [max-ns-code (-> staged-db :namespace-codes iri/get-max-namespace-code)]
    (-> staged-db
        (assoc :commit new-commit
               :staged nil
               :prev-commit prev-commit
               :max-namespace-code max-ns-code)
        (commit-data/add-commit-flakes))))

(defn sanitize-commit-options
  "Parses the commit options and removes non-public opts."
  [opts]
  (if (string? opts)
    {:message opts}
    (select-keys opts [:context :did :private :message :tag :index-files-ch])))

(defn parse-commit-context
  [context]
  (let [parsed-context (if context
                         (-> context
                             json-ld/parse-context
                             (json-ld/parse-context f-context))
                         (json-ld/parse-context f-context))]
    (context/stringify parsed-context)))

(defn parse-keypair
  [ledger {:keys [did private] :as opts}]
  (let [private* (or private
                     (:private did)
                     (-> ledger :did :private))
        did*     (or (some-> private* did/private->did)
                     did
                     (:did ledger))]
    (assoc opts :did did*, :private private*)))

(defn parse-data-helpers
  [{:keys [context] :as opts}]
  (let [ctx-used-atom (atom {})
        compact-fn    (json-ld/compact-fn context ctx-used-atom)]
    (assoc opts
           :commit-data-opts {:compact-fn    compact-fn
                              :compact       (fn [iri] (json-ld/compact iri compact-fn))
                              :id-key        (json-ld/compact "@id" compact-fn)
                              :type-key      (json-ld/compact "@type" compact-fn)
                              :ctx-used-atom ctx-used-atom})))

(defn parse-commit-opts
  [ledger opts]
  (-> opts
      (update :context parse-commit-context)
      (->> (parse-keypair ledger))
      parse-data-helpers))

(defn commit!
  "Finds all uncommitted transactions and wraps them in a Commit document as the subject
  of a VerifiableCredential. Persists according to the :ledger :conn :method and
  returns a db with an updated :commit."
  ([ledger db]
   (commit! ledger db {}))
  ([{:keys [conn] ledger-alias :alias, :as ledger}
    {:keys [branch t stats commit] :as staged-db}
    opts]
   (go-try
     (let [{:keys [commit-catalog]} conn

           {:keys [tag time message did private commit-data-opts index-files-ch]
            :or   {time (util/current-time-iso)}}
           (parse-commit-opts ledger opts)

           {:keys [db-jsonld staged-txn]}
           (flake-db/db->jsonld staged-db commit-data-opts)

           {:keys [txn-id author annotation]}
           (<? (write-transaction! conn ledger-alias staged-txn))

           data-write-result (<? (commit-storage/write-jsonld commit-catalog ledger-alias db-jsonld))
           db-address        (:address data-write-result) ; may not have address (e.g. IPFS) until after writing file
           dbid              (commit-data/hash->db-id (:hash data-write-result))
           keypair           {:did did, :private private}

           new-commit (commit-data/new-db-commit-map {:old-commit commit
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

       (if (track/track-txn? opts)
         (-> write-result
             (select-keys [:address :hash :size])
             (assoc :ledger-id ledger-alias
                    :t t
                    :db db*))
         db*)))))

(defn stage-triples
  "Stages a new transaction that is already parsed into the
   internal Fluree triples format."
  [db parsed-txn]
  (go-try
    (let [parsed-opts    (:opts parsed-txn)
          parsed-context (:context parsed-opts)
          identity       (:identity parsed-opts)]
      (if (track/track-txn? parsed-opts)
        (let [track-time? (track/track-time? parsed-opts)
              track-fuel? (track/track-fuel? parsed-opts)
              tracker     (track/init parsed-opts)
              policy-db   (if (policy/policy-enforced-opts? parsed-opts)
                            (<? (policy/policy-enforce-db db tracker parsed-context parsed-opts))
                            db)]
          (try*
            (let [staged-db     (<? (transact/stage policy-db tracker identity parsed-txn parsed-opts))
                  policy-report (policy.rules/enforcement-report staged-db)
                  tally         (track/tally tracker)]
              (cond-> (assoc tally :status 200, :db staged-db)
                policy-report (assoc :policy policy-report)))
            (catch* e
              (throw (ex-info (ex-message e)
                              (let [policy-report (policy.rules/enforcement-report policy-db)
                                    tally         (track/tally tracker)]
                                (cond-> (merge (ex-data e) tally)
                                  policy-report (assoc :policy policy-report)))
                              e)))))
        (let [policy-db (if (policy/policy-enforced-opts? parsed-opts)
                          (<? (policy/policy-enforce-db db parsed-context parsed-opts))
                          db)]
          (<? (transact/stage policy-db identity parsed-txn parsed-opts)))))))

(defn transact-ledger!
  [_conn ledger parsed-txn]
  (go-try
    (let [{:keys [branch] :as parsed-opts,
           :or   {branch commit-data/default-branch}}
          (:opts parsed-txn)

          db       (ledger/current-db ledger branch)
          staged   (<? (stage-triples db parsed-txn))
          ;; commit API takes a did-map and parsed context as opts
          ;; whereas stage API takes a did IRI and unparsed context.
          ;; Dissoc them until deciding at a later point if they can carry through.
          cmt-opts (dissoc parsed-opts :context :identity)]
      (if (track/track-txn? parsed-opts)
        (let [staged-db     (:db staged)
              commit-result (<? (commit! ledger staged-db cmt-opts))]
          (merge staged commit-result))
        (<? (commit! ledger staged cmt-opts))))))

(defn not-found?
  [e]
  (-> e ex-data :status (= 404)))

(defn load-transacting-ledger
  [conn ledger-id]
  (go-try
    (try* (<? (load-ledger conn ledger-id))
          (catch* e
            (if (not-found? e)
              (throw (ex-info (str "Ledger " ledger-id " does not exist")
                              {:status 409 :error :db/ledger-not-exists}
                              e))
              (throw e))))))

(defn transact!
  [conn {:keys [ledger-id] :as parsed-txn}]
  (go-try
    (let [ledger (<? (load-transacting-ledger conn ledger-id))]
      (<? (transact-ledger! conn ledger parsed-txn)))))

(defn replicate-index-node
  [conn address data]
  (let [clg (-> conn :index-catalog :storage)]
    (storage/write-catalog-bytes clg address data)))
