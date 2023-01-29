(ns fluree.publisher.core
  (:refer-clojure :exclude [list resolve])
  (:require
   [fluree.common.identity :as ident]
   [fluree.common.iri :as iri]
   [fluree.common.model :as model]
   [fluree.common.protocols :as service-proto]
   [fluree.db.json-ld.credential :as credential]
   [fluree.db.util.async :refer [<??]]
   [fluree.db.util.log :as log]
   [fluree.publisher.ledger :as ledger]
   [fluree.publisher.model :as pub-model]
   [fluree.publisher.protocols :as pub-proto]
   [fluree.store.api :as store]
   [clojure.string :as str]))

(defn stop-publisher
  [pub]
  (log/info "Stopping publisher " (service-proto/id pub) ".")
  (store/stop (:store pub))
  :stopped)

(defn resolve-ledger
  [pub ledger-name]
  (<?? (store/read (:store pub) (ledger/ledger-path ledger-name))))

(defn init-ledger
  [{:keys [store] :as pub} ledger-name {:keys [context tx-address db-address] :as opts}]
  (let [ledger-path (ledger/ledger-path ledger-name)
        existing?   (resolve-ledger pub ledger-name)

        _ (when existing? (throw (ex-info (str "Cannot initialize ledger: " (pr-str ledger-name)
                                               " already exists.")
                                          {:ledger-name ledger-name
                                           :opts        opts})))

        ledger       (ledger/create-ledger store ledger-name opts)
        final-ledger (if (:did pub)
                       ledger
                       ;; TODO: actually generate
                       #_(credential/generate ledger (:did pub))
                       ledger)]
    ;; create the ledger in the store
    (<?? (store/write store ledger-path final-ledger))
    final-ledger))

(defn list-ledgers
  [{:keys [store] :as pub}]
  (let [ledger-head-paths (<?? (store/list store "ledger/"))]
    (map (fn [head-path]
           ;; manually construct ledger-name from head-path (chop "ledger/" off front)
           (resolve-ledger pub (subs head-path (count "ledger/"))))
         (sort ledger-head-paths))))

(defn publish-ledger
  [pub ledger-name {:keys [tx-summary db-summary]}]
  (let [store        (:store pub)
        prev-ledger  (resolve-ledger pub ledger-name)
        ;; unwrap ledger from credential if it's wrapped
        prev-ledger  (get prev-ledger :cred/credential-subject prev-ledger)
        new-head     (ledger/create-ledger-entry prev-ledger tx-summary db-summary)
        ledger       (assoc prev-ledger iri/LedgerHead new-head)
        final-ledger (if (:did pub)
                       ledger
                       ;; TODO: actually generate
                       #_(credential/generate ledger (:did pub))
                       ledger)]
    ;; mutate the head in store
    (<?? (store/write store (ledger/ledger-path ledger-name) final-ledger))
    final-ledger))

(defrecord Publisher [id store]
  service-proto/Service
  (id [_] id)
  (stop [pub] (stop-publisher pub))

  pub-proto/Publisher
  (init [pub ledger-name opts] (init-ledger pub ledger-name opts))
  (list [pub] (list-ledgers pub))
  (publish [pub ledger-name info] (publish-ledger pub ledger-name info))
  (resolve [pub ledger-name] (resolve-ledger pub ledger-name)))

(defn create-publisher
  [{:keys [:pub/id :pub/did :pub/store-config :pub/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Started Publisher." id)
    (map->Publisher {:id id :did did :store store})))

(defn start
  [config]
  (log/info "Starting Publisher." config)
  (if-let [validation-error (model/explain pub-model/PublisherConfig config)]
    (throw (ex-info "Invalid publisher config." {:errors (model/report validation-error)
                                                 :config config}))
    (create-publisher config)))

(defn stop
  [publisher]
  (service-proto/stop publisher))

(defn init
  [publisher ledger-name opts]
  (pub-proto/init publisher ledger-name opts))

(defn list
  [publisher]
  (pub-proto/list publisher))

(defn publish
  [publisher ledger-name summary]
  (pub-proto/publish publisher ledger-name summary))

(defn resolve
  [publisher ledger-name]
  (pub-proto/resolve publisher ledger-name))
