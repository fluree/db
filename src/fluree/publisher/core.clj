(ns fluree.publisher.core
  (:refer-clojure :exclude [list])
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
   [fluree.store.api :as store]))

(defn stop-publisher
  [pub]
  (log/info "Stopping publisher " (service-proto/id pub) ".")
  (store/stop (:store pub))
  :stopped)

(defn pull-publisher
  [pub address]
  (let [{:keys [address/path]} (ident/address-parts address)]
    (<?? (store/read (:store pub) path))))

(defn init-ledger
  [pub ledger-name {:keys [context head-address db-address] :as opts}]
  (let [store  (:store pub)
        ledger (ledger/create-ledger store ledger-name opts)

        ledger-address (get ledger iri/LedgerAddress)
        existing?      (pull-publisher pub ledger-address)

        _ (when existing? (throw (ex-info (str "Cannot initialize ledger: " (pr-str ledger-name)
                                               " already exists.")
                                          {:ledger-name    ledger-name
                                           :ledger-address ledger-address
                                           :opts           opts})))
        final-ledger  (if (:did pub)
                        ledger
                        ;; TODO: actually generate
                        #_(credential/generate ledger (:did pub))
                        ledger)

        ledger-path (ledger/ledger-path ledger-name)]
    ;; create the ledger in the store
    (<?? (store/write store ledger-path final-ledger))
    ledger-address))

(defn list-ledgers
  [{:keys [store] :as pub}]
  (let [ledger-head-paths (<?? (store/list store "head/"))]
    (map (fn [head-path] (<?? (store/read store head-path))) (sort ledger-head-paths))))

(defn push-publisher
  [pub ledger-address {:keys [commit-summary db-summary]}]
  (let [store        (:store pub)
        prev-ledger  (pull-publisher pub ledger-address)
        ;; unwrap ledger from credential if it's wrapped
        prev-ledger  (get prev-ledger :cred/credential-subject prev-ledger)
        new-head     (ledger/create-ledger-entry prev-ledger commit-summary db-summary)
        ledger       (assoc prev-ledger iri/LedgerHead new-head)
        final-ledger (if (:did pub)
                       ledger
                       ;; TODO: actually generate
                       #_(credential/generate ledger (:did pub))
                       ledger)

        {ledger-path :address/path} (ident/address-parts ledger-address)]
    ;; mutate the head in store
    (<?? (store/write store ledger-path final-ledger))
    final-ledger))

(defrecord Publisher [id store]
  service-proto/Service
  (id [_] id)
  (stop [pub] (stop-publisher pub))

  pub-proto/Publisher
  (init [pub ledger-name opts] (init-ledger pub ledger-name opts))
  (list [pub] (list-ledgers pub))
  (push [pub ledger-address info] (push-publisher pub ledger-address info))
  (pull [pub ledger-address] (pull-publisher pub ledger-address)))

(defn create-publisher
  [{:keys [:pub/id :pub/did :pub/store-config :pub/store] :as config}]
  (let [store (or store (store/start store-config))
        id (or id (random-uuid))]
    (log/info "Starting Publisher " id "." config)
    (map->Publisher {:id id :did did :store store})))

(defn start
  [config]
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

(defn push
  [publisher ledger-address summary]
  (pub-proto/push publisher ledger-address summary))

(defn pull
  [publisher address]
  (pub-proto/pull publisher address))
