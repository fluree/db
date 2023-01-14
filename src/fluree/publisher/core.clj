(ns fluree.publisher.core
  (:refer-clojure :exclude [list])
  (:require [fluree.common.identity :as ident]
            [fluree.common.model :as model]
            [fluree.common.protocols :as service-proto]
            [fluree.db.json-ld.credential :as credential]
            [fluree.db.util.async :refer [<?? go-try]]
            [fluree.db.util.log :as log]
            [fluree.publisher.ledger :as ledger]
            [fluree.publisher.ledger-entry :as ledger-entry]
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
  (let [{:keys [address/type address/path]} (ident/address-parts address)]
    (case type
      :ledger (let [entry-path (<?? (store/read (:store pub) path))]
                (<?? (store/read (:store pub) entry-path)))
      :ledger-entry (<?? (store/read (:store pub) path)))))

(defn init-ledger
  [pub ledger-name {:keys [context head-address db-address] :as opts}]
  (let [store          (:store pub)
        init-ledger    (ledger/create store ledger-name opts)
        ledger-entry   (ledger-entry/create store init-ledger nil {:db/address db-address})
        ledger         (assoc init-ledger :ledger/head ledger-entry)
        ledger-address (:ledger/address ledger)
        existing?      (pull-publisher pub ledger-address)

        _ (when existing? (throw (ex-info (str "Cannot initialize file ledger: " (pr-str ledger-name)
                                               " already exists.")
                                          {:ledger-name    ledger-name
                                           :ledger-address ledger-address
                                           :opts           opts})))

        final-ledger  (if (:did pub)
                        (credential/generate ledger (:did pub))
                        ledger)
        entry-address (ledger-entry/create-entry-address store ledger-name "init")

        {entry-path :address/path}  (ident/address-parts entry-address)
        {ledger-path :address/path} (ident/address-parts (:ledger/address ledger))]
    ;; create the ledger in the store
    (<?? (store/write store entry-path final-ledger))
    ;; set the head to the initial entry (no commit)
    (<?? (store/write store ledger-path entry-path))
    (:ledger/address ledger)))

(defn list-ledgers
  [{:keys [store] :as pub}]
  (let [ledger-head-paths (<?? (store/list store "head/"))
        ledger-heads (map (fn [head-path] (<?? (store/read store head-path))) ledger-head-paths)]
    (map (fn [entry-address] (<?? (store/read store entry-address))) ledger-heads)))

(defn push-publisher
  [pub ledger-address {:keys [commit-summary db-summary]}]
  (let [store        (:store pub)
        prev-ledger  (pull-publisher pub ledger-address)
        ;; unwrap ledger from credential if it's wrapped
        prev-ledger  (get prev-ledger :cred/credential-subject prev-ledger)
        entry        (ledger-entry/create store prev-ledger commit-summary db-summary)
        ledger       (assoc prev-ledger :ledger/head entry)
        final-ledger (if (:did pub)
                       (credential/generate ledger (:did pub))
                       ledger)

        {entry-path :address/path}  (ident/address-parts (:entry/address entry))
        {ledger-path :address/path} (ident/address-parts (:ledger/address ledger))]
    ;; add the entry to the store
    (<?? (store/write store entry-path final-ledger))
    ;; mutate the head in store
    (<?? (store/write store ledger-path entry-path))
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
