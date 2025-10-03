(ns fluree.db.virtual-graph.create
  "Handles creation of virtual graphs, delegating to type-specific implementations."
  (:require [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.virtual-graph :as ns-vg]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.virtual-graph.nameservice-loader :as vg-loader]))

(defmulti create-vg
  "Creates a virtual graph based on its type. Dispatches on :type key."
  (fn [_conn config] (:type config)))

(defn- validate-common-config
  "Validates common configuration parameters for all virtual graph types."
  [{:keys [name type] :as config}]
  (cond
    (not name)
    (throw (ex-info "Virtual graph requires :name"
                    {:error :db/invalid-config :config config}))

    (not type)
    (throw (ex-info "Virtual graph requires :type"
                    {:error :db/invalid-config :config config}))

    (and (string? name) (str/includes? name "@"))
    (throw (ex-info "Virtual graph name cannot contain '@' symbol"
                    {:error :db/invalid-config :name name}))))

(defn create
  "Main entry point for creating virtual graphs."
  [conn config]
  (go-try
    (validate-common-config config)
    (<? (create-vg conn config))))

(defn- validate-bm25-config
  "Validates BM25-specific configuration."
  [{:keys [config]}]
  (let [ledgers (get-in config [:ledgers] [])]
    (when (not= 1 (count ledgers))
      (throw (ex-info "BM25 virtual graphs currently support only a single ledger. Multi-ledger support is not yet implemented."
                      {:error :db/invalid-config
                       :type :bm25
                       :ledgers ledgers
                       :count (count ledgers)})))))

(defn- ensure-string-name
  "Converts keyword names to strings."
  [name]
  (if (keyword? name)
    (clojure.core/name name)
    name))

(defn- prepare-bm25-config
  "Prepares the BM25 configuration for publishing."
  [{:keys [name config dependencies]}]
  (let [vg-name (ensure-string-name name)
        ledgers (get-in config [:ledgers] [])]
    {:vg-name vg-name
     :vg-type "fidx:BM25"
     :config config
     :dependencies (or dependencies
                       (mapv #(str % ":main") ledgers))}))

(defn- load-and-validate-ledgers
  "Loads all ledgers and validates they exist. Returns loaded ledgers."
  [conn ledger-aliases]
  (go-try
    (loop [remaining ledger-aliases
           loaded {}]
      (if-let [ledger-alias (first remaining)]
        (if-let [ledger (<? (connection/load-ledger-alias conn ledger-alias))]
          (recur (rest remaining) (assoc loaded ledger-alias ledger))
          (throw (ex-info (str "Ledger does not exist: " ledger-alias)
                          {:error :db/invalid-config
                           :ledger ledger-alias})))
        loaded))))

(defn- initialize-bm25-for-ledgers
  "Initializes the BM25 virtual graph for all dependent ledgers.
   Returns the loaded virtual graph instance."
  [loaded-ledgers publisher vg-name]
  (go-try
    ;; Single ledger support only for now
    (let [[_alias ledger] (first loaded-ledgers)
          db (ledger/current-db ledger)
          vg (<? (vg-loader/load-virtual-graph-from-nameservice db publisher vg-name))]
      vg)))

(defmethod create-vg :bm25
  [conn vg-config]
  (go-try
    (validate-bm25-config vg-config)

    (let [full-config (prepare-bm25-config vg-config)
          {:keys [vg-name]} full-config
          publisher (connection/primary-publisher conn)
          ledger-aliases (get-in vg-config [:config :ledgers] [])]

      ;; Check if virtual graph already exists
      (when (<? (ns-vg/virtual-graph-exists? publisher vg-name))
        (throw (ex-info (str "Virtual graph already exists: " vg-name)
                        {:error :db/invalid-config
                         :vg-name vg-name})))

      (let [loaded-ledgers (<? (load-and-validate-ledgers conn ledger-aliases))]
        (<? (nameservice/publish publisher full-config))
        (<? (initialize-bm25-for-ledgers loaded-ledgers publisher vg-name))))))

(defmethod create-vg :default
  [_conn {:keys [type]}]
  (go-try
    (throw (ex-info (str "Unknown virtual graph type: " type)
                    {:error :db/invalid-config
                     :type type}))))