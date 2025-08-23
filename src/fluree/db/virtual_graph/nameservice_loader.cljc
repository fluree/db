(ns fluree.db.virtual-graph.nameservice-loader
  (:require ;; Register VG type loaders
   #?(:clj [fluree.db.virtual-graph.bm25.index :as bm25])
   #?(:clj [fluree.db.virtual-graph.r2rml.db :as r2rml-db])
   [fluree.db.nameservice.virtual-graph :as ns-vg]
   [fluree.db.util.async :refer [<? go-try]]
   [fluree.db.util.json :as json]
   [fluree.db.util.log :as log]
   [fluree.db.virtual-graph :as vg]
   [fluree.db.virtual-graph.parse :as vg-parse]))

#?(:clj (set! *warn-on-reflection* true))

(defn load-vg-config-from-nameservice
  "Loads a virtual graph configuration from the nameservice"
  [nameservice vg-name]
  (go-try
    (let [vg-record (<? (ns-vg/get-virtual-graph nameservice vg-name))]
      (when (= :not-found vg-record)
        (throw (ex-info (str "Virtual graph not found in nameservice: " vg-name)
                        {:status 404
                         :error :db/invalid-query})))
      vg-record)))

(defn vg-record->config
  "Converts a nameservice VG record to the internal configuration format"
  [vg-record]
  (let [vg-name (get vg-record "@id")
        vg-type (get vg-record "@type")
        raw-config (get-in vg-record ["fidx:config" "@value"])
        ;; Config is stored as JSON string, need to parse it
        config (if (string? raw-config)
                 (json/parse raw-config false)
                 raw-config)]
    {:id vg-name
     :alias vg-name
     :type vg-type
     :vg-name vg-name
     :config config}))

(defmulti create-vg-impl
  "Creates a virtual graph instance based on type.
  Implementations should be registered by the respective VG type namespaces."
  (fn [_db _vg-opts vg-config]
    (let [types (:type vg-config)]
      (cond
        (some #{"fidx:BM25"} types) :bm25
        (some #{"fidx:R2RML"} types) :r2rml
        :else :unknown))))

(defmethod create-vg-impl :unknown
  [_db _vg-opts vg-config]
  (throw (ex-info "Unknown virtual graph type"
                  {:status 400
                   :error :db/invalid-query
                   :type (:type vg-config)})))

(defn create-vg-instance
  "Creates a virtual graph instance from configuration"
  [db vg-config]
  (log/debug "Creating VG instance for config:" vg-config)
  (let [{:keys [type config alias vg-name]} vg-config
        vg-opts (-> config
                    (assoc :alias alias
                           :vg-name vg-name
                           :id (:id vg-config)
                           :type type
                           :genesis-t (:t db))
                    (update "query" vg-parse/select-one->select))]
    (log/debug "VG opts prepared:" vg-opts "Dispatching to type:" type)
    (create-vg-impl db vg-opts vg-config)))

(defn load-virtual-graph-from-nameservice
  "Loads a virtual graph from the nameservice and creates/returns the VG instance.
  This is called when a query references a virtual graph that isn't already loaded."
  [db nameservice vg-name]
  (go-try
    (log/debug "Loading virtual graph from nameservice:" vg-name)
    ;; First check if VG is already loaded
    (if-let [existing-vg (get-in db [:vg vg-name])]
      (do
        (log/debug "Virtual graph already loaded:" vg-name)
        existing-vg)
      ;; Load from nameservice
      (do
        (log/debug "Loading VG config from nameservice...")
        (let [vg-record (<? (load-vg-config-from-nameservice nameservice vg-name))
              _ (log/debug "VG record loaded:" vg-record)
              vg-config (vg-record->config vg-record)
              _ (log/debug "VG config parsed:" vg-config)
              vg-instance (create-vg-instance db vg-config)
              _ (log/debug "VG instance created, initializing...")
              ;; Initialize the VG with current db state
              initialized-vg (<? (vg/initialize vg-instance db))]
          (log/debug "VG initialized successfully")
          ;; Return the initialized VG instance directly
          initialized-vg)))))

;; Register BM25 implementation
#?(:clj
   (defmethod create-vg-impl :bm25
     [db vg-opts _vg-config]
     (bm25/new-bm25-index db [] vg-opts)))

;; R2RML implementation hook – returns a DB-like matcher that can push down whole GRAPH clauses.
#?(:clj
   (defmethod create-vg-impl :r2rml
     [_db vg-opts _vg-config]
     (r2rml-db/create vg-opts)))