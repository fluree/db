(ns fluree.sdk.browser
  (:require [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.sdk.version :refer [version]]))

(enable-console-print!)

;; define your app data so that it doesn't get over-written on reload
(defonce app-state (atom (assoc (version) :product "Fluree browser SDK")))

(println (:product @app-state) (:version @app-state))

;; optionally touch your app-state to force rerendering depending on
;; your application
;; (swap! app-state update-in [:__figwheel_counter] inc)
(defn on-js-reload [])

;; ----------------------------------------
;; JSON-LD
;; ----------------------------------------

(defn ^:export connect
  [opts]
  (fluree/connect (js->clj opts :keywordize-keys false)))

(defn ^:export connectMemory
  [opts]
  (fluree/connect-memory (js->clj opts :keywordize-keys true)))

(defn ^:export connectLocalStorage
  [opts]
  (let [opts* (js->clj opts :keywordize-keys true)
        storage-id (or (:storage-id opts*) "fluree-db")
        config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                            "@vocab" "https://ns.flur.ee/system#"}
                "@id"      "localStorage"
                "@graph"   [{"@id"          "localStorageStorage"
                             "@type"        "Storage"
                             "storageType"  "localstorage"
                             "identifier"   storage-id}
                            {"@id"              "connection"
                             "@type"            "Connection"
                             "parallelism"      (or (:parallelism opts*) 4)
                             "cacheMaxMb"       (or (:cache-max-mb opts*) 100)
                             "commitStorage"    {"@id" "localStorageStorage"}
                             "indexStorage"     {"@id" "localStorageStorage"}
                             "primaryPublisher" {"@type"   "Publisher"
                                                 "storage" {"@id" "localStorageStorage"}}}]}
        config* (if-let [defaults (:defaults opts*)]
                  (assoc-in config ["@graph" 1 "defaults"] defaults)
                  config)]
    (fluree/connect config*)))

(defn ^:export create
  ([conn ledger-alias] (fluree/create conn ledger-alias))
  ([conn ledger-alias opts] (fluree/create conn ledger-alias (js->clj opts :keywordize-keys true))))

(defn ^:export exists
  [conn alias-or-address]
  (fluree/exists? conn alias-or-address))

(defn ^:export load
  ([conn ledger-alias] (fluree/load conn ledger-alias)))

(defn ^:export stage
  ([db json-ld]
   (fluree/update db (js->clj json-ld)))
  ([db json-ld opts]
   (fluree/update db (js->clj json-ld)
                  (js->clj opts :keywordize-keys true))))

(defn ^:export commit
  ([conn db] (fluree/commit! conn db))
  ([conn db opts] (.then (fluree/commit! conn db
                                         (js->clj opts :keywordize-keys true))
                         (fn [result]
                           (if (map? result)
                             ;; If result is a map with :db key, handle it specially
                             (let [db-val (:db result)
                                   js-result (-> result
                                                 (dissoc :db)
                                                 clj->js)]
                               (aset js-result "db" db-val)
                               js-result)
                             ;; Otherwise just return the db as-is
                             result)))))

(defn ^:export status
  [conn ledger-id]
  (clj->js (fluree/status conn ledger-id)))

(defn ^:export db
  [conn ledger-id]
  (fluree/db conn ledger-id))

(defn ^:export query
  [db query]
  (let [query* (js->clj query :keywordize-keys false)]
    (.then (fluree/query db query*)
           (fn [result] (clj->js result)))))

;; ======================================
;;
;; Support logging at different levels
;;
;; ======================================
(log/set-level! :warning) ;; default to log only warnings or errors
;(def ^:export logging-levels log/levels)

(defn ^:export setLogging
  "Configure logging for Fluree processes.  Supported options:
  1. level [Values: severe, warning, info, config, fine, finer, finest]
  "
  [opts]
  (let [opts' (js->clj opts :keywordize-keys true)
        {:keys [level]} opts']
    (log/set-level! (keyword level))))

(def ^:export fluree-browser-sdk
  #js {:commit               commit
       :connect              connect
       :connectMemory        connectMemory
       :connectLocalStorage  connectLocalStorage
       :create               create
       :db                   db
       :exists               exists
       :load                 load
       :query                query
       :setLogging           setLogging
       :stage                stage
       :status               status})
