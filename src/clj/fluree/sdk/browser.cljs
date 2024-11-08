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

(defn ^:export create
  ([conn] (fluree/create conn))
  ([conn ledger-alias] (fluree/create conn ledger-alias))
  ([conn ledger-alias opts] (fluree/create conn ledger-alias (js->clj opts :keywordize-keys true))))

(defn ^:export exists
  [conn alias-or-address]
  (fluree/exists? conn alias-or-address))

(defn ^:export load
  ([conn ledger-alias] (fluree/load conn ledger-alias)))

(defn ^:export stage
  ([db json-ld]
   (fluree/stage db (js->clj json-ld)))
  ([db json-ld opts]
   (fluree/stage db (js->clj json-ld)
                 (js->clj opts :keywordize-keys true))))

(defn ^:export commit
  ([ledger db] (.then (fluree/commit! ledger db)
                      (fn [result] (clj->js result))))
  ([ledger db opts] (.then (fluree/commit! ledger db
                                           (js->clj opts :keywordize-keys true))
                           (fn [result] (clj->js result)))))

(defn ^:export status
  ([ledger] (clj->js (fluree/status ledger)))
  ([ledger branch] (clj->js (fluree/status ledger branch))))

(defn ^:export db
  ([ledger] (fluree/db ledger))
  ([ledger opts] (fluree/db ledger (js->clj opts :keywordize-keys true))))

(defn ^:export query
  [db query]
  (let [query* (->> (js->clj query :keywordize-keys false)
                    (reduce-kv (fn [acc k v]
                                 (assoc acc (keyword k) v))
                               {}))]
    (.then (fluree/q db query*)
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
  #js {:commit          commit
       :connect         connect
       :create          create
       :db              db
       :exists          exists
       :load            load
       :query           query
       :setLogging      setLogging
       :stage           stage
       :status          status})
