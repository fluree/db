(ns fluree.sdk.node
  (:require [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]))

(defn ^:export connect
  [opts]
  (fluree/connect (js->clj opts :keywordize-keys false)))

(defn ^:export connectMemory
  [opts]
  (fluree/connect-memory (js->clj opts :keywordize-keys true)))

(defn ^:export connectFile
  [opts]
  (fluree/connect-file (js->clj opts :keywordize-keys true)))

(defn ^:export create
  ([conn ledger-alias] (fluree/create conn ledger-alias))
  ([conn ledger-alias opts] (fluree/create conn ledger-alias
                                           (js->clj opts :keywordize-keys true))))

(defn ^:export exists
  [conn alias-or-address]
  (fluree/exists? conn alias-or-address))

(defn ^:export load
  [conn ledger-alias]
  (fluree/load conn ledger-alias))

(defn ^:export stage
  ([db json-ld]
   (fluree/update db (js->clj json-ld)))
  ([db json-ld opts]
   (fluree/update db (js->clj json-ld)
                  (-> opts
                      (js->clj :keywordize-keys true)))))

(defn ^:export commit
  ([conn db] (fluree/commit! conn db))
  ([conn db opts] (fluree/commit! conn db
                                  (js->clj opts :keywordize-keys true))))

(defn ^:export status
  [conn ledger-id]
  (clj->js (fluree/status conn ledger-id)))

(defn ^:export db
  [conn ledger-id]
  (fluree/db conn ledger-id))

(defn ^:export query
  [db query]
  (let [query* (->> (js->clj query :keywordize-keys false)
                    (reduce-kv (fn [acc k v]
                                 (assoc acc (if (str/starts-with? k "@")
                                              k
                                              (keyword k)) v))
                               {}))]
    (.then (fluree/query db query*)
           (fn [result] (clj->js result)))))

(log/set-level! :warning)

(defn ^:export setLogging
  "Configure logging for Fluree processes.  Supported options:
  1. level [Values: severe, warning, info, config, fine, finer, finest]
  "
  [opts]
  (let [opts' (js->clj opts :keywordize-keys true)
        {:keys [level]} opts']
    (log/set-level! (keyword level))))
