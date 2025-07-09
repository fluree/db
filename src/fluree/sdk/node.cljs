(ns fluree.sdk.node
  (:require ["@peculiar/webcrypto" :refer [Crypto]]
            [cljs.nodejs :as node-js]
            [clojure.string :as str]
            [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]))

(set! js/crypto (Crypto.))

(node-js/enable-util-print!)

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
  ([ledger db] (fluree/commit! ledger db))
  ([ledger db opts] (fluree/commit! ledger db
                                    (js->clj opts :keywordize-keys true))))

(defn ^:export status
  ([ledger] (clj->js (fluree/status ledger)))
  ([ledger branch] (clj->js (fluree/status ledger branch))))

(defn ^:export db
  [ledger]
  (fluree/db ledger))

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
