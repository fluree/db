(ns fluree.sdk.node
  (:require [cljs.nodejs :as node-js]
            [clojure.string :as str]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(node-js/enable-util-print!)

(defn ^:export connect
  [opts]
  (fluree/connect (js->clj opts :keywordize-keys true)))

(defn ^:export create
  ([conn] (fluree/create conn))
  ([conn ledger-alias] (fluree/create conn ledger-alias))
  ([conn ledger-alias opts] (fluree/create conn ledger-alias
                                           (js->clj opts :keywordize-keys true))))

(defn ^:export exists
  [conn alias-or-address]
  (fluree/exists? conn alias-or-address))

(defn ^:export loadFromAddress
  ([address] (fluree/load-from-address address))
  ([conn address] (fluree/load-from-address conn address)))

(defn ^:export load
  [conn ledger-alias]
  (fluree/load conn ledger-alias))

(defn ^:export stage
  ([db-or-ledger json-ld]
   (fluree/stage db-or-ledger (js->clj json-ld) {:context-type :string}))
  ([db-or-ledger json-ld opts]
   (fluree/stage db-or-ledger (js->clj json-ld)
                 (-> opts
                     (js->clj :keywordize-keys true)
                     (assoc :context-type :string)))))

(defn ^:export commit
  ([ledger db] (fluree/commit! ledger db))
  ([ledger db opts] (fluree/commit! ledger db
                                    (js->clj opts :keywordize-keys true))))

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
                                 (assoc acc (if (str/starts-with? k "@")
                                              k
                                              (keyword k)) v))
                               {}))]
    (.then (fluree/query db (assoc-in query* [:opts :context-type] :string))
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
