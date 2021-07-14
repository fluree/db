(ns fluree.db.query.transaction
  (:require [clojure.core.async :as async :refer [<! go]]
            [clojure.tools.logging :as log]
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.core :as util :refer [catch* try*]]
            [fluree.db.util.async :refer [<? go-try]]))

(defn- authorize
  [{:keys [network dbid] :as db} flakes]
  (go
   (if (-> db :permissions :root?)
     flakes
     (let [allowed (<! (perm-validate/allow-flakes? db flakes))]
       (if-not (util/exception? allowed)
         allowed
         (do (log/error allowed "Error validating transaction permissions for:"
                        network dbid)
             []))))))

(defn lookup
  [{:keys [conn network dbid] :as db} txid]
  (go
    (let [res (<! (storage/read-transaction conn network dbid txid))]
      (if-not (or (nil? res) (util/exception? res))
        (let [{:keys [flakes]} res
              allowed-flakes   (<! (authorize db flakes))
              tx-map           (select-keys res [:t :block])]
          (assoc tx-map :flakes allowed-flakes))
        (log/error res "Error reading transaction" txid "from storage for"
                   network dbid)))))
