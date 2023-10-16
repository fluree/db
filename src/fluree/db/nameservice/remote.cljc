(ns fluree.db.nameservice.remote
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.method.remote.core :as remote]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn remote-lookup
  [state server-state ledger-address opts]
  (go-try
    (let [head-commit  (<? (remote/remote-read state server-state ledger-address false))
          head-address (get head-commit "address")]
      head-address)))

(defn remote-ledger-exists?
  [state server-state ledger-address]
  (go-try
    (boolean
      (<? (remote-lookup state server-state ledger-address nil)))))

(defrecord RemoteNameService
  [state server-state sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-alias] (remote-lookup state server-state ledger-alias nil))
  (-lookup [_ ledger-alias opts] (remote-lookup state server-state ledger-alias opts))
  (-push [_ commit-data] (throw (ex-info "Unsupported RemoteNameService op: push" {})))
  (-subscribe [nameservice ledger-alias callback] (throw (ex-info "Unsupported RemoteNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-alias] (throw (ex-info "Unsupported RemoteNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [_ ledger-address] (remote-ledger-exists? state server-state ledger-address))
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported RemoteNameService op: ledgers" {})))
  (-address [_ ledger-alias {:keys [branch] :or {branch :main} :as _opts}]
    (go (str ledger-alias "/" (name branch) "/head")))
  (-alias [_ ledger-address]
    ledger-address)
  (-close [nameservice] true))

(defn initialize
  [server-state state-atom]
  (map->RemoteNameService {:server-state server-state
                           :state        (or state-atom (atom nil))
                           :sync?        true}))
