(ns fluree.db.conn.fn
  (:require [clojure.core.async :refer [go]]
            [fluree.db.index :as index]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.log :as log]
            [fluree.db.conn.cache :as conn-cache]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.conn.state-machine :as state-machine]))

(defn- ctx-fn
  "Invokes function f with a Fluree context arg built from conn and req (in the
  :conn and :request keys, respectively) and returns the result."
  [f conn req]
  (let [res (f {:request req, :fluree/conn conn})]
    (log/debug "ctx-fn result:" (pr-str res))
    res))

(defrecord FnConnection [id f ledger-defaults state async-cache]
  conn-proto/iNameService
  (-pull [conn ledger]
    (ctx-fn f conn {:cmd  ::conn-proto/-pull
                    :args {:ledger ledger}}))
  (-subscribe [conn ledger]
    (ctx-fn f conn {:cmd  ::conn-proto/-subscribe
                    :args {:ledger ledger}}))
  (-alias [conn {:keys [ledger-address]}]
    (ctx-fn f conn {:cmd  ::conn-proto/-alias
                    :args {:ledger-address ledger-address}}))
  (-push [conn head-path commit-data]
    (go (ctx-fn f conn {:cmd  ::conn-proto/-push
                        :args {:head-path   head-path
                               :commit-data commit-data}})))
  (-lookup [conn {:keys [head-commit-address]}]
    (go (ctx-fn f conn {:cmd  ::conn-proto/-lookup
                        :args {:head-address head-commit-address}})))
  (-address [conn params]
    (go (ctx-fn f conn {:cmd  ::conn-proto/-address
                        :args params})))
  (-exists? [conn params]
    (go (ctx-fn f conn {:cmd  ::conn-proto/-exists?
                        :args params})))

  conn-proto/iLedger
  (-create [conn args]
    (go (ctx-fn f conn
                {:cmd ::conn-proto/-create, :args args})))
  (-load [conn args]
    (go (ctx-fn f conn {:cmd ::conn-proto/-load, :args args})))
  (-load-from-address [conn args]
    (go (ctx-fn f conn
                {:cmd ::conn-proto/-load-from-address, :args args})))

  conn-proto/iStorage
  (-c-read [conn commit-key]
    (go (ctx-fn f conn {:cmd  ::conn-proto/-read-commit
                        :args {:commit-key commit-key}})))
  (-c-write [conn commit-data]
    (go
      (ctx-fn f conn {:cmd  ::conn-proto/-commit
                      :args {:commit-data commit-data}})))
  (-c-write [conn db commit-data]
    (go
      (ctx-fn f conn {:cmd  ::conn-proto/-commit
                      :args {:db          db
                             :commit-data commit-data}})))

  conn-proto/iConnection
  (-close [_]
    (log/info "Closing fn connection" id)
    (swap! state assoc :closed? true))
  (-closed? [_] (-> @state :closed? boolean))
  (-method [_] :fn)
  (-parallelism [_] 1) ; TODO: need to think through this one a bit...
  (-id [_] id)
  (-context [_] (:context ledger-defaults))
  (-new-indexer [_ _] nil) ; TODO: Does this make sense to have here?
  (-did [_] (:did ledger-defaults))
  (-msg-in [_ _] (throw (ex-info "Unsupported FnConnection op: msg-in" {})))
  (-msg-out [_ _] (throw (ex-info "Unsupported FnConnection op: msg-out" {})))
  (-state [_] @state)
  (-state [_ ledger] (get @state ledger))

  index/Resolver
  (resolve [_conn node] (storage/resolve-empty-leaf node)))

(defn connect
  "Create a new function callback connection."
  [{:keys [defaults async-cache memory] f :fn :as _opts}]
  (go
    (let [conn-id        (str (random-uuid))
          state          (state-machine/blank-state)
          async-cache-fn (or async-cache
                             (conn-cache/default-async-cache-fn memory))]
      (map->FnConnection {:id              conn-id
                          :f               f
                          :ledger-defaults defaults
                          :state           state
                          :async-cache     async-cache-fn}))))

