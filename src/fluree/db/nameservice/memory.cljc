(ns fluree.db.nameservice.memory
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [clojure.core.async :as async :refer [go <!]]
            [clojure.string :as str]
            [fluree.db.platform :as platform]
            [fluree.db.util.log :as log]
            [fluree.db.storage :as store]
            [fluree.db.storage.util :as store-util]))

#?(:clj (set! *warn-on-reflection* true))

(defn memory-address
  "Turn a path into a fluree memory address."
  [path]
  (str "fluree:memory://" path))

(defn- address-path
  "Returns the path portion of a Fluree memory address."
  [address]
  (if-let [[_ path] (re-find #"^fluree:memory://(.+)$" address)]
    path
    (throw (ex-info (str "Incorrectly formatted Fluree memory db address: " address)
                    {:status 500 :error :db/invalid-db}))))

(defn push!
  [store {commit-address   :address
          nameservice-iris :ns
          :as              commit-data}]
  (go
    (let [my-ns-iri   (some #(when (re-matches #"^fluree:memory:(.+)" (:id %)) (:id %)) nameservice-iris)
          head-path   (address-path my-ns-iri)

          commit (<! (store/read store commit-address))
          _      (when-not commit
                   (throw (ex-info (str "Unable to locate commit in memory, cannot push!: " commit-address)
                                   {:status 500 :error :db/invalid-db})))
          commit* (assoc commit "address" commit-address)]
      (<! (store/write store head-path commit*))
      commit-data)))

(defn lookup
  [store ledger-alias opts]
  (go (if-let [head-commit (<! (store/read store ledger-alias))]
        (-> head-commit (get "address"))
        (throw (ex-info (str "Unable to lookup ledger address from conn: "
                             ledger-alias)
                        {:status 500 :error :db/missing-head})))))

(defn ledger-list
  [store opts]
  (go
    (->> (<! (store/list store ""))
         (filter #(and (string? %)
                       (str/ends-with? % "head"))))))

(defn address
  [ledger-alias {:keys [branch] :as _opts}]
  (go (memory-address (str ledger-alias "/" (name branch) "/head"))))

(defrecord MemoryNameService
  [store sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup store ledger-alias nil))
  (-lookup [_ ledger-alias opts] (lookup store ledger-alias opts))
  (-push [_ commit-data] (push! store commit-data))
  (-subscribe [nameservice ledger-alias callback] (throw (ex-info "Unsupported MemoryNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-alias] (throw (ex-info "Unsupported MemoryNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [_ ledger-address] (go (boolean (<! (store/read store ledger-address)))))
  (-ledgers [_ opts] (ledger-list store opts))
  (-address [_ ledger-alias opts]
    (address ledger-alias opts))
  (-alias [_ ledger-address]
    (-> (address-path ledger-address)
        (str/split #"/")
        (->> (drop 2)
             (str/join "/"))))
  (-close [nameservice] (reset! store {})))


(defn initialize
  [store]
  (map->MemoryNameService {:store store
                           :sync? true}))
