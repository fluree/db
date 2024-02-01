(ns fluree.db.nameservice.filesystem
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.db.storage :as store]
            [fluree.db.storage.util :as store-util]))

#?(:clj (set! *warn-on-reflection* true))

(defn push!
  "Writes the commit address to the ledger's head."
  [store {commit-address :address nameservices :ns}]
  (let [my-ns-iri   (some #(when (re-matches #"^fluree:file:.+" (:id %)) (:id %)) nameservices)
        commit-path (:local (store-util/address-parts commit-address))
        head-path   (:local (store-util/address-parts my-ns-iri))

        work        (fn [complete]
                      (store/write store head-path (bytes/string->UTF8 commit-path))
                      (complete (store/address store head-path)))]
    #?(:clj  (let [p (promise)]
               (future (work (partial deliver p)))
               p)
       :cljs (js/Promise. (fn [resolve reject] (work resolve))))))

(defn head
  "The ledger's head address."
  [store ledger-alias {:keys [branch] :as _opts}]
  (let [branch (if branch (name branch) "main")]
    (store/address store (str ledger-alias "/" branch "/head"))))

(defn lookup
  "Return the head commit address."
  [store ledger-address]
  (go-try
    (let [commit-path (<? (store/read store ledger-address))]
      (store/address store commit-path))))

(defn address->alias
  [address]
  ;; TODO: need to validate that the branch doesn't have a slash?
  (-> (:local (store-util/address-parts address))
      (str/split #"/")
      (->> (drop-last 2)                ; branch-name, head
           (str/join #"/"))))

(defrecord FileNameService [store sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-address] (lookup store ledger-address))
  (-push [_ commit-data] (go (push! store commit-data)))
  (-subscribe [nameservice ledger-alias callback] (throw (ex-info "Unsupported FileNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-alias] (throw (ex-info "Unsupported FileNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [nameservice ledger-address] (store/exists? store ledger-address))
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported FileNameService op: ledgers" {})))
  (-address [_ ledger-alias opts]
    (go (head store ledger-alias opts)))
  (-alias [_ ledger-address] (address->alias ledger-address))
  (-close [nameservice] true))

(defn initialize
  [store]
  (map->FileNameService {:store store
                         :sync?  true}))
