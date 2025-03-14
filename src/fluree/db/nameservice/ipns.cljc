(ns fluree.db.nameservice.ipns
  (:require [clojure.string :as str]
            [fluree.db.method.ipfs :as ipfs]
            [fluree.db.method.ipfs.directory :as ipfs-dir]
            [fluree.db.method.ipfs.keys :as ipfs-keys]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
            [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn trim-slashes
  "Trims any leading or following slash '/' characters from string"
  [s]
  (when s
    (cond-> s
      (str/ends-with? s "/") (subs 0 (dec (count s)))
      (str/starts-with? s "/") (subs 1))))

(defn address-parts
  "Returns three-tuple of ipfs/ipns (protocol), address, and ledger alias(directory)
  If no match, returns nil.
  e.g. fluree:ipfs://QmZ9FQA7eHnnuTV5kjiaQKPf99NSPzk2pi1AMe6XkDa2P2
       ->> [QmZ9FQA7eHnnuTV5kjiaQKPf99NSPzk2pi1AMe6XkDa2P2 nil]
       fluree:ipns://bafybeibtk2qwvuvbawhcgrktkgbdfnor4qzxitk4ct5mfwmvbaao53awou/my/db
       ->> [bafybeibtk2qwvuvbawhcgrktkgbdfnor4qzxitk4ct5mfwmvbaao53awou my/db]"
  [address]
  (when-let [[_ proto address db] (re-find #"^fluree:([^:]+)://([^/]+)(/.+)?$" address)]
    [proto address (trim-slashes db)]))

(defn lookup-address
  "Given IPNS address, performs lookup and returns latest ledger address."
  [ipfs-endpoint ipns-key ledger-name]
  (go-try
    (let [ipns-address (if-let [[_proto address _ledger] (address-parts ledger-name)]
                         address
                         (<? (ipfs-keys/address ipfs-endpoint ipns-key)))
          ipfs-address (str "/ipns/" ipns-address)
          ledgers      (<? (ipfs-dir/list-all ipfs-endpoint ipfs-address))]
      (log/debug "Looking up address for ledger:" ledger-name "all ledgers under ipns address are:" ledgers)
      (get ledgers ledger-name))))

(defn ipns-address
  "Returns IPNS address for a given ipns key and ledger alias."
  [ipfs-endpoint ipns-key ledger-alias]
  (go-try
    (log/debug "Getting address for ledger alias:" ledger-alias)
    (if-let [base-address (<? (ipfs-keys/address ipfs-endpoint ipns-key))]
      (str "fluree:ipns://" base-address "/" ledger-alias)
      (do
        (log/warn "Failed to retrieve IPNS address because provided key" ipns-key
                  "does not exist on the server.")
        (throw (ex-info (str "Unable to get address for ledger: " ledger-alias ". "
                             "IPNS key: " ipns-key " does not exist on the server.")
                        {:status 400, :error :db/missing-ipns-key}))))))

(defrecord IpnsNameService [ipfs-endpoint ipns-key]
  nameservice/Publisher
  (publish [_ commit-jsonld]
    (ipfs/push! ipfs-endpoint commit-jsonld))
  (publishing-address [_ ledger-alias]
    (ipns-address ipfs-endpoint ipns-key ledger-alias))

  nameservice/iNameService
  (lookup [_ ledger-alias]
    (when-let [address (lookup-address ipfs-endpoint ipns-key ledger-alias)]
      (ipfs/read ipfs-endpoint address)))
  (alias [_ ledger-address]
    (let [[_ _ alias] (address-parts ledger-address)]
      alias)))

(defn initialize
  [ipfs-endpoint ipns-key]
  (map->IpnsNameService {:ipfs-endpoint ipfs-endpoint
                         :ipns-key      ipns-key}))
