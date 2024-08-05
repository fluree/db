(ns fluree.db.nameservice.ipns
  (:require [clojure.string :as str]
            [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.method.ipfs.directory :as ipfs-dir]
            [fluree.db.method.ipfs.keys :as ipfs-keys]
            [fluree.db.util.log :as log]))

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
  [ipfs-endpoint ipns-profile ledger-name opts]
  (go-try
    (let [ipns-address (if-let [[proto address ledger] (address-parts ledger-name)]
                         address
                         (<? (ipfs-keys/address ipfs-endpoint ipns-profile)))
          ipfs-address (str "/ipns/" ipns-address)
          ledgers      (<? (ipfs-dir/list-all ipfs-endpoint ipfs-address))]
      (log/debug "Looking up address for ledger:" ledger-name "all ledgers under ipns address are:" ledgers)
      (get ledgers ledger-name))))

(defn ipns-address
  "Returns IPNS address for a given ipns profile and ledger alias."
  [ipfs-endpoint ipns-profile ledger-alias opts]
  (go-try
    (log/debug "Getting address for ledger alias:" ledger-alias)
    (let [base-address (<? (ipfs-keys/address ipfs-endpoint ipns-profile))]
      (if base-address
        (str "fluree:ipns://" base-address "/" ledger-alias)
        (do
          (log/warn "Throwing exception for IPNS get-address as provided profile does not exist: " ipns-profile
                    ". IPNS profile keys found on server are: " (<? (ipfs-keys/list ipfs-endpoint)))
          (throw (ex-info (str "IPNS profile: " ipns-profile " does not appear on the server. "
                               "Therefore, unable to get address for ledger: " ledger-alias)
                          {:status 400 :error :db/ipns-profile})))))))

(defrecord IpnsNameService
  [ipfs-endpoint ipns-key base-address sync?]
  ns-proto/Publisher
  (-push [_ commit-data] (ipfs/push! ipfs-endpoint commit-data))

  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup-address ipfs-endpoint ipns-key ledger-alias nil))
  (-lookup [_ ledger-alias opts] (lookup-address ipfs-endpoint ipns-key ledger-alias opts))
  (-sync? [_] sync?)
  (-address [_ ledger-alias opts]
    (ipns-address ipfs-endpoint ipns-key ledger-alias opts))
  (-alias [_ ledger-address]
    (let [[_ _ alias] (address-parts ledger-address)]
      alias))
  (-close [_] true))

(defn initialize
  [ipfs-endpoint ipns-key]
  (go-try
    (let [base-address (<? (ipfs-keys/address ipfs-endpoint ipns-key))]
      (when-not base-address
        (throw (ex-info (str "IPNS publishing appears to have an issue. No corresponding ipns address found for key: "
                             ipns-key)
                        {:status 400 :error :db/ipfs-keys})))
      (map->IpnsNameService {:ipfs-endpoint ipfs-endpoint
                             :ipns-key      ipns-key
                             :base-address  base-address
                             :sync?         false}))))
