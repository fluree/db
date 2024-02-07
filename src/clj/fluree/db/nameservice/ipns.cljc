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


(defn address-exists?
  [ipfs-endpoint ledger-address]
  (go-try
    (log/debug "Checking for existence of ledger" ledger-address)
    (boolean
      (when-let [[proto address ledger] (address-parts ledger-address)]
        (let [ipfs-addr (if (= "ipns" proto)
                          (str "/ipns/" address)
                          address)
              ledgers   (<? (ipfs-dir/list-all ipfs-endpoint ipfs-addr))]
          (contains? ledgers ledger))))))

(defn lookup-address
  "Given IPNS address, performs lookup and returns latest ledger address."
  [ipfs-endpoint ledger-name opts]
  (go-try
    (if-let [[proto address ledger] (address-parts ledger-name)]
      (let [ipfs-addr (if (= "ipns" proto)
                        (str "/ipns/" address)
                        address)]
        ;; address might be a directory, or could directly be a commit file - try to look up as directory first
        (let [ledgers (<? (ipfs-dir/list-all ipfs-endpoint ipfs-addr))]
          (or (get ledgers ledger)
              ledger-name)))
      ledger-name)))

(defn get-address
  "Returns IPNS address for a given key."
  [ipfs-endpoint ipns-key ledger-alias opts]
  (go-try
    (log/debug "Getting address for ledger alias:" ledger-alias)
    (let [base-address (<? (ipfs-keys/address ipfs-endpoint ipns-key))]
      (str "fluree:ipns://" base-address "/" ledger-alias))))

(defrecord IpnsNameService
  [ipfs-endpoint ipns-key base-address sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-address] (lookup-address ipfs-endpoint ledger-address nil))
  (-lookup [_ ledger-address opts] (lookup-address ipfs-endpoint ledger-address opts))
  (-push [_ commit-data] (ipfs/push! ipfs-endpoint commit-data))
  (-subscribe [nameservice ledger-address callback] (throw (ex-info "Unsupported IpfsNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-address] (throw (ex-info "Unsupported IpfsNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [_ ledger-address] (address-exists? ipfs-endpoint ledger-address))
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported FileNameService op: ledgers" {})))
  (-address [_ ledger-alias opts]
    (get-address ipfs-endpoint ipns-key ledger-alias opts))
  (-alias [_ ledger-address]
    (let [[_ _ alias] (address-parts ledger-address)]
      alias))
  (-close [nameservice] true))

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
