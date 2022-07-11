(ns fluree.db.method.ipfs.core
  (:require [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.method.ipfs.directory :as ipfs-dir]
            [fluree.db.method.ipfs.keys :as ipfs-key]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn default-commit-fn
  "Default push function for IPFS"
  [ipfs-endpoint]
  (fn [data]
    (go-try
      (let [json (json-ld/normalize-data data)
            res  (<? (ipfs/add ipfs-endpoint json))
            {:keys [name]} res]
        (when-not name
          (throw (ex-info (str "IPFS publish error, unable to retrieve IPFS name. Response object: " res)
                          {:status 500 :error :db/push-ipfs})))
        (assoc res :address (str "fluree:ipfs://" name))))))


(defn default-read-fn
  "Default reading function for IPFS. Reads either IPFS or IPNS docs"
  [ipfs-endpoint]
  (fn [file-key]
    (when-not (string? file-key)
      (throw (ex-info (str "Invalid file key, cannot read: " file-key)
                      {:status 500 :error :db/invalid-commit})))
    (let [[address path] (str/split file-key #"://")
          [type method] (str/split address #":")
          ipfs-cid (str "/" method "/" path)]
      (when-not (and (= "fluree" type)
                     (#{"ipfs" "ipns"} method))
        (throw (ex-info (str "Invalid file type or method: " file-key)
                        {:status 500 :error :db/invalid-commit})))
      (ipfs/cat ipfs-endpoint ipfs-cid))))


(defn address-parts
  "Parses full ipns ledger address and returns parts"
  [address]
  (let [[_ ipns-address relative-address] (re-find #"^fluree:ipns://([^/]+)/(.+)$" address)]
    {:ipns-address     ipns-address
     :relative-address relative-address}))


(defn push!
  "Publishes ledger metadata document (ledger root) to IPFS and recursively updates any
  directory files, culminating in an update to the IPNS address."
  [conn address ipfs-commit-map]
  (go-try
    (let [{:keys [meta t dbid ledger-state]} ipfs-commit-map
          {:keys [hash size]} meta
          {:keys [ipns-address relative-address]} (address-parts address)
          ipfs-endpoint    (:ipfs-endpoint conn)
          current-dag-map  (<? (ipfs-dir/refresh-state ipfs-endpoint (str "/ipns/" ipns-address)))
          updated-dir-map  (<? (ipfs-dir/update-directory! current-dag-map ipfs-endpoint relative-address hash size))
          _                (swap! ledger-state update-in [:push :pending] (fn [m] (assoc m :t t :dag updated-dir-map)))
          ipns-key         (<? (ipfs-key/key ipfs-endpoint ipns-address))
          _                (when-not ipns-key
                             (throw (ex-info (str "IPNS key for address: " ipns-address " appears to no longer be registered "
                                                  "with the connected IPFS server: " ipfs-endpoint ". Unable to publish updates.")
                                             {:status 500 :error :db/ipns})))
          publish-response (<? (ipfs/publish ipfs-endpoint (:hash updated-dir-map) ipns-key))]
      (when (not= (:name publish-response) ipns-address)
        (log/warn "IPNS address for key " ipns-key " used to be: " ipns-address
                  " but now is resolving to: " (:name publish-response) "."
                  "Publishing is now happening to the new address."))
      ;; update ledger state with new push event
      (swap! ledger-state update :push (fn [{:keys [pending complete] :as m}]
                                         (let [pending*  (if (= t (:t pending))
                                                           {:t nil :dag nil}
                                                           (do
                                                             (log/info "IPNS publishing is slower than your commits and will have delays.")
                                                             pending))
                                               complete* (if (> t (:t complete))
                                                           {:t t :dag (:hash updated-dir-map)}
                                                           complete)]
                                           (assoc m :pending pending*
                                                    :complete complete*))))

      updated-dir-map)))
