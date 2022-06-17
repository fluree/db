(ns fluree.db.method.ipfs.push
  (:require [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.method.ipfs.xhttp :as ipfs]
            [fluree.db.method.ipfs.directory :as ipfs-dir]
            [fluree.db.method.ipfs.keys :as ipfs-key]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.log :as log]))

;; push involves (a) creating an IPFS directory stricture to maintain multiple ledgers
;; under the same namespace and (b) updating the IPNS address to point to the new directory

#?(:clj (set! *warn-on-reflection* true))

(defn address-parts
  "Parses full ipns ledger address and returns parts"
  [address]
  (let [[_ ipns-address relative-address] (re-find #"^fluree:ipns://([^/]+)/(.+)$" address)]
    {:ipns-address     ipns-address
     :relative-address relative-address}))



(defn push!
  "Publishes ledger metadata document (ledger root) to IPFS and recursively updates any
  directory files, culminating in an update to the IPNS address."
  [{:keys [conn address] :as _ledger} ipfs-commit-map]
  (go-try
    (let [{:keys [meta t dbid ledger-state]} ipfs-commit-map
          {:keys [hash size]} meta
          {:keys [ipns-address relative-address]} (address-parts address)
          ipfs-endpoint    (:ipfs-endpoint conn)
          current-dag-map  (async/<! (ipfs-dir/refresh-state ipfs-endpoint (str "/ipns/" ipns-address)))
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
