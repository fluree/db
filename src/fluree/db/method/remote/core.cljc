(ns fluree.db.method.remote.core
  (:require [fluree.db.util.xhttp :as xhttp]
            [fluree.db.util.log :as log]))


(defn pick-server
  "Currently does just a round-robin selection if multiple servers are given.
  TODO - add re-tries with a different server if fails to connect. Consider keeping stats to select optimal server."
  [servers]
  (rand-nth servers))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [state servers commit-key keywordize-keys?]
  (log/debug "[remote conn] remote read initiated for: " commit-key)
  (xhttp/post-json (str (pick-server servers) "/fluree/remoteResource")
                   {:resource commit-key}
                   {:keywordize-keys keywordize-keys?}))

;; NOTE, below function works in conjunction with message broadcasting (not in current PR)
#_(defn remote-read
    "Returns a core async channel with value of remote resource."
    [state servers commit-key keywordize-keys?]
    (log/debug "[remote conn] remote read initiated for: " commit-key)
    (if-let [cached (get-in @state [:resource commit-key])]
      (go cached)
      (xhttp/post-json (str (pick-server servers) "/fluree/remoteResource")
                       {:resource commit-key}
                       {:keywordize-keys keywordize-keys?})))


;; NOTE, below function works in conjunction with message broadcasting (not in current PR)
#_(defn remote-lookup
    [state servers ledger-address]
    (go-try
      (or (get-in @state [:lookup ledger-address])
          (let [head-commit  (<? (remote-read state servers ledger-address false))
                head-address (get head-commit "address")]
            (swap! state assoc-in [:lookup ledger-address] head-address)
            (swap! state assoc-in [:resource head-address] head-commit)
            head-address))))