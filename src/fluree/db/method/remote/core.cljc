(ns fluree.db.method.remote.core
  (:require [fluree.db.util.xhttp :as xhttp]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))


(defn pick-server
  "Currently does just a round-robin selection if multiple servers are given.
  TODO - add re-tries with a different server if fails to connect. Consider keeping stats to select optimal server."
  [server-state]
  (or (:connected-to @server-state)
      (-> (swap! server-state (fn [{:keys [connected-to servers] :as ss}]
                                (if connected-to
                                  connected-to
                                  (let [chosen-server (rand-nth servers)]
                                    (assoc ss :connected-to chosen-server
                                              :secure? (str/starts-with? chosen-server "https")
                                              :connected-at (util/current-time-millis))))))
          :connected-to)))

(defn remote-read
  "Returns a core async channel with value of remote resource."
  [state server-state commit-key keywordize-keys?]
  (log/debug "[remote conn] remote read initiated for: " commit-key)
  (let [server-host (pick-server server-state)]
    (xhttp/post-json (str server-host "/fluree/remoteResource")
                     {:resource commit-key}
                     {:keywordize-keys keywordize-keys?})))

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