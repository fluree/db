(ns fluree.db.nameservice.filesystem
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.util.log :as log]
            [fluree.db.storage :as store]))

#?(:clj (set! *warn-on-reflection* true))

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    path))

(defn file-address
  "Turn a path or a protocol-relative URL into a fluree file address."
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

(defn address-path-exists?
  [store address]
  (store/exists? store address))

(defn read-address
  [store address]
  (store/read store address))

(defn address
  [ledger-alias {:keys [branch] :as _opts}]
  (let [branch (if branch (name branch) "main")]
    (go (file-address (str ledger-alias "/" branch "/head")))))

(defn push!
  "Just write to a different directory?"
  [store {commit-address :address nameservices :ns}]
  (let [my-ns-iri   (some #(when (re-matches #"^fluree:file:.+" (:id %)) (:id %)) nameservices)
        commit-path (address-path commit-address)
        head-path   (address-path my-ns-iri)

        work        (fn [complete]
                      (store/write store head-path (bytes/string->UTF8 commit-path))
                      (complete (file-address head-path)))]
    #?(:clj  (let [p (promise)]
               (future (work (partial deliver p)))
               p)
       :cljs (js/Promise. (fn [resolve reject] (work resolve))))))


(defn lookup
  [store ledger-alias {:keys [branch] :or {branch "main"} :as _opts}]
  (go-try
    (file-address (read-address store ledger-alias))))


(defrecord FileNameService
  [store sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup store ledger-alias nil))
  (-lookup [_ ledger-alias opts] (lookup store ledger-alias opts))
  (-push [_ commit-data] (go (push! store commit-data)))
  (-subscribe [nameservice ledger-alias callback] (throw (ex-info "Unsupported FileNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-alias] (throw (ex-info "Unsupported FileNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [nameservice ledger-address] (go (address-path-exists? store ledger-address)))
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported FileNameService op: ledgers" {})))
  (-address [_ ledger-alias opts]
    (address ledger-alias opts))
  (-alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (address-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/"))))
  (-close [nameservice] true))


(defn initialize
  [store]
  (map->FileNameService {:store store
                         :sync?  true}))
