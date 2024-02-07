(ns fluree.db.nameservice.filesystem
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    path))

(defn address-full-path
  [local-path address]
  (str local-path "/" (address-path address)))

(defn file-address
  "Turn a path or a protocol-relative URL into a fluree file address."
  [path]
  (if (str/starts-with? path "//")
    (str "fluree:file:" path)
    (str "fluree:file://" path)))

(defn address-path-exists?
  [local-path address]
  (->> address
       (address-full-path local-path)
       fs/exists?))

(defn read-address
  [local-path address]
  (->> address
       (address-full-path local-path)
       fs/read-file))

(defn address
  [ledger-alias {:keys [branch] :as _opts}]
  (let [branch (if branch (name branch) "main")]
    (go (file-address (str ledger-alias "/" branch "/head")))))

(defn push!
  "Just write to a different directory?"
  [local-path {commit-address :address
               nameservices   :ns}]
  (let [my-ns-iri   (some #(when (re-matches #"^fluree:file:.+" (:id %)) (:id %)) nameservices)
        commit-path (address-path commit-address)
        head-path   (address-path my-ns-iri)
        write-path  (str local-path "/" head-path)

        work        (fn [complete]
                      (log/debug (str "Updating head at " write-path " to " commit-path "."))
                      (fs/write-file write-path (bytes/string->UTF8 commit-path))
                      (complete (file-address head-path)))]
    #?(:clj  (let [p (promise)]
               (future (work (partial deliver p)))
               p)
       :cljs (js/Promise. (fn [resolve reject] (work resolve))))))


(defn lookup
  [local-path ledger-address {:keys [branch] :or {branch "main"} :as _opts}]
  (go-try
    (file-address (read-address local-path ledger-address))))


(defrecord FileNameService
  [local-path sync?]
  ns-proto/iNameService
  (-lookup [_ ledger-address] (lookup local-path ledger-address nil))
  (-lookup [_ ledger-address opts] (lookup local-path ledger-address opts))
  (-push [_ commit-data] (go (push! local-path commit-data)))
  (-subscribe [nameservice ledger-address callback] (throw (ex-info "Unsupported FileNameService op: subscribe" {})))
  (-unsubscribe [nameservice ledger-address] (throw (ex-info "Unsupported FileNameService op: unsubscribe" {})))
  (-sync? [_] sync?)
  (-exists? [nameservice ledger-address] (go (address-path-exists? local-path ledger-address)))
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
  [path]
  (let [local-path (fs/local-path path)]
    (map->FileNameService {:local-path local-path
                           :sync?      true})))
