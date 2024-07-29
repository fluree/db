(ns fluree.db.nameservice.core
  (:refer-clojure :exclude [exists?])
  (:require [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn nameservices
  [conn]
  (connection/-nameservices conn))

(defn relative-ledger-alias?
  [ledger-alias]
  (not (str/starts-with? ledger-alias "fluree:")))

(defn ns-address
  "Returns async channel"
  [nameservice ledger-alias branch]
  (ns-proto/-address nameservice ledger-alias {:branch branch}))

(defn addresses
  "Retrieve address for each nameservices based on a relative ledger-alias.
  If ledger-alias is not relative, returns only the current ledger alias.

  TODO - if a single non-relative address is used, and the ledger exists,
  we should retrieve all stored ns addresses in the commit if possible and
  try to use all nameservices."
  [conn ledger-alias {:keys [branch] :or {branch "main"} :as _opts}]
  (go-try
    (if (relative-ledger-alias? ledger-alias)
      (let [nameservices (nameservices conn)]
        (when-not (and (sequential? nameservices)
                       (> (count nameservices) 0))
          (throw (ex-info "No nameservices configured on connection!"
                          {:status 500 :error :db/invalid-nameservice})))
        (loop [nameservices* nameservices
               addresses     []]
          (let [ns (first nameservices*)]
            (if ns
              (if-let [address (<? (ns-address ns ledger-alias branch))]
                (recur (rest nameservices*) (conj addresses address))
                (recur (rest nameservices*) addresses))
              addresses))))
      [ledger-alias])))

(defn primary-address
  "From a connection, lookup primary address from
  nameservice(s) for a given ledger alias"
  [conn ledger-alias opts]
  (go-try
    (first (<? (addresses conn ledger-alias opts)))))

(defn push!
  "Executes a push operation to all nameservices registered on the connection."
  [conn json-ld-commit]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (let [sync? (ns-proto/-sync? ns)]
            (if sync?
              (<? (ns-proto/-push ns json-ld-commit))
              (ns-proto/-push ns json-ld-commit))
            (recur (rest nameservices*))))))))

(defn lookup-commit
  "Returns commit address from first matching nameservice on a conn
   for a given ledger alias and branch"
  [conn ledger-address]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (let [commit-address (<? (ns-proto/-lookup ns ledger-address))]
            (if commit-address
              commit-address
              (recur (rest nameservices*)))))))))

(defn read-latest-commit
  [conn resource-address]
  (go-try
    (let [commit-addr (<? (lookup-commit conn resource-address))
          _           (when-not commit-addr
                        (throw (ex-info (str "Unable to load. No commit exists for: " resource-address)
                                        {:status 400 :error :db/invalid-commit-address})))
          commit-data (<? (connection/-c-read conn commit-addr))]
      (assoc commit-data "address" commit-addr))))

(defn file-read?
  [address]
  (str/ends-with? address ".json"))

(defn read-resource
  [conn resource-address]
  (if (file-read? resource-address)
    (connection/-c-read conn resource-address)
    (read-latest-commit conn resource-address)))

(defn exists?
  "Checks nameservices on a connection and returns true
  if any nameservice already has a ledger associated with
  the given alias."
  [conn ledger-alias]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (if-let [ns (first nameservices*)]
          (let [exists? (<? (ns-proto/-lookup ns ledger-alias))]
            (if exists?
              true
              (recur (rest nameservices*))))
          false)))))

(defn subscribe-ledger
  "Initiates subscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (nameservices conn)
        callback     (fn [msg]
                       (log/info "Subscription message received: " msg)
                       (let [action       (get msg "action")
                             ledger-alias (get msg "ledger")
                             data         (get msg "data")]
                         (if (= "new-commit" action)
                           (connection/notify-ledger conn data)
                           (log/info "New subscritipn message with action: " action "received, ignored."))))]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (ns-proto/-subscribe ns ledger-alias callback))
          (recur (rest nameservices*)))))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (ns-proto/-unsubscribe ns ledger-alias))
          (recur (rest nameservices*)))))))
