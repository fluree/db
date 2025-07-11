(ns fluree.db.nameservice
  (:refer-clojure :exclude [alias])
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iNameService
  (lookup [nameservice ledger-address]
    "Performs lookup operation on ledger alias and returns map of latest commit
    and other metadata")
  (alias [nameservice ledger-address]
    "Given a ledger address, returns ledger's default alias name else nil, if
    not avail"))

(defprotocol Publisher
  (publish [publisher commit-jsonld]
    "Publishes new commit.")
  (retract [publisher ledger-alias]
    "Remove the nameservice record for the ledger.")
  (publishing-address [publisher ledger-alias]
    "Returns full publisher address/iri which will get published in commit. If
    'private', return `nil`."))

(defprotocol Publication
  (subscribe [publication ledger-alias]
    "Creates a subscription to publication for ledger events. Will call
    callback with event data as received.")
  (unsubscribe [publication ledger-alias]
    "Unsubscribes to publication for ledger events")
  (known-addresses [publication ledger-alias]))

(defn publish-to-all
  [commit-jsonld publishers]
  (->> publishers
       (map (fn [ns]
              (go
                (try*
                  (<? (publish ns commit-jsonld))
                  (catch* e
                    (log/warn e "Publisher failed to publish commit")
                    ::publishing-error)))))
       async/merge))

(defn published-ledger?
  [nsv ledger-alias]
  (go-try
    (let [addr (<? (publishing-address nsv ledger-alias))]
      (boolean (<? (lookup nsv addr))))))

(defn address-path
  [address]
  (storage/get-local-path address))

(defn extract-branch
  "Splits a given namespace address into its nameservice and branch parts.
  Returns two-tuple of [nameservice branch].
  If no branch is found, returns nil as branch value and original ns-address as the nameservice."
  [ns-address]
  (if (str/ends-with? ns-address ")")
    (let [[_ ns branch] (re-matches #"(.*)\((.*)\)" ns-address)]
      [ns branch])
    [ns-address nil]))

(defn absolute-address?
  [address location]
  (str/starts-with? address location))

(defn resolve-address
  "Resolves a provided namespace address, which might be relative or absolute,
   into three parts returned as a map:
  - :alias - ledger alias
  - :branch - branch (or nil if default)
  - :address - absolute namespace address (including branch if provided)
  If 'branch' parameter is provided will always use it as the branch regardless
  of if a branch is specificed in the ns-address."
  [location ns-address branch]
  (let [[ns-address* extracted-branch] (extract-branch ns-address)
        branch* (or branch extracted-branch)
        [ns-address** alias] (if (absolute-address? ns-address location)
                               [ns-address* (storage/get-local-path ns-address*)]
                               [(storage/build-address location ns-address*) ns-address*])]
    {:alias   alias
     :branch  branch*
     :address (if branch*
                (str ns-address** "(" branch* ")")
                ns-address*)}))
