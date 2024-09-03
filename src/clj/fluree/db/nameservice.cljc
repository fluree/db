(ns fluree.db.nameservice
  (:refer-clojure :exclude [alias])
  (:require [clojure.string :as str]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iNameService
  (lookup [nameservice ledger-address]
    "Performs lookup operation on ledger alias and returns map of latest commit
    and other metadata")
  (alias [nameservice ledger-address]
    "Given a ledger address, returns ledger's default alias name else nil, if
    not avail")
  (address [nameservice ledger-alias branch]
    "Returns full nameservice address/iri which will get published in commit. If
    'private', return nil.")
  (-close [nameservice]
    "Closes all resources for this nameservice"))

(defprotocol Publisher
  (publish [nameservice commit-data]
    "Publishes new commit to nameservice."))

(defprotocol Publication
  (-subscribe [nameservice ledger-alias callback]
    "Creates a subscription to nameservice(s) for ledger events. Will call
    callback with event data as received.")
  (-unsubscribe [nameservice ledger-alias]
    "Unsubscribes to nameservice(s) for ledger events"))

(defn full-address
  [prefix ledger-alias]
  (str prefix ledger-alias))

(defn ns-record
  "Generates nameservice metadata map for JSON storage. For now, since we only
  have a single branch possible, always sets default-branch. Eventually will
  need to merge changes from different branches into existing metadata map"
  [ns-address {address "address", alias "alias", branch "branch", :as commit-jsonld}]
  (let [branch-iri (str ns-address "(" branch ")")]
    {"@context"      "https://ns.flur.ee/ledger/v1"
     "@id"           ns-address
     "defaultBranch" branch-iri
     "ledgerAlias"   alias
     "branches"      [{"@id"     branch-iri
                       "address" address
                       "commit"  commit-jsonld}]}))

(defn commit-address-from-record
  [record branch]
  (let [branch-iri (if branch
                     (str (get record "@id") "(" branch ")")
                     (get record "defaultBranch"))]
    (->> (get record "branches")
         (some #(when (= (get % "@id") branch-iri)
                  (get % "address"))))))

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    (subs path 2)))

(defn address->alias
  [ledger-address]
  (-> ledger-address
      address-path
      (str/split #"/")
      (->> (drop-last 2) ; branch-name, head
           (str/join #"/"))))

(defn extract-branch
  "Splits a given namespace address into its nameservice and branch parts.
  Returns two-tuple of [nameservice branch].
  If no branch is found, returns nil as branch value and original ns-address as the nameservice."
  [ns-address]
  (if (str/ends-with? ns-address ")")
    (let [[_ ns branch] (re-matches #"(.*)\((.*)\)" ns-address)]
      [ns branch])
    [ns-address nil]))

(defn resolve-address
  "Resolves a provided namespace address, which might be relative or absolute,
   into three parts returned as a map:
  - :alias - ledger alias
  - :branch - branch (or nil if default)
  - :address - absolute namespace address (including branch if provided)
  If 'branch' parameter is provided will always use it as the branch regardless
  of if a branch is specificed in the ns-address."
  [base-address ns-address branch]
  (let [[ns-address* extracted-branch] (extract-branch ns-address)
        branch*   (or branch extracted-branch)
        absolute? (str/starts-with? ns-address base-address)
        [ns-address** alias] (if absolute?
                               [ns-address* (subs ns-address* (count base-address))]
                               [(str base-address ns-address*) ns-address*])]
    {:alias   alias
     :branch  branch*
     :address (if branch*
                (str ns-address** "(" branch* ")")
                ns-address*)}))
