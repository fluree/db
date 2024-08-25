(ns fluree.db.nameservice
  (:refer-clojure :exclude [-lookup exists?])
  (:require [clojure.string :as str]
            [fluree.db.connection :as connection]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iNameService
  (-lookup [nameservice ledger-address]
    "Performs lookup operation on ledger alias and returns map of latest commit
    and other metadata")
  (-sync? [nameservice]
    "Indicates if nameservice updates should be performed synchronously, before
    commit is finalized. Failure will cause commit to fail")
  (-close [nameservice]
    "Closes all resources for this nameservice")
  (-alias [nameservice ledger-address]
    "Given a ledger address, returns ledger's default alias name else nil, if
    not avail")
  (-address [nameservice ledger-alias branch]
    "Returns full nameservice address/iri which will get published in commit. If
    'private', return nil."))

(defprotocol Publisher
  (-push [nameservice commit-data]
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

(defn nameservices
  [conn]
  (connection/-nameservices conn))

(defn relative-ledger-alias?
  [ledger-alias]
  (not (str/starts-with? ledger-alias "fluree:")))

(defn ns-address
  "Returns async channel"
  [nameservice ledger-alias branch]
  (-address nameservice ledger-alias branch))

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
          (let [sync? (-sync? ns)]
            (if sync?
              (<? (-push ns json-ld-commit))
              (-push ns json-ld-commit))
            (recur (rest nameservices*))))))))

(defn lookup-commit
  "Returns commit address from first matching nameservice on a conn
   for a given ledger alias and branch"
  [conn ledger-address]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (let [commit-address (<? (-lookup ns ledger-address))]
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
          (let [exists? (<? (-lookup ns ledger-alias))]
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
          (<? (-subscribe ns ledger-alias callback))
          (recur (rest nameservices*)))))))

(defn unsubscribe-ledger
  "Initiates unsubscription requests for a ledger into all namespaces on a connection."
  [conn ledger-alias]
  (let [nameservices (nameservices conn)]
    (go-try
      (loop [nameservices* nameservices]
        (when-let [ns (first nameservices*)]
          (<? (-unsubscribe ns ledger-alias))
          (recur (rest nameservices*)))))))
