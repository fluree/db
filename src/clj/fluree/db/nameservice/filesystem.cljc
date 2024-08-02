(ns fluree.db.nameservice.filesystem
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.bytes :as bytes]
            [clojure.core.async :as async :refer [go]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            #?(:clj [clojure.java.io :as io])))

#?(:clj (set! *warn-on-reflection* true))

(defn ns-record
"Generates nameservice metadata map for JSON storage..
For now, since we only have a single branch possible,
always sets default-branch. Eventually will need to merge
changes from different branches into existing metadata map"
[ns-address
 {address "address"
  alias  "alias"
  branch "branch"
  :as    json-ld-commit}]
(let [branch-iri (str ns-address "(" branch ")")]
  {"@context"      "https://ns.flur.ee/ledger/v1"
   "@id"           ns-address
   "defaultBranch" branch-iri
   "ledgerAlias"   alias
   "branches"      [{"@id"     branch-iri
                     "address" address
                     "commit"  json-ld-commit}]}))

(defn address-path
  [address]
  (let [[_ _ path] (str/split address #":")]
    (subs path 2)))

(defn list-files-in-path
  "Lists all files in the given directory path"
  [path]
  (let [directory (io/file path)
        files (file-seq directory)]
    files))

(defn file-path
  "Returns fully formed file path where a ns record would be stored."
  [local-path alias]
  (str local-path "/" alias ".json"))

(defn ns-record-from-disk
  [local-path ledger-alias]
  (->> ledger-alias
       (file-path local-path)
       fs/read-file))

(defn address
  [base-address ledger-alias {:keys [branch] :as _opts}]
  (when base-address
    (str base-address ledger-alias)))

(defn write-ns-record
  [ns-record local-path alias]
  (let [p-chan     (async/promise-chan) ;; return value
        write-path (file-path local-path alias)
        record-bs  (try* (json/stringify-UTF8 ns-record)
                         (catch* e
                                 (log/error "Error json-encoding nameservice record for ledger: " alias
                                            "with exception: " (ex-message e)
                                            "Original record where error occurred: " ns-record)
                                 ;; return exception, don't throw.. will check for it below
                                 (ex-info (str "Exception encoding file nameservice file for ledger: " alias)
                                          {:status 500 :error :db/invalid-commit})))]
    (if (util/exception? record-bs)
      (async/put! p-chan record-bs)
      #?(:clj  (async/thread
                 (try
                   (fs/write-file write-path record-bs)
                   (async/put! p-chan write-path)
                   (catch Exception e
                     (log/error (str "Exception writing file nameservice file for ledger: " alias
                                     "with exception: " (ex-message e))
                                e)
                     (async/put! p-chan (ex-info (str "Exception writing file nameservice file for ledger: " alias
                                                      " with exception: " (ex-message e))
                                                 {:status 500 :error :db/invalid-commit}
                                                 e)))))
         :cljs (try*
                 (fs/write-file write-path record-bs)
                 (async/put! p-chan write-path)
                 (catch* e (async/put! p-chan e)))))
    p-chan))

(defn push!
  "Pushes updated commit to internal format stored on file system"
  [local-path base-address {alias          "alias"
                            :as commit-json-ld}]
  (let [ns-address     (address base-address alias nil)
        record         (ns-record ns-address commit-json-ld)]
    ;; write-ns-record returns a promise chan, with path of file if successful or exception
    (write-ns-record record local-path alias)))


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

(defn retrieve-ns-record
  "Loads nameservice record from disk given a local path and ledger alias"
  [local-path ledger-alias]
  (go-try
    (let [ns-record (<? (ns-record-from-disk local-path ledger-alias))]
      (when ns-record
        (json/parse ns-record false)))))

(defn commit-address-from-record
  [ns-record branch]
  (let [branch-iri (if branch
                     (str (get ns-record "@id") "(" branch ")")
                     (get ns-record "defaultBranch"))]
    (->> (get ns-record "branches")
         (some #(when (= (get % "@id") branch-iri)
                  (get % "address"))))))

(defn convert-legacy-ns-record
  [alias commit-address local-path legacy-path]
  (async/go
    (let [ns-address (str "fluree:file://" alias)
          ns-record  (ns-record ns-address {"address" commit-address "alias" alias, "branch" "main"})]
      (let [successful? (async/<! (write-ns-record ns-record local-path alias))]
        (if (util/exception? successful?)
          (log/error successful?
                     (str "Unable to update legacy nameservice file for ledger: " alias
                          "with exception: " (ex-message successful?)))
          #?(:clj (try
                    (.renameTo (io/file legacy-path) (io/file (str legacy-path ".legacy")))
                    (catch Exception e
                      (log/error "Exception renaming legacy nameservice file for ledger: " alias
                                 "with exception: " (ex-message e))))))))))

(defn try-legacy-ns-lookup
  "This is for legacy filesystem nameservice file format only.
  It should be removed once Fluree v3 is GA, but will allow for
  the old format to be read and converted to the new format in the background.
  If deemed important, this can be done in an upgrade script instead as part of
  the v3 GA launch."
  [local-path alias]
  (go-try
    (let [legacy-path    (str local-path "/" alias "/main/head")
          commit-address (when-let [address (<? (fs/read-file legacy-path))]
                           (cond
                             (str/starts-with? address "//")
                             (str "fluree:file:" address)

                             (str/starts-with? address "fluree:")
                             address

                             :else
                             (do
                               (log/warn "Unexpected commit address format in legacy nameservice file:"
                                         legacy-path
                                         "with address:" address)
                               nil)))]
      (when commit-address
        ;; write out new NS file record format in the background
        (convert-legacy-ns-record alias commit-address local-path legacy-path)

        commit-address))))

(defn lookup
  "When provided a 'relative' ledger alias, looks in file system to see if
  nameservice file exists and if so returns the latest commit address."
  [ns-address local-path base-address {:keys [branch] :as _opts}]
  (go-try
    (let [{:keys [alias branch* address]} (resolve-address base-address ns-address branch)
          ns-record (<? (retrieve-ns-record local-path alias))]
      (if ns-record
        (or (commit-address-from-record ns-record branch*)
            (throw (ex-info (str "No nameservice record found for ledger alias: " ns-address)
                            {:status 404 :error :db/ledger-not-found})))
        ;; Note, below is for leagacy conversion only, will get removed in v3 GA
        (<? (try-legacy-ns-lookup local-path alias))))))


(defrecord FileNameService
  [local-path sync? base-address]
  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup ledger-alias local-path base-address nil))
  (-lookup [_ ledger-alias opts] (lookup ledger-alias local-path base-address opts))
  (-push [_ commit-data] (push! local-path base-address commit-data))
  (-sync? [_] sync?)
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported FileNameService op: ledgers" {})))
  (-address [_ ledger-alias opts]
    (go (address base-address ledger-alias opts)))
  (-alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (address-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join #"/"))))
  (-close [nameservice] true))


(defn initialize
  "Initializes nameservice that will manage commit data via a
  local file system in the directory provided by `path` parameter.
  This ns can publish any ns address in newly generated commits by
  supplying an `address-base` parameter which will be appended with
  the ledger alias. The default value for 'address-base' is
  `fluree:file://`.
  If you wanted the nameservice to show up in the commit metadata
  as https://data.mydomain.com/<ledger-alias> and to be stored
  in the file system at path /opt/fluree/ns/<ledger-alias>,
  then you would set:
  - path = /opt/fluree/ns (directory, so trailing slash doesn't matter)
  - address-base = https://data.mydomain.com/ (trailing slash important)
  address-base can be anything, but when appended with the ledger alias
  should be a URI/IRI. Ledger names are relative, e.g. 'my/ledger/name',
  so the address-base should include a trailing '/' if a URL, or a
  trailing ':' if in the form of a URN.
  address-base can be 'nil' if you don't want the address
  published as part of the commit metadata's nameservices."
  ([path] (initialize path nil))
  ([path {:keys [sync? base-address]
          :or   {base-address "fluree:file://"}}]
   (let [local-path (fs/local-path path)
         sync?      (if (some? sync?)
                      sync?
                      true)]
     (map->FileNameService {:local-path   local-path
                            :sync?        sync?
                            :base-address base-address}))))
