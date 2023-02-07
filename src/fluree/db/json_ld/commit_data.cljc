(ns fluree.db.json-ld.commit-data
  (:require [fluree.db.index :as index]
            [fluree.crypto :as crypto]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.log :as log]
            [clojure.string :as str]
            [fluree.db.constants :as const]))

(comment
  ;; commit map - this map is what gets recorded in a few places:
  ;; - in a 'commit' file: (translated to JSON-LD, and optionally wrapped in a Verifiable Credential)
  ;; - attached to each DB: to know the last commit state when db was pulled from ledger
  ;; - in the ledger-state: since a db may be operated on asynchronously, it can
  ;;                        see if anything (e.g. an index) has since been updated
  {:id       "fluree:commit:sha256:ljklj" ;; relative from source, source is the 'ledger address'
   :address  "" ;; commit address, if using something like IPFS this is blank
   :v        0 ;; version of commit format
   :alias    "mydb" ;; human-readable alias name for ledger
   :branch   "main" ;; ledger's "branch" - if not included, default of 'main'
   :time     "2022-08-26T19:51:27.220086Z" ;; ISO-8601 timestamp of commit
   :tag      [] ;; optional commit tags
   :message  "optional commit message"
   :issuer   {:id ""} ;; issuer of the commit
   :previous {:id      "fluree:commit:sha256:ljklj"
              :address "previous commit address"} ;; previous commit address
   ;; data information commit refers to:
   :data     {:id       "fluree:db:sha256:lkjlkjlj" ;; db's unique identifier
              :t        52
              :address  "fluree:ipfs://sdfsdfgfdgk" ;; address to locate data file / db
              :previous {:id      "fluree:db:sha256:lkjlkjlj" ;; previous db
                         :address "fluree:ipfs://sdfsdfgfdgk"}
              :flakes   4242424
              :size     123145
              :source   {:id      "csv:sha256:lkjsdfkljsdf" ;; sha256 of original source (e.g. signed transaction, CSV file)
                         :address "/ipfs/sdfsdfgfdgk"
                         :issuer  {:id ""}}} ;; issuer of the commit
   ;; name service(s) used to manage global ledger state
   :ns       {:id  "fluree:ipns://data.flur.ee/my/db" ;; one (or more) Name Services that can be consulted for the latest ledger state
              :foo ""} ;; each name service can contain additional data relevant to it
   ;; latest index (note the index roots below are not recorded into JSON-LD commit file, but short-cut when internally managing transitions)
   :index    {:id      "fluree:index:sha256:fghfgh" ;; unique id (hash of root) of index
              :address "fluree:ipfs://lkjdsflkjsdf" ;; address to get to index 'root'
              :data    {:id      "fluree:db:sha256:lkjlkjlj" ;; db of last index unique identifier
                        :t       42
                        :address "fluree:ipfs://sdfsdfgfdgk" ;; address to locate db
                        :flakes  4240000
                        :size    120000}
              :spot    "fluree:ipfs://spot" ;; following 4 items are not recorded in the commit, but used to shortcut updated index retrieval in-process
              :psot    "fluree:ipfs://psot"
              :post    "fluree:ipfs://post"
              :opst    "fluree:ipfs://opst"
              :tspo    "fluree:ipfs://tspo"}})


(def json-ld-base-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["@context" "https://ns.flur.ee/ledger/v1"]
   ["id" :id]
   ["v" 0]
   ["address" :address]
   ["type" ["Commit"]]
   ["alias" :alias]
   ["issuer" :issuer]
   ["branch" :branch]
   ["time" :time]
   ["tag" :tag]
   ["message" :message]
   ["previous" :previous] ;; refer to :prev-commit template
   ["data" :data] ;; refer to :data template
   ["ns" :ns] ;; refer to :ns template
   ["index" :index] ;; refer to :index template
   [const/iri-default-context (keyword const/iri-default-context)]])

(def json-ld-prev-commit-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["Commit"]]
   ["address" :address]])

(def json-ld-prev-data-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["DB"]]
   ["address" :address]])


(def json-ld-data-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["DB"]]
   ["t" :t]
   ["address" :address]
   ["previous" :previous]
   ["flakes" :flakes]
   ["size" :size]])

(def json-ld-issuer-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]])

(def json-ld-ns-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["FNS"]]])

(def json-ld-index-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["Index"]]
   ["address" :address]
   ["data" :data]])

(defn merge-template
  "Merges provided map with template and places any
  values in map with respective template value. If value
  does not exist in map, removes value from template.
  Any default values specified in template are carried through,
  provided there is at least one matching map value, else returns nil."
  [m template]
  (when m
    (loop [[[k v] & r] template
           have-value? false
           acc         (transient [])]
      (if k
        (if (keyword? v)
          (if-let [v* (get m v)]
            (recur r true (-> acc
                              (conj! k) ; note, CLJS allows multi-arity for conj!, but clj does not
                              (conj! v*)))
            (recur r have-value? acc))
          (recur r have-value? (-> acc
                                   (conj! k)
                                   (conj! v))))
        (when have-value?
          (apply array-map (persistent! acc)))))))

(defn ->json-ld
  "Converts a clojure commit map to a JSON-LD version.
  Uses the JSON-LD template, and only incorporates values
  that exist in both the commit-map and the json-ld template,
  except for some defaults (like rdf:type) which are not in
  our internal commit map, but are part of json-ld."
  [{:keys [previous data ns index issuer] :as commit-map}]
  (let [prev-data   (when (not-empty (:previous data))
                      (merge-template (:previous data) json-ld-prev-data-template))
        commit-map* (assoc commit-map
                      :previous (merge-template previous json-ld-prev-commit-template)
                      :data (merge-template (assoc data :previous prev-data) json-ld-data-template)
                      :issuer (merge-template issuer json-ld-issuer-template)
                      :ns (merge-template ns json-ld-ns-template)
                      :index (-> (merge-template (:data index) json-ld-data-template) ;; index has an embedded db map
                                 (#(assoc index :data %))
                                 (merge-template json-ld-index-template)))]
    (merge-template commit-map* json-ld-base-template)))

(defn- un-jsonify
  "Turns json commit IRIs into keywords"
  [m]
  (reduce-kv
    (fn [acc k v]
      (if (string? k)
        (let [kw (-> (str/split k #"#") second keyword)
              v* (or (:value v) (:id v))]
          (if v*
            (assoc acc kw v*)
            acc))
        acc))
    {} m))

(defn json-ld->map
  "Turns json-ld commit meta into the clojure map structure."
  [commit-json-ld {:keys [commit-address spot psot post opst tspo]}]
  (let [{id          :id,
         address     const/iri-address,
         v           const/iri-v,
         alias       const/iri-alias,
         branch      const/iri-branch,
         issuer      const/iri-issuer
         time        const/iri-time,
         tag         const/iri-tag,
         message     const/iri-message
         prev-commit const/iri-previous,
         data        const/iri-data,
         ns          const/iri-ns,
         index       const/iri-index} commit-json-ld
        db-object (fn [{id      :id,
                        t       const/iri-t,
                        address const/iri-address,
                        flakes  const/iri-flakes,
                        size    const/iri-size :as _db-item}]
                    {:id      id ;; db's unique identifier
                     :t       (:value t)
                     :address (:value address) ;; address to locate db
                     :flakes  (:value flakes)
                     :size    (:value size)})]
    {:id       id
     :address  (if (empty? (:value address)) ;; commit address, if using something like IPFS this is empty string
                 commit-address
                 (:value address))
     :v        (:value v) ;; version of commit format
     :alias    (:value alias) ;; human-readable alias name for ledger
     :branch   (:value branch) ;; ledger's "branch" - if not included, default of 'main'
     :issuer   (when issuer {:id (:id issuer)})
     :time     (:value time) ;; ISO-8601 timestamp of commit
     :tag      (mapv :value tag)
     :message  (:value message)
     :previous {:id      (:id prev-commit)
                :address (get-in prev-commit [const/iri-address :value])} ;; previous commit address
     ;; database information commit refers to:
     :data     (db-object data)
     ;; name service(s) used to manage global ledger state
     ;; TODO - flesh out with final ns data structure
     :ns       (when ns ;; one (or more) Fluree Name Services that can be consulted for the latest ledger state
                 (if (sequential? ns)
                   (mapv (fn [namespace] {:id (:id namespace)}) ns)
                   {:id (:id ns)}))
     ;; latest index (note the index roots below are not recorded into JSON-LD commit file, but short-cut when internally managing transitions)
     :index    (when index
                 {:id      (:id index) ;; unique id (hash of root) of index
                  :address (get-in index [const/iri-address :value]) ;; address to get to index 'root'
                  :data    (db-object (get index const/iri-data))
                  :spot    spot ;; following 4 items are not recorded in the commit, but used to shortcut updated index retrieval in-process
                  :psot    psot
                  :post    post
                  :opst    opst
                  :tspo    tspo})}))


(defn update-commit-id
  "Once a commit id is known (by hashing json-ld version of commit), update
  the id prior to writing the commit to disk"
  [commit commit-id]
  (assoc commit :id commit-id))

(defn update-commit-address
  "Once a commit address is known, which might be after the commit is written
  if IPFS, add the final address into the commit map."
  [commit commit-address]
  (assoc commit :address commit-address))

(defn commit-json->commit-id
  [jld]
  (let [b32-hash (-> jld
                     json-ld/normalize-data
                     (crypto/sha2-256 :base32))]
    (str "fluree:commit:sha256:b" b32-hash)))

(defn commit-jsonld
  "Generates JSON-LD commit map, and hash to include the @id value.
  Return a two-tuple of the updated commit map and the final json-ld document"
  [commit]
  (let [jld         (->json-ld commit)
        commit-id   (commit-json->commit-id jld)
        commit-map* (update-commit-id commit commit-id)
        jld*        (assoc jld "id" commit-id)]
    [commit-map* jld*]))


(defn blank-commit
  "Creates a skeleton blank commit map."
  [{:keys [v branch alias]
    :or   {v 0, branch "main"}}]
  {:alias  alias
   :v      v
   :branch branch})

(defn new-index
  "Creates a new commit index record, given the commit-map used to trigger
  the indexing process (which contains the db info used for the index), the
  index id, index address and optionally index-type-addresses which contain
  the address for each index type top level branch node."
  [data-map id address index-root-maps]
  (merge {:id      id
          :address address
          :data    data-map}
         index-root-maps))

(defn update-index
  "Updates an existing commit with recently completed index data.
  This differs from new-index, as that is always created from the current
  db's commit map that generates the index. This will update the most recent
  commit map with the results of an asynchronous indexing process that just completed.

  Verifies commit-index is at least as current as the index in the commit map, else
  just returns original commit map."
  [commit-map new-commit-index]
  (let [{existing-index-db :data} (:index commit-map)
        {new-index-db :data} new-commit-index]
    (if (and existing-index-db
             (> (:t existing-index-db) (:t new-index-db)))
      commit-map
      (assoc commit-map :index new-commit-index))))

(defn t
  "Given a commit map, returns the t value of the commit."
  [commit-map]
  (-> commit-map :data :t))

(defn index-t
  "Given a commit map, returns the t value of the index (if exists)."
  [commit-map]
  (-> commit-map :index :data :t))


(defn older-t?
  "Returns true if first 't' value is older than second 't' value."
  [t t']
  (< t t'))

(defn use-latest-index
  "Checks if old-commit has a more current index than new-commit and
  if so, updates new-commit to contain the updated index.

  This can happen when processing a new commit while an asynchronous
  indexing process complete giving it a newer index point than the
  new commit"
  [{new-index :index :as new-commit} {old-index :index :as old-commit}]
  (if (not= (index-t new-commit)
            (index-t old-commit))
    (cond
      ;; there is no old index, just return new commit
      (nil? old-index)
      new-commit

      ;; new-index is nil but there is an old index, or old index is newer
      (or (nil? new-index)
          (< (index-t new-commit) (index-t old-commit)))
      (assoc new-commit :index old-index)

      ;; index in new-commit is newer, no changes to new commit
      :else
      new-commit)
    new-commit))

(defn new-db-commit
  "Returns the :data portion of the commit map for a new db commit."
  [dbid t db-address prev-data flakes size]
  (cond-> {:id      dbid ;; db's unique identifier
           :t       (- t)
           :address db-address ;; address to locate db
           :flakes  flakes
           :size    size}
          (not-empty prev-data) (assoc :previous prev-data)))

(defn data
  "Given a commit map, returns them most recent data map."
  [commit]
  (-> commit :data))

(defn data-id
  "Given a commit, returns the id of the most recent (previous) data id"
  [commit]
  (let [commit-data (data commit)]
    (:id commit-data)))

(defn new-db-commit-map
  "Returns a commit map with a new db registered.
  Assumes commit is not yet created (but db is persisted), so
  commit-id and commit-address are added after finalizing and persisting commit."
  [{:keys [old-commit issuer message tag dbid t db-address flakes size]
    :as commit}]
  (let [prev-data   (select-keys (data old-commit) [:id :address])
        data-commit (new-db-commit dbid t db-address prev-data flakes size)
        prev-commit (not-empty (select-keys old-commit [:id :address]))
        context-key (keyword const/iri-default-context)
        context     (get commit context-key)
        commit      (-> old-commit
                        (dissoc :id :address :data :issuer :time :message :tag :prev-commit)
                        (assoc :address ""
                               :data data-commit
                               :time (util/current-time-iso)))]
    (cond-> commit
            issuer (assoc :issuer {:id issuer})
            prev-commit (assoc :previous prev-commit)
            message (assoc :message message)
            tag (assoc :tag tag)
            context (assoc context-key context))))

(defn update-db
  "Updates the :data portion of the commit map to represent a saved new db update."
  [commit-map dbid t db-address flakes size]
  (let [prev-data (select-keys (:data commit-map) [:id :address])]
    (assoc commit-map :data (new-db-commit dbid t db-address prev-data flakes size))))
