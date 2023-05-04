(ns fluree.db.json-ld.commit-data
  (:require [fluree.crypto :as crypto]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]
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
   ["defaultContext" :defaultContext]]) ;; refer to :defaultContext template

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

(def json-ld-default-ctx-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["id" :id]
   ["type" ["Context"]]
   ["address" :address]])

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
  [{:keys [previous data ns index issuer defaultContext] :as commit-map}]
  (let [prev-data   (when (not-empty (:previous data))
                      (merge-template (:previous data) json-ld-prev-data-template))
        commit-map* (assoc commit-map
                      :previous (merge-template previous json-ld-prev-commit-template)
                      :data (merge-template (assoc data :previous prev-data) json-ld-data-template)
                      :issuer (merge-template issuer json-ld-issuer-template)
                      :ns (merge-template ns json-ld-ns-template)
                      :index (-> (merge-template (:data index) json-ld-data-template) ;; index has an embedded db map
                                 (#(assoc index :data %))
                                 (merge-template json-ld-index-template))
                      :defaultContext (merge-template defaultContext json-ld-default-ctx-template))]
    (merge-template commit-map* json-ld-base-template)))

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
         default-ctx const/iri-default-context
         message const/iri-message
         prev-commit const/iri-previous,
         data const/iri-data,
         ns const/iri-ns,
         index const/iri-index} commit-json-ld
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
                  :tspo    tspo})
     :defaultContext (let [address (get-in default-ctx [const/iri-address :value])]
                       (-> default-ctx
                           (select-keys [:id :type])
                           (assoc :address address)))}))


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

(defn t
  "Given a commit map, returns the t value of the commit."
  [commit-map]
  (-> commit-map :data :t))

(defn index-t
  "Given a commit map, returns the t value of the index (if exists)."
  [commit-map]
  (-> commit-map :index :data :t))

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
    :as   _commit}]
  (let [prev-data   (select-keys (data old-commit) [:id :address])
        data-commit (new-db-commit dbid t db-address prev-data flakes size)
        prev-commit (not-empty (select-keys old-commit [:id :address]))
        commit      (-> old-commit
                        (dissoc :id :address :data :issuer :time :message :tag :prev-commit)
                        (assoc :address ""
                               :data data-commit
                               :time (util/current-time-iso)))]
    (cond-> commit
            issuer (assoc :issuer {:id issuer})
            prev-commit (assoc :previous prev-commit)
            message (assoc :message message)
            tag (assoc :tag tag))))

(defn ref?
  [f]
  (-> f
      flake/dt
      (= const/$xsd:anyURI)))

(defn ref-flakes
  "Returns ref flakes from set of all flakes. Uses Flake datatype to know if a ref."
  [flakes]
  (filter ref? flakes))

(defn update-novelty-idx
  [novelty-idx add remove]
  (-> (reduce disj novelty-idx remove)
      (into add)))

(defn update-novelty
  ([db add]
   (update-novelty db add []))

  ([db add rem]
   (let [ref-add     (ref-flakes add)
         ref-rem     (ref-flakes rem)
         flake-count (cond-> 0
                       add (+ (count add))
                       rem (- (count rem)))
         flake-size  (cond-> 0
                       add (+ (flake/size-bytes add))
                       rem (- (flake/size-bytes rem)))]
     (-> db
         (update-in [:novelty :spot] update-novelty-idx add rem)
         (update-in [:novelty :psot] update-novelty-idx add rem)
         (update-in [:novelty :post] update-novelty-idx add rem)
         (update-in [:novelty :opst] update-novelty-idx ref-add ref-rem)
         (update-in [:novelty :tspo] update-novelty-idx add rem)
         (update-in [:novelty :size] + flake-size)
         (update-in [:stats :size] + flake-size)
         (update-in [:stats :flakes] + flake-count)))))

(def commit-schema-flakes
  #{(flake/create const/$_previous const/$xsd:anyURI const/iri-previous const/$xsd:string -1 true nil)
    (flake/create const/$_address const/$xsd:anyURI const/iri-address const/$xsd:string -1 true nil)
    (flake/create const/$_v const/$xsd:anyURI const/iri-v const/$xsd:string -1 true nil)

    (flake/create const/$_ledger:alias const/$xsd:anyURI const/iri-alias const/$xsd:string -1 true nil)
    (flake/create const/$_ledger:branch const/$xsd:anyURI const/iri-branch const/$xsd:string -1 true nil)
    (flake/create const/$_ledger:context const/$xsd:anyURI const/iri-default-context const/$xsd:string -1 true nil)

    (flake/create const/$_commit:signer const/$xsd:anyURI const/iri-issuer const/$xsd:string -1 true nil)
    (flake/create const/$_commit:message const/$xsd:anyURI const/iri-message const/$xsd:string -1 true nil)
    (flake/create const/$_commit:time const/$xsd:anyURI const/iri-time const/$xsd:string -1 true nil)
    (flake/create const/$_commit:data const/$xsd:anyURI const/iri-data const/$xsd:string -1 true nil)

    (flake/create const/$_commitdata:flakes const/$xsd:anyURI const/iri-flakes const/$xsd:string -1 true nil)
    (flake/create const/$_commitdata:size const/$xsd:anyURI const/iri-size const/$xsd:string -1 true nil)
    (flake/create const/$_commitdata:t const/$xsd:anyURI const/iri-t const/$xsd:string -1 true nil)})

(defn add-tt-id
  "Associates a unique tt-id for any in-memory staged db in their index roots.
  tt-id is used as part of the caching key, by having this in place it means
  that even though the 't' value hasn't changed it will cache each stage db
  data as its own entity."
  [db]
  (let [tt-id   (random-uuid)
        indexes [:spot :psot :post :opst :tspo]]
    (-> (reduce
          (fn [db* idx]
            (let [{:keys [children] :as node} (get db* idx)
                  children* (reduce-kv
                              (fn [children* k v]
                                (assoc children* k (assoc v :tt-id tt-id)))
                              {} children)]
              (assoc db* idx (assoc node :tt-id tt-id
                                         :children children*))))
          db indexes)
        (assoc :tt-id tt-id))))

(defn add-commit-schema-flakes
  [db]
  (-> db
      (update-novelty commit-schema-flakes)
      add-tt-id
      (update :schema vocab/update-with* -1 commit-schema-flakes)))

(defn commit-metadata-flakes
  [{:keys [address alias branch data id time v]} t db-sid]
  (let [{db-id :id db-t :t db-address :address :keys [flakes size]} data]
    [;; link db to associated commit meta: @id
     (flake/create t const/$xsd:anyURI id const/$xsd:string t true nil)

     ;; commit flakes
     ;; address
     (flake/create t const/$_address address const/$xsd:string t true nil)
     ;; alias
     (flake/create t const/$_ledger:alias alias const/$xsd:string t true nil)
     ;; branch
     (flake/create t const/$_ledger:branch branch const/$xsd:string t true nil)
     ;; v
     (flake/create t const/$_v v const/$xsd:int t true nil)
     ;; time
     (flake/create t const/$_commit:time (util/str->epoch-ms time)
                   const/$xsd:dateTime t true nil) ;; data
     (flake/create t const/$_commit:data db-sid const/$xsd:anyURI t true nil)

     ;; db flakes
     ;; @id
     (flake/create db-sid const/$xsd:anyURI db-id const/$xsd:string t true nil)
     ;; t
     (flake/create db-sid const/$_commitdata:t db-t const/$xsd:int t true nil)
     ;; address
     (flake/create db-sid const/$_address db-address const/$xsd:string t true nil)
     ;; size
     (flake/create db-sid const/$_commitdata:size size const/$xsd:int t true nil)
     ;; flakes
     (flake/create db-sid const/$_commitdata:flakes flakes const/$xsd:int t true
                   nil)]))


(defn add-commit-flakes
  "Translate commit metadata into flakes and merge them into novelty."
  [prev-commit {:keys [commit] :as db}]
  (go-try
   (let [last-sid           (volatile! (jld-ledger/last-commit-sid db))
         next-sid           (fn [] (vswap! last-sid inc))

         {:keys [data defaultContext issuer message]} commit
         {db-t :t} data

         {previous-id :id prev-data :data} prev-commit
         prev-data-id       (:id prev-data)

         t                  (- db-t)
         db-sid             (next-sid)

         base-flakes        (commit-metadata-flakes commit t db-sid)

         prev-commit-flakes (when previous-id
                              (let [prev-sid (<? (dbproto/-subid db previous-id))]
                                [(flake/create t const/$_previous prev-sid const/$xsd:anyURI t true nil)]))

         prev-db-flakes     (when prev-data-id
                              (let [prev-sid (<? (dbproto/-subid db prev-data-id))]
                                [(flake/create db-sid const/$_previous prev-sid const/$xsd:anyURI t true nil)]))

         issuer-flakes      (when-let [issuer-iri (:id issuer)]
                              (if-let [issuer-sid (<? (dbproto/-subid db issuer-iri))]
                                ;; create reference to existing issuer
                                [(flake/create t const/$_commit:signer issuer-sid const/$xsd:anyURI t true nil)]
                                ;; create new issuer flake and a reference to it
                                (let [new-issuer-sid (next-sid)]
                                  [(flake/create t const/$_commit:signer new-issuer-sid const/$xsd:anyURI t true nil)
                                   (flake/create new-issuer-sid const/$xsd:anyURI issuer-iri const/$xsd:string t true nil)])))
         message-flakes     (when message
                              [(flake/create t const/$_commit:message message const/$xsd:string t true nil)])
         default-ctx-flakes (when-let [default-ctx-iri (:id defaultContext)]
                              (if-let [default-ctx-id (<? (dbproto/-subid db default-ctx-iri))]
                                [(flake/create t const/$_ledger:context default-ctx-id const/$xsd:anyURI t true nil)]
                                (let [new-default-ctx-id (next-sid)
                                      address            (:address defaultContext)]
                                  [(flake/create t const/$_ledger:context new-default-ctx-id const/$xsd:anyURI t true nil)
                                   (flake/create new-default-ctx-id const/$xsd:anyURI default-ctx-iri const/$xsd:string t true nil)
                                   (flake/create new-default-ctx-id const/$_address address const/$xsd:string t true nil)])))
         commit-flakes      (cond-> base-flakes
                                    prev-commit-flakes (into prev-commit-flakes)
                                    prev-db-flakes (into prev-db-flakes)
                                    issuer-flakes (into issuer-flakes)
                                    message-flakes (into message-flakes)
                                    default-ctx-flakes (into default-ctx-flakes))]
     (-> db
         (assoc-in [:ecount const/$_shard] @last-sid)
         (cond-> (= 1 db-t) add-commit-schema-flakes)
         (update-novelty commit-flakes)
         add-tt-id))))
