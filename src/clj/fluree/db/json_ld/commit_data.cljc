(ns fluree.db.json-ld.commit-data
  (:require [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [get-first get-first-value try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]))

(def commit-version 1)

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
              :post    "fluree:ipfs://post"
              :opst    "fluree:ipfs://opst"
              :tspo    "fluree:ipfs://tspo"}})


(def json-ld-base-template
  "Note, key-val pairs are in vector form to preserve ordering of final commit map"
  [["@context" "https://ns.flur.ee/ledger/v1"]
   ["id" :id]
   ["v" :v]
   ["address" :address]
   ["type" ["Commit"]]
   ["alias" :alias]
   ["issuer" :issuer]
   ["author" :author]
   ["txn" :txn]
   ["annotation" :annotation]
   ["branch" :branch]
   ["time" :time]
   ["tag" :tag]
   ["message" :message]
   ["previous" :previous] ;; refer to :prev-commit template
   ["data" :data]         ;; refer to :data template
   ["ns" :ns]             ;; refer to :ns template
   ["index" :index]]) ;; refer to :index template


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
  [["id" :id]])

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

(defn data-map->json-ld
  [data-map]
  (let [prev-data-map  (:previous data-map)
        prev-data      (when (not-empty prev-data-map)
                         (merge-template prev-data-map json-ld-prev-data-template))]
    (-> data-map
        (assoc :previous prev-data)
        (merge-template json-ld-data-template))))

(defn ->json-ld
  "Converts a clojure commit map to a JSON-LD version. Uses the JSON-LD template,
  and only incorporates values that exist in both the commit-map and the json-ld
  template, except for some defaults (like rdf:type) which are not in our
  internal commit map, but are part of json-ld."
  [{:keys [previous data ns index issuer] :as commit-map}]
  (let [commit-map*    (assoc commit-map
                              :previous (merge-template previous json-ld-prev-commit-template)
                              :data (data-map->json-ld data)
                              :issuer (merge-template issuer json-ld-issuer-template)
                              :ns (mapv #(merge-template % json-ld-ns-template) ns)
                              :index (-> index
                                         (update :data data-map->json-ld) ; index has an embedded db map
                                         (merge-template json-ld-index-template)))]
    (merge-template commit-map* json-ld-base-template)))

(defn parse-db-data
  [data]
  {:id      (:id data)
   :t       (get-first-value data const/iri-fluree-t)
   :address (get-first-value data const/iri-address)
   :flakes  (get-first-value data const/iri-flakes)
   :size    (get-first-value data const/iri-size)})

(defn jsonld->clj
  [jsonld]
  (let [id          (:id jsonld)
        v           (get-first-value jsonld const/iri-v)
        alias       (get-first-value jsonld const/iri-alias)
        branch      (get-first-value jsonld const/iri-branch)
        address     (-> jsonld
                        (get-first-value const/iri-address)
                        not-empty)
        author      (get-first-value jsonld const/iri-author)
        txn         (get-first-value jsonld const/iri-txn)

        time        (get-first-value jsonld const/iri-time)
        message     (get-first-value jsonld const/iri-message)
        tags        (get-first jsonld const/iri-tag)
        issuer      (get-first jsonld const/iri-issuer)
        prev-commit (get-first jsonld const/iri-previous)
        data        (get-first jsonld const/iri-data)
        ns          (get-first jsonld const/iri-ns)
        index       (get-first jsonld const/iri-index)]

    (cond-> {:id     id
             :v      v
             :alias  alias
             :branch branch
             :time   time
             :tag    (mapv :value tags)
             :data   (parse-db-data data)
             :author author}
            txn (assoc :txn txn)
            address (assoc :address address)
            prev-commit (assoc :previous {:id      (:id prev-commit)
                                          :address (get-first-value prev-commit const/iri-address)})
            message (assoc :message message)
            ns (assoc :ns (->> ns
                               util/sequential
                               (mapv (fn [namespace]
                                       (select-keys namespace [:id])))))
            index (assoc :index {:id      (:id index)
                                 :address (get-first-value index const/iri-address)
                                 :data    (parse-db-data (get-first index const/iri-data))})
            issuer (assoc :issuer (select-keys issuer [:id])))))

(defn update-index-roots
  [commit-map {:keys [spot post opst tspo]}]
  (if (contains? commit-map :index)
    (update commit-map :index assoc :spot spot, :post post, :opst opst, :tspo tspo)
    commit-map))

(defn json-ld->map
  ([commit-jsonld index-roots]
   (json-ld->map commit-jsonld nil index-roots))

  ([commit-jsonld fallback-address index-roots]
   (let [commit-map (jsonld->clj commit-jsonld)]
     (cond-> commit-map
       (and (some? fallback-address)
            (not (contains? commit-map :address)))
       (assoc :address fallback-address) ; address, if using something like
                                         ; IPFS, is empty string

       true
       (update-index-roots index-roots)))))

(defn update-commit-id
  "Once a commit id is known (by hashing json-ld version of commit), update
  the id prior to writing the commit to disk"
  [commit commit-id]
  (assoc commit :id commit-id))

(defn hash->commit-id
  [hsh]
  (str "fluree:commit:sha256:b" hsh))

(defn commit-json->commit-id
  [jld]
  (-> jld
      json/stringify-UTF8
      (crypto/sha2-256 :base32)
      hash->commit-id))

(defn hash->db-id
  [hsh]
  (str "fluree:db:sha256:b" hsh))

(defn db-json->db-id
  [payload]
  (-> payload
      json/stringify-UTF8
      (crypto/sha2-256 :base32)
      hash->db-id))

(defn blank-commit
  "Creates a skeleton blank commit map."
  [alias branch publish-addresses init-time]
  (let [commit-json  (->json-ld {:alias  alias
                                 :v      0
                                 :branch (if branch
                                           (util/keyword->str branch)
                                           "main")
                                 :data   {:t      0
                                          :flakes 0
                                          :size   0}
                                 :time   init-time
                                 :ns     (mapv #(if (map? %)
                                                  %
                                                  {:id %})
                                               publish-addresses)})
        db-json      (get commit-json "data")
        dbid         (db-json->db-id db-json)
        commit-json* (assoc-in commit-json ["data" "id"] dbid)
        commit-id    (commit-json->commit-id commit-json*)]
    (assoc commit-json* "id" commit-id)))

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
          (flake/t-before? (index-t new-commit) (index-t old-commit)))
      (assoc new-commit :index old-index)

      ;; index in new-commit is newer, no changes to new commit
      :else
      new-commit)
    new-commit))

(defn new-db-commit
  "Returns the :data portion of the commit map for a new db commit."
  [dbid t db-address prev-data flakes size]
  (cond-> {:id      dbid ;; db's unique identifier
           :t       t
           :address db-address ;; address to locate db
           :flakes  flakes
           :size    size}
          (not-empty prev-data) (assoc :previous prev-data)))

(defn data
  "Given a commit map, returns them most recent data map."
  [commit]
  (:data commit))

(defn data-id
  "Given a commit, returns the id of the most recent (previous) data id"
  [commit]
  (let [commit-data (data commit)]
    (:id commit-data)))

(defn new-db-commit-map
  "Returns a commit map with a new db registered.
  Assumes commit is not yet created (but db is persisted), so
  commit-id and commit-address are added after finalizing and persisting commit."
  [{:keys [old-commit issuer message tag dbid t db-address flakes size author
           txn-id annotation time]
    :as   _commit}]
  (let [prev-data   (select-keys (data old-commit) [:id :address])
        data-commit (new-db-commit dbid t db-address prev-data flakes size)
        prev-commit (not-empty (select-keys old-commit [:id :address]))
        commit      (-> old-commit
                        (dissoc :id :address :data :issuer :time :message :tag
                                :prev-commit)
                        (assoc :id ""
                               :address ""
                               :v commit-version
                               :data data-commit
                               :time time))]
    (cond-> commit
            txn-id (assoc :txn txn-id)
            author (assoc :author author)
            issuer (assoc :issuer {:id issuer})
            prev-commit (assoc :previous prev-commit)
            message (assoc :message message)
            annotation (assoc :annotation annotation)
            tag (assoc :tag tag))))

(defn ref?
  [f]
  (-> f
      flake/dt
      (= const/$id)))

(defn ref-flakes
  "Returns ref flakes from set of all flakes. Uses Flake datatype to know if a ref."
  [flakes]
  (filter ref? flakes))

;; TODO - flakes-size takes considerable time for lg txns, see if can be optimized
(defn calc-flake-size
  [add rem]
  (cond-> 0
          add (+ (flake/size-bytes add))
          rem (- (flake/size-bytes rem))))

(defn update-novelty
  ([db add]
   (update-novelty db add []))

  ([{:keys [t] :as db} add rem]
   (try*
     (let [flake-count (cond-> 0
                               add (+ (count add))
                               rem (- (count rem)))
           ;; launch futures for parallellism on JVM
           flake-size  #?(:clj  (future (calc-flake-size add rem))
                          :cljs (calc-flake-size add rem))
           post        #?(:clj  (future (flake/revise (get-in db [:novelty :post]) add rem))
                          :cljs (flake/revise (get-in db [:novelty :post]) add rem))
           opst        #?(:clj  (future (flake/revise (get-in db [:novelty :opst]) (ref-flakes add) (ref-flakes rem)))
                          :cljs (flake/revise (get-in db [:novelty :opst]) (ref-flakes add) (ref-flakes rem)))]
       (-> db
           (update-in [:novelty :spot] flake/revise add rem)
           (update-in [:novelty :tspo] flake/revise add rem)
           (assoc-in [:novelty :post] #?(:clj  @post
                                         :cljs post))
           (assoc-in [:novelty :opst] #?(:clj  @opst
                                         :cljs opst))
           (update-in [:novelty :size] + #?(:clj  @flake-size
                                            :cljs flake-size))
           (assoc-in [:novelty :t] t)
           (update-in [:stats :size] + #?(:clj  @flake-size
                                          :cljs flake-size))
           (update-in [:stats :flakes] + flake-count)))
     (catch* e
             (log/error (str "Update novelty unexpected error while attempting to updated db: "
                             (pr-str db) " due to exception: " (ex-message e))
                        {:add-flakes add
                         :rem-flakes rem})
       (throw e)))))

(defn add-tt-id
  "Associates a unique tt-id for any in-memory staged db in their index roots.
  tt-id is used as part of the caching key, by having this in place it means
  that even though the 't' value hasn't changed it will cache each stage db
  data as its own entity."
  [db]
  (let [tt-id   (random-uuid)
        indexes [:spot :post :opst :tspo]]
    (-> (reduce
          (fn [db* idx]
            (let [{:keys [children] :as node} (get db* idx)
                  children* (reduce-kv
                              (fn [children* k v]
                                (assoc children* k (assoc v :tt-id tt-id)))
                              (empty children) children)]
              (assoc db* idx (assoc node :tt-id tt-id
                                         :children children*))))
          db indexes)
        (assoc :tt-id tt-id))))

(defn commit-metadata-flakes
  "Builds and returns the commit metadata flakes for the given commit, t, and
  db-sid. Used when committing to an in-memory ledger value and when reifying
  a ledger from storage on load."
  [db t commit]
  (let [{:keys [id address alias branch data time v previous author issuer message txn]} commit
        {db-t :t, db-address :address, data-id :id, :keys [flakes size]} data
        commit-sid (iri/encode-iri db id)
        db-sid     (iri/encode-iri db data-id)]
    (cond->
     [;; commit flakes
      ;; address
      (flake/create commit-sid const/$_address address const/$xsd:string t true nil)
      ;; alias
      (flake/create commit-sid const/$_ledger:alias alias const/$xsd:string t true nil)
      ;; branch
      (flake/create commit-sid const/$_ledger:branch branch const/$xsd:string t true nil)
      ;; v
      (flake/create commit-sid const/$_v v const/$xsd:int t true nil)
      ;; time
      (flake/create commit-sid const/$_commit:time (util/str->epoch-ms time) const/$xsd:long t true nil)
      ;; data
      (flake/create commit-sid const/$_commit:data db-sid const/$id t true nil)

      ;; db flakes
      ;; t
      (flake/create db-sid const/$_commitdata:t db-t const/$xsd:int t true nil)
      ;; address
      (flake/create db-sid const/$_address db-address const/$xsd:string t true nil)
      ;; size
      (flake/create db-sid const/$_commitdata:size size const/$xsd:int t true nil)
      ;; flakes
      (flake/create db-sid const/$_commitdata:flakes flakes const/$xsd:int t true nil)]

     (:id previous)
     (conj (flake/create commit-sid const/$_previous (iri/encode-iri db (:id previous)) const/$id t true nil))

     (:id issuer)
     (conj (flake/create commit-sid const/$_commit:signer (iri/encode-iri db (:id issuer)) const/$id t true nil))

     message
     (conj (flake/create commit-sid const/$_commit:message message const/$xsd:string t true nil))

     ;; TODO - author should really be an IRI, not a string
     author
     (conj (flake/create commit-sid const/$_commit:author author const/$xsd:string t true nil))

     ;; TODO - txn should really be an IRI, not a string
     txn
     (conj (flake/create commit-sid const/$_commit:txn txn const/$xsd:string t true nil)))))

(defn annotation-flakes
  [db t commit-sid annotation]
  (if annotation
    (let [allowed-vars #{}
          parsed       (q-parse/parse-triples (util/sequential annotation) allowed-vars nil)
          a-sid        (->> parsed ffirst where/get-iri (iri/encode-iri db))
          db-vol       (volatile! db)
          flakes       (into [(flake/create commit-sid const/$_commit:annotation a-sid const/$id t true nil)]
                             (map (partial update/build-flake db-vol t))
                             parsed)]
      [@db-vol flakes])
    [db]))

(defn add-commit-flakes
  "Translate commit metadata into flakes and merge them into novelty."
  [{:keys [commit] :as db}]
  (let [{:keys [data id annotation]} commit
        t (:t data)
        commit-sid         (iri/encode-iri db id)
        base-flakes        (commit-metadata-flakes db t commit)

        ;; TODO - if annotation flakes exist, do they also need to get created when doing a `load`?
        [db* annotation-flakes] (annotation-flakes db t commit-sid annotation)

        commit-flakes      (cond-> base-flakes
                                   annotation-flakes (into annotation-flakes))]
    (-> db*
        (update-novelty commit-flakes)
        add-tt-id)))
