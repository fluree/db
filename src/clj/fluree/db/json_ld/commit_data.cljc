(ns fluree.db.json-ld.commit-data
  (:require [fluree.crypto :as crypto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]))

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
   ["v" 0]
   ["address" :address]
   ["type" ["Commit"]]
   ["alias" :alias]
   ["issuer" :issuer]
   ["author" :author]
   ["txn" :txn]
   ["branch" :branch]
   ["time" :time]
   ["tag" :tag]
   ["message" :message]
   ["previous" :previous] ;; refer to :prev-commit template
   ["data" :data] ;; refer to :data template
   ["ns" :ns] ;; refer to :ns template
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
  [{:keys [previous data ns index issuer] :as commit-map}]
  (let [prev-data   (when (not-empty (:previous data))
                      (merge-template (:previous data) json-ld-prev-data-template))
        commit-map* (assoc commit-map
                      :previous (merge-template previous json-ld-prev-commit-template)
                      :data (merge-template (assoc data :previous prev-data) json-ld-data-template)
                      :issuer (merge-template issuer json-ld-issuer-template)
                      :ns (mapv #(merge-template % json-ld-ns-template) ns)
                      :index (-> (merge-template (:data index) json-ld-data-template) ;; index has an embedded db map
                                 (#(assoc index :data %))
                                 (merge-template json-ld-index-template)))]
    (merge-template commit-map* json-ld-base-template)))

(defn parse-db-data
  [data]
  {:id      (:id data)
   :t       (get-first-value data const/iri-t)
   :address (get-first-value data const/iri-address)
   :flakes  (get-first-value data const/iri-flakes)
   :size    (get-first-value data const/iri-size)})

(defn json-ld->map
  "Turns json-ld commit meta into the clojure map structure."
  [commit-json-ld {:keys [commit-address spot post opst tspo]}]
  (let [id          (:id commit-json-ld)
        address     (-> commit-json-ld
                        (get-first-value const/iri-address)
                        not-empty
                        (or commit-address)) ; address, if using something like
                                             ; IPFS, is empty string
        v           (get-first-value commit-json-ld const/iri-v)
        alias       (get-first-value commit-json-ld const/iri-alias)
        branch      (get-first-value commit-json-ld const/iri-branch)

        time        (get-first-value commit-json-ld const/iri-time)
        message     (get-first-value commit-json-ld const/iri-message)
        tags        (get-first commit-json-ld const/iri-tag)
        issuer      (get-first commit-json-ld const/iri-issuer)
        prev-commit (get-first commit-json-ld const/iri-previous)
        data        (get-first commit-json-ld const/iri-data)
        ns          (get-first commit-json-ld const/iri-ns)

        index       (get-first commit-json-ld const/iri-index)]
    (cond-> {:id             id
             :address        address
             :v              v
             :alias          alias
             :branch         branch
             :time           time
             :message        message
             :tag            (mapv :value tags)
             :previous       {:id      (:id prev-commit)
                              :address (get-first-value prev-commit const/iri-address)}
             :data           (parse-db-data data)}
            ns (assoc :ns (->> ns
                               util/sequential
                               (mapv (fn [namespace] {:id (:id namespace)}))))
            index (assoc :index {:id      (:id index)
                                 :address (get-first-value index const/iri-address)
                                 :data    (parse-db-data (get-first index const/iri-data))
                                 :spot    spot
                                 :post    post
                                 :opst    opst
                                 :tspo    tspo})
            issuer (assoc :issuer {:id (:id issuer)}))))


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
  [alias branch ns-addresses]
  {:alias  alias
   :v      0
   :branch (if branch
             (util/keyword->str branch)
             "main")
   :ns     (mapv #(if (map? %)
                    %
                    {:id %})
                 ns-addresses)})

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
  [{:keys [old-commit issuer message tag dbid t db-address flakes size author txn-id]
    :as   _commit}]
  (let [prev-data   (select-keys (data old-commit) [:id :address])
        data-commit (new-db-commit dbid t db-address prev-data flakes size)
        prev-commit (not-empty (select-keys old-commit [:id :address]))
        commit      (-> old-commit
                        (dissoc :id :address :data :issuer :time :message :tag :prev-commit)
                        (assoc :address ""
                               :author author
                               :txn  txn-id
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
         (update-in [:novelty :spot] flake/revise add rem)
         (update-in [:novelty :post] flake/revise add rem)
         (update-in [:novelty :opst] flake/revise ref-add ref-rem)
         (update-in [:novelty :tspo] flake/revise add rem)
         (update-in [:novelty :size] + flake-size)
         (update-in [:stats :size] + flake-size)
         (update-in [:stats :flakes] + flake-count)))))

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
  [{:keys [address alias branch data id time v author txn] :as _commit} t commit-sid db-sid]
  (let [{db-id :id db-t :t db-address :address :keys [flakes size]} data]
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
     (flake/create commit-sid const/$_commit:time (util/str->epoch-ms time) const/$xsd:long t true nil) ;; data
     (flake/create commit-sid const/$_commit:data db-sid const/$xsd:anyURI t true nil)
     ;; author
     (flake/create commit-sid const/$_commit:author author const/$xsd:string t true nil)
     ;; txn
     (flake/create commit-sid const/$_commit:txn txn const/$xsd:string t true nil)

     ;; db flakes
     ;; t
     (flake/create db-sid const/$_commitdata:t db-t const/$xsd:int t true nil)
     ;; address
     (flake/create db-sid const/$_address db-address const/$xsd:string t true nil)
     ;; size
     (flake/create db-sid const/$_commitdata:size size const/$xsd:int t true nil)
     ;; flakes
     (flake/create db-sid const/$_commitdata:flakes flakes const/$xsd:int t true
                   nil)]))

(defn prev-commit-flakes
  "Builds and returns a channel containing the previous commit flakes for the
  given db, t, and previous-id (the id of a commit's previous commit). Used when
  committing to an in-memory ledger and when reifying a ledger from storage on
  load."
  [db t commit-sid previous-id]
  (let [prev-sid (iri/encode-iri db previous-id)]
    [(flake/create commit-sid const/$_previous prev-sid const/$xsd:anyURI t true nil)]))

(defn prev-data-flakes
  "Builds and returns a channel containing the previous data flakes for the
  given db, db-sid, t, and prev-data-id (the id of commit's data section's
  previous pointer). Used when committing to an in-memory ledger value and when
  reifying a ledger from storage on load."
  [db db-sid t prev-data-id]
  (let [prev-sid (iri/encode-iri db prev-data-id)]
    [(flake/create db-sid const/$_previous prev-sid const/$xsd:anyURI t true nil)]))

(defn issuer-flakes
  "Builds and returns a channel containing the credential issuer's flakes for
  the given db, t, next-sid, and issuer-iri. next-sid should be a zero-arity fn
  that returns the next subject id to use if the issuer doesn't already exist in
  the db and that updates some internal state to reflect that this one is now
  used. It will only be called if needed. Used when committing to an in-memory
  ledger value and when reifying a ledger from storage on load."
  [db t commit-sid issuer-iri]
  (if-let [issuer-sid (iri/encode-iri db issuer-iri)]
    ;; create reference to existing issuer
    [(flake/create commit-sid const/$_commit:signer issuer-sid const/$xsd:anyURI t true
                   nil)]
    ;; create new issuer flake and a reference to it
    (let [new-issuer-sid (iri/encode-iri db issuer-iri)]
      [(flake/create commit-sid const/$_commit:signer new-issuer-sid const/$xsd:anyURI t
                     true nil)])))

(defn message-flakes
  "Builds and returns the commit message flakes for the given t and message.
  Used when committing to an in-memory ledger value and when reifying a ledger
  from storage on load."
  [t commit-sid message]
  [(flake/create commit-sid const/$_commit:message message const/$xsd:string t true nil)])


(defn add-commit-flakes
  "Translate commit metadata into flakes and merge them into novelty."
  [prev-commit {:keys [commit] :as db}]
  (let [{:keys [data id issuer message]} commit
        {db-t :t, db-id :id} data

        {previous-id :id prev-data :data} prev-commit
        prev-data-id       (:id prev-data)

        t                  db-t
        commit-sid         (iri/encode-iri db id)
        db-sid             (iri/encode-iri db db-id)
        base-flakes        (commit-metadata-flakes commit t commit-sid db-sid)
        prev-commit-flakes (when previous-id
                             (prev-commit-flakes db t commit-sid previous-id))
        prev-db-flakes     (when prev-data-id
                             (prev-data-flakes db db-sid t prev-data-id))
        issuer-flakes      (when-let [issuer-iri (:id issuer)]
                             (issuer-flakes db t commit-sid issuer-iri))
        message-flakes     (when message
                             (message-flakes t commit-sid message))
        commit-flakes      (cond-> base-flakes
                             prev-commit-flakes (into prev-commit-flakes)
                             prev-db-flakes (into prev-db-flakes)
                             issuer-flakes (into issuer-flakes)
                             message-flakes (into message-flakes))]
    (-> db
        (update-novelty commit-flakes)
        add-tt-id)))
