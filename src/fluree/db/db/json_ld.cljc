(ns fluree.db.db.json-ld
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.query.fql :as fql]
            [fluree.db.index :as index]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try]]
            #?(:clj  [clojure.core.async :refer [go] :as async]
               :cljs [cljs.core.async :refer [go] :as async])
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.commit-data :as commit-data])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {:f/view   {:root? true}
   :f/modify {:root? true}})


(defn validate-ledger-name
  "Returns when ledger name is valid.
  Otherwise throws."
  [ledger-id type]
  (when-not (re-matches #"^[a-z0-9-]{1,100}" ledger-id)
    (throw (ex-info (str "Invalid " type " id: " ledger-id ". Must match a-z0-9- and be no more than 100 characters long.")
                    {:status 400 :error :db/invalid-db}))))

;; TODO - we should make the names more restrictive
(defn validate-ledger-ident
  "Returns two-tuple of [network name-or-dbid] if db-ident is valid.

  Will ignore a direct db name reference (prefixed with '_')
  Otherwise throws."
  [ledger]
  (let [[_ network maybe-alias] (re-find #"^([^/]+)/(?:_)?([^/]+)$" (util/keyword->str ledger))]
    (if (and network maybe-alias)
      [network maybe-alias]
      (throw (ex-info (str "Invalid ledger identity: " ledger)
                      {:status 400 :error :db/invalid-ledger-name})))))

;; exclude these predicates from the database
(def ^:const exclude-predicates #{const/$_tx:tx const/$_tx:sig const/$_tx:tempids})

(defn add-predicate-to-idx
  "Adds a predicate to post index when :index true is turned on.
  Ensures adding the predicate into novelty won't blow past novelty-max.
  When reindex? is true, we are doing a full reindex and allow the novelty
  to grow beyond novelty-max."
  [db pred-id {:keys [reindex?] :as opts}]
  (go-try
    (let [flakes-to-idx (<? (query-range/index-range db :psot = [pred-id]))
          {:keys [post size]} (:novelty db)
          with-size     (flake/size-bytes flakes-to-idx)
          total-size    (+ size with-size)
          {:keys [novelty-min novelty-max]} (get-in db [:conn :meta])
          _             (if (and (> total-size novelty-max) (not reindex?))
                          (throw (ex-info (str "You cannot add " pred-id " to the index at this point. There are too many affected flakes.")
                                          {:error  :db/max-novelty-size
                                           :status 400})))
          post          (into post flakes-to-idx)]
      (swap! (:schema-cache db) empty)
      (-> db
          (assoc-in [:novelty :post] post)
          (assoc-in [:novelty :size] total-size)))))


(defn- with-db-size
  "Calculates db size. On JVM, would typically be used in a separate thread for concurrency"
  [current-size flakes]
  (+ current-size (flake/size-bytes flakes)))


(defn- with-t-novelty
  [db flakes flakes-bytes]
  (let [{:keys [novelty schema]} db
        pred-map (:pred schema)
        {:keys [spot psot post opst size]} novelty
        res      {:spot (into spot flakes)
                  :psot (into psot flakes)
                  :post (into post flakes)
                  :opst (->> flakes
                             (sort-by flake/p)
                             (partition-by flake/p)
                             (reduce
                               (fn [opst* p-flakes]
                                 (if (get-in pred-map [(-> p-flakes first flake/p) :ref?])
                                   (into opst* p-flakes)
                                   opst*))
                               opst))
                  :size (+ size #?(:clj @flakes-bytes :cljs flakes-bytes))}]
    res))


(defn- with-t-ecount
  "Calculates updated ecount based on flakes for with-t. Also records if a schema or settings change
  occurred."
  [{:keys [ecount schema] :as db} flakes]
  (loop [[flakes-s & r] (partition-by flake/s flakes)
         schema-change?  (boolean (nil? schema))            ;; if no schema for any reason, make sure one is generated
         setting-change? false
         ecount          ecount]
    (if flakes-s
      (let [sid (-> flakes-s first flake/s)
            cid (flake/sid->cid sid)]
        (recur r
               (if (true? schema-change?)
                 schema-change?
                 (boolean (schema-util/is-schema-sid? sid)))
               (if (true? setting-change?)
                 setting-change?
                 (schema-util/is-setting-sid? sid))
               (update ecount cid #(if % (max % sid) sid))))
      {:schema-change?  schema-change?
       :setting-change? setting-change?
       :ecount          ecount})))


(defn- with-t-updated-schema
  "If the schema changed, there may be also be new flakes with the transaction that rely on those
  schema changes. Re-run novelty with the updated schema so things make it into the proper indexes.

  This is not common, so while this duplicates the novelty work, in most circumstances
  it allows novelty to be run in parallel and this function is never triggered."
  [proposed-db flakes flake-bytes]
  (go-try
    (let [schema  (<? (vocab/vocab-map proposed-db))
          novelty (with-t-novelty (assoc proposed-db :schema schema) flakes flake-bytes)]
      (assoc proposed-db :schema schema
                         :novelty novelty))))


(defn- with-t-updated-settings
  "If settings changed, return new settings map."
  [proposed-db]
  (go-try
    (assoc proposed-db :settings (<? (schema/setting-map proposed-db)))))


(defn with-t
  ([db flakes] (with-t db flakes nil))
  ([{:keys [stats t] :as db} flakes opts]
   (go-try
     (let [new-t           (-> flakes first flake/t)
           _               (when (not= new-t (dec t))
                             (throw (ex-info (str "Invalid with called for db " (:dbid db) " because current 't', " t " is not beyond supplied transaction t: " new-t ".")
                                             {:status 500
                                              :error  :db/unexpected-error})))
           bytes #?(:clj   (future (flake/size-bytes flakes)) ;; calculate in separate thread for CLJ
                    :cljs (flake/size-bytes flakes))
           novelty #?(:clj (future (with-t-novelty db flakes bytes)) ;; calculate in separate thread for CLJ
                      :cljs (with-t-novelty db flakes bytes))
           {:keys [schema-change? setting-change? ecount]} (with-t-ecount db flakes)
           stats*          (-> stats
                               (update :size + #?(:clj @bytes :cljs bytes)) ;; total db ~size
                               (update :flakes + (count flakes)))]
       (cond-> (assoc db :t new-t
                         :novelty #?(:clj @novelty :cljs novelty)
                         :ecount ecount
                         :stats stats*)
               schema-change? (-> (with-t-updated-schema flakes bytes) <?)
               setting-change? (-> with-t-updated-settings <?))))))


(defn with
  "Returns db 'with' flakes added as a core async promise channel.
  Note this always does a re-sort."
  ([db block flakes] (with db block flakes nil))
  ([db block flakes opts]
   (let [resp-ch (async/promise-chan)]
     (async/go
       (try*
         (if (empty? flakes)
           (async/put! resp-ch db)
           (let [flakes-by-t (->> flakes
                                  (sort flake/cmp-flakes-block)
                                  (partition-by :t))]
             (loop [[t-flakes & r] flakes-by-t
                    db db]
               (if t-flakes
                 (let [db' (<? (with-t db t-flakes opts))]
                   (recur r db'))
                 (async/put! resp-ch db)))))
         (catch* e
                 (async/put! resp-ch e))))
     resp-ch)))

(defn forward-time-travel-db?
  "Returns true if db is a forward time travel db."
  [db]
  (not (nil? (:tt-id db))))

(defn forward-time-travel
  "Returns a core async chan with a new db based on the provided db, including the provided flakes.
  Flakes can contain one or more 't's, but should be sequential and start after the current
  't' of the provided db. (i.e. if db-t is -14, flakes 't' should be -15, -16, etc.).
  Remember 't' is negative and thus should be in descending order.

  A tt-id (time-travel-id), if provided, can be any unique identifier of any type and is required.
  It must be unique (to the computer/process) to avoid any query caching issues.

  A forward-time-travel dbf can be further forward-time-traveled. If a tt-id is provided, ensure
  it is unique for each successive call.

  A forward-time travel DB is held in memory, and is not shared across servers. Ensure you
  have adequate memory to hold the flakes you generate and add. If access is provided via
  an external API, do any desired size restrictions or controls within your API endpoint.

  Remember schema operations done via forward-time-travel should be done in a 't' prior to
  the flakes that end up requiring the schema change."
  [db tt-id flakes]
  (go-try
    (let [tt-id'      (if (nil? tt-id) (random-uuid) tt-id)
          ;; update each root index with the provided tt-id
          ;; As the root indexes are resolved, the tt-id will carry through the b-tree and ensure
          ;; query caching is specific to this tt-id
          db'         (reduce (fn [db* idx]
                                (assoc db* idx (-> (get db* idx)
                                                   (assoc :tt-id tt-id'))))
                              (assoc db :tt-id tt-id')
                              [:spot :psot :post :opst])
          flakes-by-t (->> flakes
                           (sort-by :t)
                           reverse
                           (partition-by :t))]
      (loop [db db'
             [flakes & rest] flakes-by-t]
        (if flakes
          (recur (<? (with-t db flakes)) rest)
          db)))))

(defn expand-iri
  "Expands an IRI from the db's context."
  ([{:keys [schema] :as _db} iri]
   (json-ld/expand-iri iri (:context schema)))
  ([{:keys [schema] :as _db} iri provided-context]
   (->> (json-ld/parse-context (:context schema) provided-context)
        (json-ld/expand-iri iri))))

(defn iri->sid
  "Returns subject id or nil if no match.

  iri can be compact iri in string or keyword form."
  [db iri]
  (go-try
    (let [iri* (expand-iri db iri)]
      ;; string? necessary because expand-iri will return original iri if not matched, and could be a keyword
      (when (string? iri*)
        (some-> (<? (query-range/index-range db :post = [const/$iri iri*]))
                first
                flake/s)))))

(defn subid
  "Returns subject ID of ident as async promise channel.
  Closes channel (nil) if doesn't exist, or if strict? is true, will return exception."
  [db ident strict?]
  (let [return-chan (async/promise-chan)]
    (go
      (try*
        (let [res (cond (number? ident)
                        (when (not-empty (<? (query-range/index-range db :spot = [ident])))
                          ident)

                        ;; assume iri
                        (string? ident)
                        (<? (iri->sid db ident))

                        ;; assume iri that needs to be expanded (should we allow this, or should it be expanded before getting this far?)
                        (keyword? ident)
                        (<? (iri->sid db ident))

                        ;; TODO - should we validate this is an ident predicate? This will return first result of any indexed value
                        (util/pred-ident? ident)
                        (if-let [pid (dbproto/-p-prop db :id (first ident))]
                          (some-> (<? (query-range/index-range db :post = [pid (second ident)]))
                                  first
                                  flake/s)
                          (throw (ex-info (str "Subject ID lookup failed. The predicate " (pr-str (first ident)) " does not exist.")
                                          {:status 400
                                           :error  :db/invalid-ident})))

                        :else
                        (throw (ex-info (str "Entid lookup must be a number or valid two-tuple identity: " (pr-str ident))
                                        {:status 400
                                         :error  :db/invalid-ident})))]
          (cond
            res
            (async/put! return-chan res)

            (and (nil? res) strict?)
            (async/put! return-chan
                        (ex-info (str "Subject identity does not exist: " ident)
                                 {:status 400 :error :db/invalid-subject}))

            :else
            (async/close! return-chan)))

        (catch* e
                (async/put! return-chan
                            (ex-info (str "Error looking up subject id: " ident)
                                     {:status 400 :error :db/invalid-subject}
                                     e)))))
    return-chan))

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db subject-id]
  (go-try
    (map flake/o
         (-> (dbproto/-rootdb db)
             (query-range/index-range :spot = [subject-id const/$rdf:type])
             <?))))

(defn iri
  "Returns the iri for a given subject ID"
  [db subject-id compact-fn]
  (go-try
    (when-let [flake (first (<? (query-range/index-range db :spot = [subject-id 0])))]
      (-> flake flake/o compact-fn))))

;; ================ GraphDB record support fns ================================

(defn- graphdb-latest-db [{:keys [current-db-fn policy]}]
  (go-try
    (let [current-db (<? (current-db-fn))]
      (assoc current-db :policy policy))))

(defn- graphdb-root-db [this]
  (assoc this :policy root-policy-map))

(defn- graphdb-c-prop [{:keys [schema]} property collection]
  ;; collection properties TODO-deprecate :id property below in favor of :partition
  (assert (#{:name :id :sid :partition :spec :specDoc :base-iri} property)
          (str "Invalid collection property: " (pr-str property)))
  (if (neg-int? collection)
    (get-in schema [:coll "_tx" property])
    (get-in schema [:coll collection property])))

(defn- graphdb-p-prop [{:keys [schema] :as this} property predicate]
  (assert (#{:name :id :iri :type :ref? :idx? :unique :multi :index :upsert
             :component :noHistory :restrictCollection :spec :specDoc :txSpec
             :txSpecDoc :restrictTag :retractDuplicates :subclassOf :new?} property)
          (str "Invalid predicate property: " (pr-str property)))
  (cond->> (get-in schema [:pred predicate property])
           (= :restrictCollection property) (dbproto/-c-prop this :partition)))

(defn- graphdb-pred-name
  "Lookup the predicate name if needed; return ::no-pred if pred arg is nil so
  we can differentiate between that and (dbproto/-p-prop ...) returning nil"
  [this pred]
  (cond
    (nil? pred) ::no-pred
    (string? pred) pred
    :else (dbproto/-p-prop this :name pred)))

(defn- graphdb-tag
  "resolves a tags's value given a tag subject id; optionally shortening the
  return value if it starts with the given predicate name"
  ([this tag-id]
   (go-try
     (let [tag-pred-id 30]
       (some-> (<? (query-range/index-range (dbproto/-rootdb this)
                                            :spot = [tag-id tag-pred-id]))
               first
               flake/o))))
  ([this tag-id pred]
   (go-try
     (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))
           tag       (<? (dbproto/-tag this tag-id))]
       (when (and pred-name tag)
         (if (str/includes? tag ":")
           (-> (str/split tag #":") second)
           tag))))))

(defn- graphdb-tag-id
  ([this tag-name]
   (go-try
     (let [tag-pred-id const/$_tag:id]
       (some-> (<? (query-range/index-range (dbproto/-rootdb this) :post = [tag-pred-id tag-name]))
               first
               flake/s))))
  ([this tag-name pred]
   (go-try
     (if (str/includes? tag-name "/")
       (<? (dbproto/-tag-id this tag-name))
       (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))]
         (when pred-name
           (<? (dbproto/-tag-id this (str pred-name ":" tag-name)))))))))

(defn index-update
  "If provided commit-index is newer than db's commit index, updates db by cleaning novelty.
  If it is not newer, returns original db."
  [{:keys [ledger commit] :as db} {data-map :data, :keys [spot psot post opst tspo] :as commit-index}]
  (let [index-t      (:t data-map)
        newer-index? (and data-map
                          (or (nil? (commit-data/index-t commit))
                              (> index-t (commit-data/index-t commit))))]
    (if newer-index?
      (-> db
          (assoc :commit (assoc commit :index commit-index)
                 :novelty* (idx-proto/-empty-novelty (:indexer ledger) db (- index-t))
                 :spot spot
                 :psot psot
                 :post post
                 :opst opst
                 :tspo tspo)
          (assoc-in [:stats :indexed] index-t))
      db)))

;; ================ end GraphDB record support fns ============================

;; TODO - conn is included here because current index-range query looks for conn on the db
;; TODO - this can likely be excluded once index-range is changed to get 'conn' from (:conn ledger) where it also exists
(defrecord JsonLdDb [ledger conn method alias branch commit block t tt-id stats
                     spot psot post opst tspo schema comparators novelty
                     policy ecount]
  dbproto/IFlureeDb
  (-latest-db [this] (graphdb-latest-db this))
  (-rootdb [this] (graphdb-root-db this))
  (-forward-time-travel [db flakes] (forward-time-travel db nil flakes))
  (-forward-time-travel [db tt-id flakes] (forward-time-travel db tt-id flakes))
  (-c-prop [this property collection] (graphdb-c-prop this property collection))
  (-class-prop [this property class]
    (if (= :subclasses property)
      (get @(:subclasses schema) class)
      (get-in schema [:pred class property])))
  (-p-prop [this property predicate] (graphdb-p-prop this property predicate))
  (-expand-iri [this compact-iri] (expand-iri this compact-iri))
  (-expand-iri [this compact-iri context] (expand-iri this compact-iri context))
  (-tag [this tag-id] (graphdb-tag this tag-id))
  (-tag [this tag-id pred] (graphdb-tag this tag-id pred))
  (-tag-id [this tag-name] (graphdb-tag-id this tag-name))
  (-tag-id [this tag-name pred] (graphdb-tag-id this tag-name pred))
  (-subid [this ident] (subid this ident false))
  (-subid [this ident strict?] (subid this ident strict?))
  (-class-ids [this subject] (class-ids this subject))
  (-iri [this subject-id] (iri this subject-id identity))
  (-iri [this subject-id compact-fn] (iri this subject-id compact-fn))
  (-search [this fparts] (query-range/search this fparts))
  (-query [this query-map] (fql/query this query-map))
  (-with [this block flakes] (with this block flakes nil))
  (-with [this block flakes opts] (with this block flakes opts))
  (-with-t [this flakes] (with-t this flakes nil))
  (-with-t [this flakes opts] (with-t this flakes opts))
  (-add-predicate-to-idx [this pred-id] (add-predicate-to-idx this pred-id nil))
  (-db-type [_] :json-ld)
  (-stage [db json-ld] (jld-transact/stage db json-ld nil))
  (-stage [db json-ld opts] (jld-transact/stage db json-ld opts))
  (-index-update [db commit-index] (index-update db commit-index)))

#?(:cljs
   (extend-type JsonLdDb
     IPrintWithWriter
     (-pr-writer [db w opts]
       (-write w "#FlureeJsonLdDb ")
       (-write w (pr {:method      (:method db) :alias (:alias db) :block (:block db)
                      :t           (:t db) :stats (:stats db)
                      :policy      (:policy db)})))))

#?(:clj
   (defmethod print-method JsonLdDb [^JsonLdDb db, ^Writer w]
     (.write w (str "#FlureeJsonLdDb "))
     (binding [*out* w]
       (pr {:method (:method db) :alias (:alias db) :block (:block db)
            :t      (:t db) :stats (:stats db) :policy (:policy db)}))))

(defn new-novelty-map
  [comparators]
  (reduce
    (fn [m idx]
      (assoc m idx (-> comparators
                       (get idx)
                       flake/sorted-set-by)))
    {:size 0} index/types))

(def genesis-ecount {const/$_predicate  (flake/->sid const/$_predicate 1000)
                     const/$_collection (flake/->sid const/$_collection 19)
                     const/$_tag        (flake/->sid const/$_tag 1000)
                     const/$_fn         (flake/->sid const/$_fn 1000)
                     const/$_user       (flake/->sid const/$_user 1000)
                     const/$_auth       (flake/->sid const/$_auth 1000)
                     const/$_role       (flake/->sid const/$_role 1000)
                     const/$_rule       (flake/->sid const/$_rule 1000)
                     const/$_setting    (flake/->sid const/$_setting 1000)
                     const/$_prefix     (flake/->sid const/$_prefix 1000)
                     const/$_shard      (flake/->sid const/$_shard 1000)})


(defn create
  [{:keys [method alias conn] :as ledger}]
  (let [novelty     (new-novelty-map index/default-comparators)
        {spot-cmp :spot
         psot-cmp :psot
         post-cmp :post
         opst-cmp :opst
         tspo-cmp :tspo} index/default-comparators

        spot        (index/empty-branch method alias spot-cmp)
        psot        (index/empty-branch method alias psot-cmp)
        post        (index/empty-branch method alias post-cmp)
        opst        (index/empty-branch method alias opst-cmp)
        tspo        (index/empty-branch method alias tspo-cmp)
        stats       {:flakes 0, :size 0, :indexed 0}
        schema      (vocab/base-schema)
        branch      (branch/branch-meta ledger)]
    (map->JsonLdDb {:ledger      ledger
                    :conn        conn
                    :method      method
                    :alias       alias
                    :branch      (:name branch)
                    :commit      (:commit branch)
                    :block       0
                    :t           0
                    :tt-id       nil
                    :stats       stats
                    :spot        spot
                    :psot        psot
                    :post        post
                    :opst        opst
                    :tspo        tspo
                    :schema      schema
                    :comparators index/default-comparators
                    :novelty     novelty
                    :policy      root-policy-map
                    :ecount      genesis-ecount})))
