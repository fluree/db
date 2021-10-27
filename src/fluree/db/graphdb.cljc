(ns fluree.db.graphdb
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.query.fql :as fql]
            [fluree.db.index :as index]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [clojure.string :as str]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (fluree.db.flake Flake)
                   (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

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
        {:keys [spot psot post opst size]} novelty]
    (loop [[[p p-flakes] & r] (group-by #(.-p ^Flake %) flakes)
           spot (transient spot)
           psot (transient psot)
           post (transient post)
           opst (transient opst)]
      (if p-flakes
        (let [exclude? (exclude-predicates p)
              {:keys [idx? ref?]} (get pred-map p)]
          (if exclude?
            (recur r spot psot post opst)
            (recur r
                   (reduce conj! spot p-flakes)
                   (reduce conj! psot p-flakes)
                   (if idx?
                     (reduce conj! post p-flakes)
                     post)
                   (if ref?
                     (reduce conj! opst p-flakes)
                     opst))))
        {:spot (persistent! spot)
         :psot (persistent! psot)
         :post (persistent! post)
         :opst (persistent! opst)
         :size (+ size #?(:clj @flakes-bytes :cljs flakes-bytes))}))))


(defn- with-t-ecount
  "Calculates updated ecount based on flakes for with-t. Also records if a schema or settings change
  occurred."
  [{:keys [ecount schema] :as db} flakes]
  (loop [[flakes-s & r] (partition-by #(.-s ^Flake %) flakes)
         schema-change?  (boolean (nil? schema))            ;; if no schema for any reason, make sure one is generated
         setting-change? false
         ecount          ecount]
    (if flakes-s
      (let [sid (.-s ^Flake (first flakes-s))
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


(defn- with-t-add-pred-idx
  "If the schema changed and existing predicates are newly marked as :index true or :unique true they
   must be added to novelty (if novelty-max is not exceeded)."
  [proposed-db before-db flakes opts]
  (go-try
    (let [pred-ecount      (-> before-db :ecount (get const/$_predicate))
          add-pred-to-idx? (schema-util/add-to-post-preds? flakes pred-ecount)]
      (if (seq add-pred-to-idx?)
        (loop [[add-pred & r] add-pred-to-idx?
               db proposed-db]
          (if add-pred
            (recur r (<? (add-predicate-to-idx db add-pred opts)))
            db))
        proposed-db))))


(defn- with-t-updated-schema
  "If the schema changed, there may be also be new flakes with the transaction that rely on those
  schema changes. Re-run novelty with the updated schema so things make it into the proper indexes.

  This is not common, so while this duplicates the novelty work, in most circumstances
  it allows novelty to be run in parallel and this function is never triggered."
  [proposed-db before-db flakes flake-bytes opts]
  (go-try
    (let [schema-map (<? (schema/schema-map proposed-db))
          novelty    (with-t-novelty (assoc before-db :schema schema-map) flakes flake-bytes)]
      (-> proposed-db
          (assoc :schema schema-map
                 :novelty novelty)
          (with-t-add-pred-idx before-db flakes opts)
          <?))))


(defn- with-t-updated-settings
  "If settings changed, return new settings map."
  [proposed-db]
  (go-try
    (assoc proposed-db :settings (<? (schema/setting-map proposed-db)))))


(defn with-t
  ([db flakes] (with-t db flakes nil))
  ([{:keys [stats t] :as db} flakes opts]
   (go-try
     (let [new-t           (.-t ^Flake (first flakes))
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
               schema-change? (-> (with-t-updated-schema db flakes bytes opts) <?)
               setting-change? (-> with-t-updated-settings <?))))))


(defn with
  "Returns db 'with' flakes added as a core async promise channel.
  Note this always does a re-sort."
  ([db block flakes] (with db block flakes nil))
  ([db block flakes opts]
   (let [resp-ch (async/promise-chan)]
     (async/go
       (try*
         (when (and (not= block (inc (:block db))))
           (throw (ex-info (str "Invalid 'with' called for db " (:dbid db) " because current db 'block', " (:block db) " must be one less than supplied block " block ".")
                           {:status 500
                            :error  :db/unexpected-error})))
         (if (empty? flakes)
           (async/put! resp-ch (assoc db :block block))
           (let [flakes             (sort flake/cmp-flakes-block flakes)
                 ^Flake first-flake (first flakes)
                 db*                (loop [[^Flake f & r] flakes
                                           t        (.-t first-flake) ;; current 't' value
                                           t-flakes []      ;; all flakes for current 't'
                                           db       db]
                                      (cond (and f (= t (.-t f)))
                                            (recur r t (conj t-flakes f) db)

                                            :else
                                            (let [db' (-> db
                                                          (assoc :t (inc t)) ;; due to permissions, an entire 't' may be filtered out, set to 't' prior to the new flakes
                                                          (with-t t-flakes opts)
                                                          (<?))]
                                              (if (nil? f)
                                                (assoc db' :block block)
                                                (recur r (.-t f) [f] db')))))]

             (async/put! resp-ch db*)))
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
    (let [tt-id'      (if (nil? tt-id) (util/random-uuid) tt-id)
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
                        (let [iri (json-ld/expand ident (get-in db [:schema :prefix]))]
                          (some-> (<? (query-range/index-range db :post = [const/$iri iri]))
                                  ^Flake (first)
                                  (.-s)))

                        ;; TODO - should we validate this is an ident predicate? This will return first result of any indexed value
                        (util/pred-ident? ident)
                        (if-let [pid (dbproto/-p-prop db :id (first ident))]
                          (some-> (<? (query-range/index-range db :post = [pid (second ident)]))
                                  ^Flake (first)
                                  (.-s))
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

;; ================ GraphDB record support fns ================================

(defn- graphdb-latest-db [{:keys [current-db-fn permissions]}]
  (go-try
    (let [current-db (<? (current-db-fn))]
      (assoc current-db :permissions permissions))))

(defn- graphdb-root-db [this]
  (assoc this :permissions {:root?      true
                            :collection {:all? true}
                            :predicate  {:all? true}}))

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
             :txSpecDoc :restrictTag :retractDuplicates :subclass :new?} property)
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
         ^Flake (first)
         (.-o)))))
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
         ^Flake (first)
         (.-s)))))
  ([this tag-name pred]
   (go-try
     (if (str/includes? tag-name "/")
       (<? (dbproto/-tag-id this tag-name))
       (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))]
         (when pred-name
           (<? (dbproto/-tag-id this (str pred-name ":" tag-name)))))))))


;; ================ end GraphDB record support fns ============================

(defrecord GraphDb [conn network dbid block t tt-id stats spot psot post opst
                    schema settings index-configs schema-cache novelty
                    permissions fork fork-block current-db-fn]
  dbproto/IFlureeDb
  (-latest-db [this] (graphdb-latest-db this))
  (-rootdb [this] (graphdb-root-db this))
  (-forward-time-travel [db flakes] (forward-time-travel db nil flakes))
  (-forward-time-travel [db tt-id flakes] (forward-time-travel db tt-id flakes))
  (-c-prop [this property collection] (graphdb-c-prop this property collection))
  (-class-prop [this property class]
    (if (= :subclass property)
      (get @(:subclasses schema) class)
      (get-in schema [:pred class property])))
  (-p-prop [this property predicate] (graphdb-p-prop this property predicate))
  (-tag [this tag-id] (graphdb-tag this tag-id))
  (-tag [this tag-id pred] (graphdb-tag this tag-id pred))
  (-tag-id [this tag-name] (graphdb-tag-id this tag-name))
  (-tag-id [this tag-name pred] (graphdb-tag-id this tag-name pred))
  (-subid [this ident] (subid this ident false))
  (-subid [this ident strict?] (subid this ident strict?))
  (-search [this fparts] (query-range/search this fparts))
  (-query [this query-map] (fql/query this query-map))
  (-with [this block flakes] (with this block flakes nil))
  (-with [this block flakes opts] (with this block flakes opts))
  (-with-t [this flakes] (with-t this flakes nil))
  (-with-t [this flakes opts] (with-t this flakes opts))
  (-add-predicate-to-idx [this pred-id] (add-predicate-to-idx this pred-id nil)))

#?(:cljs
   (extend-type GraphDb
     IPrintWithWriter
     (-pr-writer [db w opts]
       (-write w "#FlureeGraphDB ")
       (-write w (pr {:network     (:network db) :dbid (:dbid db) :block (:block db)
                      :t           (:t db) :stats (:stats db)
                      :permissions (:permissions db)})))))

#?(:clj
   (defmethod print-method GraphDb [^GraphDb db, ^Writer w]
     (.write w (str "#FlureeGraphDB "))
     (binding [*out* w]
       (pr {:network (:network db) :dbid (:dbid db) :block (:block db)
            :t       (:t db) :stats (:stats db) :permissions (:permissions db)}))))

(defn new-novelty-map
  [index-configs]
  (->> [:spot :psot :post :opst]
       (reduce
         (fn [m idx]
           (let [ss (flake/sorted-set-by (get-in index-configs [idx :historyComparator]))]
             (assoc m idx ss)))
         {:size 0})))

(defn new-empty-index
  [conn index-configs network dbid idx]
  (let [index-config (get index-configs idx)
        _            (assert index-config (str "No index config found for index: " idx))
        comparator   (:historyComparator index-config)
        _            (assert comparator (str "No index comparator found for index: " idx))
        first-flake  (flake/->Flake util/max-long -1 util/max-long 0 true nil) ;; left hand side is the largest flake possible
        child-node   (storage/map->UnresolvedNode
                       {:conn  conn :config index-config :network network :dbid dbid :id :empty :leaf true
                        :first first-flake :rhs nil :size 0 :block 0 :t 0 :tt-id nil :leftmost? true})
        children     (flake/sorted-map-by comparator first-flake child-node)
        idx-node     (index/->IndexNode 0 0 nil children index-config true)]
    ;; mark all indexes as dirty to ensure they get written to disk on first indexing process
    idx-node))

(def default-index-configs
  {:spot (index/map->IndexConfig {:index-type        :spot
                                  :comparator        flake/cmp-flakes-spot
                                  :historyComparator flake/cmp-flakes-spot-novelty})
   :psot (index/map->IndexConfig {:index-type        :psot
                                  :comparator        flake/cmp-flakes-psot
                                  :historyComparator flake/cmp-flakes-psot-novelty})
   :post (index/map->IndexConfig {:index-type        :post
                                  :comparator        flake/cmp-flakes-post
                                  :historyComparator flake/cmp-flakes-post-novelty})
   :opst (index/map->IndexConfig {:index-type        :opst
                                  :comparator        flake/cmp-flakes-opst
                                  :historyComparator flake/cmp-flakes-opst-novelty})})

(defn blank-db
  [conn network dbid schema-cache current-db-fn]
  (assert conn "No conn provided when creating new db.")
  (assert network "No network provided when creating new db.")
  (assert dbid "No dbid provided when creating new db.")
  (let [novelty     (new-novelty-map default-index-configs)
        permissions {:collection {:all? false}
                     :predicate  {:all? true}
                     :root?      true}
        spot        (new-empty-index conn default-index-configs network dbid :spot)
        psot        (new-empty-index conn default-index-configs network dbid :psot)
        post        (new-empty-index conn default-index-configs network dbid :post)
        opst        (new-empty-index conn default-index-configs network dbid :opst)
        stats       {:flakes  0
                     :size    0
                     :indexed 0}
        fork        nil
        fork-block  nil
        schema      nil
        settings    nil]
    (->GraphDb conn network dbid 0 -1 nil stats spot psot post opst schema
               settings default-index-configs schema-cache novelty permissions
               fork fork-block current-db-fn)))

(defn graphdb?
  [db]
  (instance? GraphDb db))

