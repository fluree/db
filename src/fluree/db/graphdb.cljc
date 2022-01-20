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
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [clojure.string :as str]
            [fluree.json-ld :as json-ld])
  #?(:clj (:import (java.io Writer))))

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


(def ^:const exclude-predicates
  "Predicates to exclude from the database"
  #{const/$_tx:tx const/$_tx:sig const/$_tx:tempids})

(defn exclude-flake?
  [f]
  (->> f
       flake/p
       (contains? exclude-predicates)))

(def include-flake?
  (complement exclude-flake?))

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
        {:keys [spot psot post opst tspo size]} novelty]
    (loop [[[p p-flakes] & r] (group-by flake/p flakes)
           spot (transient spot)
           psot (transient psot)
           post (transient post)
           opst (transient opst)
           tspo (transient tspo)]
      (if p-flakes
        (let [exclude? (exclude-predicates p)
              {:keys [idx? ref?]} (get pred-map p)]
          (if exclude?
            (recur r spot psot post opst tspo)
            (recur r
                   (reduce conj! spot p-flakes)
                   (reduce conj! psot p-flakes)
                   (if idx?
                     (reduce conj! post p-flakes)
                     post)
                   (if ref?
                     (reduce conj! opst p-flakes)
                     opst)
                   (reduce conj! tspo p-flakes))))
        {:spot (persistent! spot)
         :psot (persistent! psot)
         :post (persistent! post)
         :opst (persistent! opst)
         :tspo (persistent! tspo)
         :size (+ size #?(:clj @flakes-bytes :cljs flakes-bytes))}))))


(defn- with-t-ecount
  "Calculates updated ecount based on flakes for with-t. Also records if a schema or settings change
  occurred."
  [{:keys [ecount schema] :as db} flakes]
  (loop [[flakes-s & r] (partition-by flake/s flakes)
         schema-change?  (boolean (nil? schema))            ;; if no schema for any reason, make sure one is generated
         setting-change? false
         ecount          ecount]
    (if flakes-s
      (let [sid (flake/s (first flakes-s))
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
     (let [new-t           (flake/t (first flakes))
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
           (throw (ex-info (str "Invalid 'with' called for db " (:dbid db)
                                " because current db 'block', " (:block db)
                                " must be one less than supplied block "
                                block ".")
                           {:status 500
                            :error  :db/unexpected-error})))
         (if (empty? flakes)
           (async/put! resp-ch (assoc db :block block))
           (let [flakes (sort flake/cmp-flakes-block flakes)
                 db*    (loop [[f & r]  flakes
                               t        (->> flakes first flake/t) ;; current 't' value
                               t-flakes []                  ;; all flakes for current 't'
                               db       db]
                          (cond (and f (= t (flake/t f)))
                                (recur r t (conj t-flakes f) db)

                                :else
                                (let [db' (-> db
                                              (assoc :t (inc t)) ;; due to permissions, an entire 't' may be filtered out, set to 't' prior to the new flakes
                                              (with-t t-flakes opts)
                                              (<?))]
                                  (if (nil? f)
                                    (assoc db' :block block)
                                    (recur r (flake/t f) [f] db')))))]

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
   (let [tt-id       (if (nil? tt-id)
                       (util/random-uuid)
                       tt-id)

         ;; update each root index with the provided tt-id
         ;; As the root indexes are resolved, the tt-id will carry through the b-tree and ensure
         ;; query caching is specific to this tt-id
         tt-db       (reduce (fn [db* idx]
                               (update db* idx assoc :tt-id tt-id))
                             (assoc db :tt-id tt-id)
                             index/types)
         flakes-by-t (->> flakes
                          (sort-by :t flake/cmp-tx)
                          (partition-by :t))]
     (loop [db tt-db
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
                        (let [iri (json-ld/expand-iri ident (get-in db [:schema :prefix]))]
                          (some-> (<? (query-range/index-range db :post = [const/$iri iri]))
                                  first
                                  flake/s))

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
               first
               (flake/o)))))
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
               (flake/s)))))
  ([this tag-name pred]
   (go-try
     (if (str/includes? tag-name "/")
       (<? (dbproto/-tag-id this tag-name))
       (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))]
         (when pred-name
           (<? (dbproto/-tag-id this (str pred-name ":" tag-name)))))))))


;; ================ end GraphDB record support fns ============================

(defrecord GraphDb [conn network dbid block t tt-id stats spot psot post opst
                    tspo schema settings comparators schema-cache novelty
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
  [comparators]
  (reduce
   (fn [m idx]
     (assoc m idx (-> comparators
                      (get idx)
                      flake/sorted-set-by)))
   {:size 0} index/types))

(defn blank-db
  [conn network dbid schema-cache current-db-fn]
  (assert conn "No conn provided when creating new db.")
  (assert network "No network provided when creating new db.")
  (assert dbid "No dbid provided when creating new db.")
  (let [novelty     (new-novelty-map index/default-comparators)
        permissions {:collection {:all? false}
                     :predicate  {:all? true}
                     :root?      true}

        {spot-cmp :spot
         psot-cmp :psot
         post-cmp :post
         opst-cmp :opst
         tspo-cmp :tspo} index/default-comparators

        spot (index/empty-branch network dbid spot-cmp)
        psot (index/empty-branch network dbid psot-cmp)
        post (index/empty-branch network dbid post-cmp)
        opst (index/empty-branch network dbid opst-cmp)
        tspo (index/empty-branch network dbid tspo-cmp)

        stats       {:flakes 0, :size 0, :indexed 0}
        fork        nil
        fork-block  nil
        schema      nil
        settings    nil]
    (->GraphDb conn network dbid 0 -1 nil stats spot psot post opst tspo schema
               settings index/default-comparators schema-cache novelty
               permissions fork fork-block current-db-fn)))

(defn graphdb?
  [db]
  (instance? GraphDb db))

(def predefined-properties
  {"http://www.w3.org/2000/01/rdf-schema#Class"          const/$rdfs:Class
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property" const/$rdf:Property
   "http://www.w3.org/2002/07/owl#Class"                 const/$owl:Class
   "http://www.w3.org/2002/07/owl#ObjectProperty"        const/$owl:ObjectProperty
   "http://www.w3.org/2002/07/owl#DatatypeProperty"      const/$owl:DatatypeProperty})

(def class+property-iris (into #{} (keys predefined-properties)))

(defn class-or-property?
  [{:keys [type] :as node}]
  (some class+property-iris type))


(defn json-ld-type-data
  "Returns two-tuple of [class-subject-ids class-flakes]
  where class-flakes will only contain newly generated class
  flakes if they didn't already exist."
  [class-iris t iris next-pid]
  (loop [[class-iri & r] class-iris
         class-sids   []
         class-flakes []]
    (if class-iri
      (if-let [existing (get @iris class-iri)]
        (recur r (conj class-sids existing) class-flakes)
        (let [type-sid (if-let [predefined-pid (get predefined-properties class-iri)]
                         predefined-pid
                         (next-pid))]
          (vswap! iris assoc class-iri type-sid)
          (recur r
                 (conj class-sids type-sid)
                 (conj class-flakes (flake/->Flake type-sid const/$iri class-iri t true nil)))))
      [class-sids class-flakes])))

(defn add-property
  [sid property {:keys [id value] :as v-map} t iris next-sid]
  (let [existing-pid   (get @iris property)
        pid            (or existing-pid
                           (let [new-id (next-sid)]
                             (vswap! iris assoc property new-id)
                             new-id))
        property-flake (when-not existing-pid
                         (flake/->Flake pid const/$iri property t true nil))
        flakes         (if id
                         (let [[id-sid id-flake] (if-let [existing (get @iris id)]
                                                   [existing nil]
                                                   (let [id-sid (next-sid)]
                                                     (vswap! iris assoc id id-sid)
                                                     (if (str/starts-with? id "_:") ;; blank node
                                                       [id-sid nil]
                                                       [id-sid (flake/->Flake id-sid const/$iri id t true nil)])))]
                           (cond-> [(flake/->Flake sid pid id-sid t true nil)]
                                   id-flake (conj id-flake)))
                         [(flake/->Flake sid pid value t true nil)])]
    (cond-> flakes
            property-flake (conj property-flake))))


(defn json-ld-node->flakes
  [node t iris next-pid next-sid]
  (let [id           (:id node)
        existing-sid (when id (get @iris id))
        sid          (or existing-sid
                         (let [new-sid (if (class-or-property? node)
                                         (next-pid)
                                         (next-sid))]
                           (vswap! iris assoc id new-sid)
                           new-sid))
        base-flakes  (if (or (nil? id)
                             existing-sid
                             (str/starts-with? id "_:"))
                       []
                       [(flake/->Flake sid const/$iri id t true nil)])]
    (reduce-kv
      (fn [flakes k v]
        (case k
          (:id :idx) flakes
          :type (let [[type-sids class-flakes] (json-ld-type-data v t iris next-pid)
                      type-flakes (map #(flake/->Flake sid const/$rdf:type % t true nil) type-sids)]
                  (into flakes (concat class-flakes type-flakes)))
          ;;else
          (if (sequential? v)
            (into flakes (mapcat #(add-property sid k % t iris next-sid) v))
            (into flakes (add-property sid k v t iris next-sid)))))
      base-flakes node)))

(defn json-ld-graph->flakes
  "Raw JSON-LD graph to a set of flakes"
  [json-ld opts]
  (let [t        (or (:t opts) -1)
        block    (or (:block opts) 1)
        expanded (fluree.json-ld/expand json-ld)
        iris     (volatile! {})
        last-pid (volatile! 1000)
        last-sid (volatile! (flake/->sid const/$_default 0))
        next-pid (fn [] (vswap! last-pid inc))
        next-sid (fn [] (vswap! last-sid inc))]
    (loop [[node & r] expanded
           flakes (flake/sorted-set-by flake/cmp-flakes-spot)]
      (if node
        (recur r (into flakes (json-ld-node->flakes node t iris next-pid next-sid)))
        {:block  block
         :t      t
         :flakes flakes}))))



(comment

  (def conn (fluree.db.conn.memory/connect))
  (def db (blank-db conn "blah" "hi" (atom {}) (fn [] (throw (Exception. "NO CURRENT DB FN YET")))))

  ;; database identifier
  "fluree:ipfs:cid"
  "fluree:ipns:docs.ipfs.io/introduction/index.html"

  "fluree:hub:namespace/db/named-graph#iri"

  "fluree:s3:us-west2.mybucket/cars"
  ;; a specific URL
  "fluree:http://127.0.0.1:5001/mynamespace/cars"

  "did:fluree:ipfs:cid:iri#keys-1"

  (set! methods {:ipfs {:server-type :ipfs
                        :endpoint    "http://localhost:5001"
                        :access-key  ""
                        :secret      ""}})

  (def myledger (connect "fluree:ipns:docs.ipfs.io/cars"
                         {:server-type :ipfs
                          :endpoint    "http://localhost:5001"
                          :access-key  ""
                          :secret      ""}))

  (def myledger2 (connect "fluree:ipns:docs.ipfs.io/cars"
                          {:server-type :fluree-hub
                           :endpoint    "http://localhost:5001"
                           :access-key  ""
                           :secret      ""}))

  (def vledger (combine myledger myledger2))

  (transact myledger {})

  (new-ledger new-ledger (combine myledger myledger2))



  (def mydb "fluree:ipfs:<cid>")
  (def mydb (db myledger 10))
  (def mydb (db myledger "<hash>"))


  (def mydb "fluree:ipns:docs.ipfs.io/cars/10")



  {:server-type :fluree-peer                                ;; ipfs
   :endpoint    "http://localhost:5001"
   :access-key  ""
   :secret      ""
   }


  (def flakes (json-ld-graph->flakes {"@context" {"owl" "http://www.w3.org/2002/07/owl#",
                                                  "ex"  "http://example.org/ns#"},
                                      "@graph"   [{"@id"   "ex:ontology",
                                                   "@type" "owl:Ontology"}
                                                  {"@id"   "ex:Book",
                                                   "@type" "owl:Class"}
                                                  {"@id"   "ex:Person",
                                                   "@type" "owl:Class"}
                                                  {"@id"   "ex:author",
                                                   "@type" "owl:ObjectProperty"}
                                                  {"@id"   "ex:name",
                                                   "@type" "owl:DatatypeProperty"}
                                                  {"@type"     "ex:Book",
                                                   "ex:author" {"@id" "_:b1"}}
                                                  {"@id"     "_:b1",
                                                   "@type"   "ex:Person",
                                                   "ex:name" {"@value" "Fred"
                                                              "@type"  "xsd:string"}}]}
                                     {}))

  flakes

  (-> db
      :novelty)

  (def db2 (async/<!! (with db 1 (:flakes flakes))))

  (-> db2
      :schema)

  @(fluree.db.api/query (with db 1 (:flakes flakes))
                        {:select ["*"]
                         :from   "http://example.org/ns#ontology"})

  )
