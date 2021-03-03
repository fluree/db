(ns fluree.db.graphdb
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.schema :as schema-util]
            [clojure.data.avl :as avl]
            [fluree.db.query.fql :as fql]
            [fluree.db.index :as index]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.log :as log]
            [clojure.string :as str])
  #?(:clj
     (:import (fluree.db.flake Flake))))

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

(defn add-predicate-to-idx
  [db pred-id]
  (go-try
    (let [flakes-to-idx (<? (query-range/index-range db :psot = [pred-id]))
          {:keys [post size]} (:novelty db)
          with-size     (flake/size-bytes flakes-to-idx)
          total-size    (+ size with-size)
          {:keys [novelty-min novelty-max]} (get-in db [:conn :meta])
          _             (if (> total-size novelty-max)
                          (throw (ex-info (str "You cannot add " pred-id " to the index at this point. There are too many affected flakes.")
                                          {:error  :db/max-novelty-size
                                           :status 400})))
          post          (into post flakes-to-idx)]
      (swap! (:schema-cache db) empty)
      (-> db
          (assoc-in [:novelty :post] post)
          (assoc-in [:novelty :size] total-size)))))

(defn with-t
  "Processes a single transaction, adding it to the DB.
  Assumes flakes are already properly sorted."
  [db flakes]
  (go-try
    (let [t                    (.-t ^Flake (first flakes))
          _                    (when (not= t (dec (:t db)))
                                 (throw (ex-info (str "Invalid with called for db " (:dbid db) " because current 't', " (:t db) " is not beyond supplied transaction t: " t ".")
                                                 {:status 500
                                                  :error  :db/unexpected-error})))
          add-flakes           (filter #(not (exclude-predicates (.-p ^Flake %))) flakes)
          add-preds            (into #{} (map #(.-p ^Flake %) add-flakes))
          idx?-map             (into {} (map (fn [p] [p (dbproto/-p-prop db :idx? p)]) add-preds))
          ref?-map             (into {} (map (fn [p] [p (dbproto/-p-prop db :ref? p)]) add-preds))
          flakes-bytes         (flake/size-bytes add-flakes)
          schema-change?       (schema-util/schema-change? add-flakes)
          root-setting-change? (schema-util/setting-change? add-flakes)
          pred-ecount          (-> db :ecount (get const/$_predicate))
          add-pred-to-idx?     (if schema-change? (schema-util/add-to-post-preds? add-flakes pred-ecount) [])
          db*                  (loop [[add-pred & r] add-pred-to-idx?
                                      db db]
                                 (if add-pred
                                   (recur r (<? (add-predicate-to-idx db add-pred)))
                                   db))
          ;; this could require reindexing, so we handle remove predicates later
          db*                  (-> db*
                                   (assoc :t t)
                                   (update-in [:stats :size] + flakes-bytes) ;; total db ~size
                                   (update-in [:stats :flakes] + (count add-flakes)))]
      (loop [[^Flake f & r] add-flakes
             spot   (get-in db* [:novelty :spot])
             psot   (get-in db* [:novelty :psot])
             post   (get-in db* [:novelty :post])
             opst   (get-in db* [:novelty :opst])
             tspo   (get-in db* [:novelty :tspo])
             ecount (:ecount db)]
        (if-not f
          (let [flake-size (-> db*
                               (get-in [:novelty :size])
                               (+ flakes-bytes))
                db*  (assoc db*
                            :ecount ecount
                            :novelty {:spot spot, :psot psot, :post post,
                                      :opst opst, :tspo tspo, :size flake-size})]
            (cond-> db*
              (or schema-change?
                  (nil? (:schema db*))) (assoc :schema (<? (schema/schema-map db*)))
              root-setting-change?      (assoc :settings (<? (schema/setting-map db*)))))
          (let [cid     (flake/sid->cid (.-s f))
                ecount* (update ecount cid #(if % (max % (.-s f)) (.-s f)))]
            (recur r
                   (conj spot f)
                   (conj psot f)
                   (if (get idx?-map (.-p f))
                     (conj post f)
                     post)
                   (if (get ref?-map (.-p f))
                     (conj opst f)
                     opst)
                   (conj tspo f)
                   ecount*)))))))

(defn with
  "Returns db 'with' flakes added as a core async promise channel.
  Note this always does a re-sort."
  [db block flakes]
  (let [resp-ch (async/promise-chan)]
    (async/go
      (try*
        (when (and (not= block (inc (:block db))))
          (throw (ex-info (str "Invalid 'with' called for db " (:dbid db) " because current db 'block', " (:block db) " must be one less than supplied block " block ".")
                          {:status 500
                           :error  :db/unexpected-error})))
        (if (empty? flakes)
          (async/put! resp-ch (assoc db :block block))
          (let [flakes (sort flake/cmp-flakes-block flakes)
                db*    (loop [[^Flake f & r] flakes
                              t        (.-t (first flakes)) ;; current 't' value
                              t-flakes []                   ;; all flakes for current 't'
                              db       db]
                         (cond (and f (= t (.-t f)))
                               (recur r t (conj t-flakes f) db)

                               :else
                               (let [db' (-> db
                                             (assoc :t (inc t)) ;; due to permissions, an entire 't' may be filtered out, set to 't' prior to the new flakes
                                             (with-t t-flakes)
                                             (<?))]
                                 (if (nil? f)
                                   (assoc db' :block block)
                                   (recur r (.-t f) [f] db')))))]

            (async/put! resp-ch db*)))
        (catch* e
                (async/put! resp-ch e))))
    resp-ch))

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
                              index/types)
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

                        ; If it's an pred-ident, but the first part of the identity doesn't resolve to an existing predicate, throws an error
                        (and (util/pred-ident? ident) (nil? (dbproto/-p-prop db :id (first ident))))
                        (throw (ex-info (str "Subject ID lookup failed. The predicate " (pr-str (first ident)) " does not exist.")
                                        {:status 400
                                         :error  :db/invalid-ident}))

                        ;; TODO - should we validate this is an ident predicate? This will return first result of any indexed value
                        (util/pred-ident? ident)
                        (some-> (<? (query-range/index-range db :post = [(dbproto/-p-prop db :id (first ident)) (second ident)]))
                                ^Flake (first)
                                (.-s))

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

(defrecord GraphDb [conn network dbid block t tt-id stats spot psot post opst tspo schema settings index-configs schema-cache novelty permissions fork fork-block current-db-fn]
  dbproto/IFlureeDb
  (-latest-db [this]
    (go-try
      (let [current-db (<? (current-db-fn))]
        (assoc current-db :permissions permissions))))
  (-rootdb [this] (assoc this :permissions {:root? true :collection {:all? true} :predicate {:all? true}}))
  (-forward-time-travel [db flakes] (forward-time-travel db nil flakes))
  (-forward-time-travel [db tt-id flakes] (forward-time-travel db tt-id flakes))
  (-c-prop [this property collection]
    ;; collection properties TODO-deprecate :id property below in favor of :partition
    (assert (#{:name :id :sid :partition :spec :specDoc} property) (str "Invalid collection property: " (pr-str property)))
    (if (neg-int? collection)
      (get-in schema [:coll "_tx" property])
      (get-in schema [:coll collection property])))
  (-p-prop [this property predicate]
    ;; predicate properties
    (assert (#{:name :id :type :ref? :idx? :unique :multi :index :upsert :component :noHistory :restrictCollection
               :spec :specDoc :txSpec :txSpecDoc :restrictTag} property) (str "Invalid predicate property: " (pr-str property)))
    (cond->> (get-in schema [:pred predicate property])
             (= :restrictCollection property) (dbproto/-c-prop this :partition)))
  (-tag [this tag-id]
    ;; resolves a tags's value given a tag subject id
    (go-try
      (let [tag-pred-id 30]
        (some-> (<? (query-range/index-range (dbproto/-rootdb this) :spot = [tag-id tag-pred-id]))
                ^Flake (first)
                (.-o)))))
  (-tag [this tag-id pred]
    ;; resolves a tag's value given a tag subject id, and shortens the return value if it
    ;; starts with the predicate name.
    (go-try
      (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))
            tag       (<? (dbproto/-tag this tag-id))]
        (when (and pred-name tag)
          (if (str/includes? tag ":")
            (-> (str/split tag #":") second)
            tag)))))
  (-tag-id [this tag-name]
    (go-try
      (let [tag-pred-id const/$_tag:id]
        (some-> (<? (query-range/index-range (dbproto/-rootdb this) :post = [tag-pred-id tag-name]))
                ^Flake (first)
                (.-s)))))
  (-tag-id [this tag-name pred]
    (go-try
      (if (str/includes? tag-name "/")
        (<? (dbproto/-tag-id this tag-name))
        (let [pred-name (if (string? pred) pred (dbproto/-p-prop this :name pred))]
          (when pred-name
            (<? (dbproto/-tag-id this (str pred-name ":" tag-name))))))))
  (-subid [this ident] (subid this ident false))
  (-subid [this ident strict?] (subid this ident strict?))
  (-search [this fparts] (query-range/search this fparts))
  (-query [this query-map] (fql/query this query-map))
  (-with [this block flakes] (with this block flakes))
  (-with-t [this flakes] (with-t this flakes))
  (-add-predicate-to-idx [this pred-id] (add-predicate-to-idx this pred-id)))
;#?@(:cljs
;    [IPrintWithWriter
;     (-pr-writer [db w opts]
;                 (-write w "#FlureeGraphDB ")
;                 (-write w (pr {:network (:network db) :dbid (:dbid db) :block (:block db) :t (:t db) :stats (:stats db) :permissions (:permissions db)})))])

#?(:cljs
   (extend-type GraphDb
     IPrintWithWriter
     (-pr-writer [db w opts]
       (-write w "#FlureeGraphDB ")
       (-write w (pr {:network (:network db) :dbid (:dbid db) :block (:block db) :t (:t db) :stats (:stats db) :permissions (:permissions db)})))))

#?(:clj
   (defmethod print-method GraphDb [^GraphDb db, ^java.io.Writer w]
     (.write w (str "#FlureeGraphDB "))
     (binding [*out* w]
       (pr {:network (:network db) :dbid (:dbid db) :block (:block db) :t (:t db) :stats (:stats db) :permissions (:permissions db)}))))

(defn new-novelty-map
  [index-configs]
  (reduce
   (fn [m idx]
     (assoc m idx (-> index-configs
                      (get-in [idx :historyComparator])
                      avl/sorted-set-by)))
   {:size 0} index/types))

(defn new-empty-index
  ([conn network dbid idx-type]
   (new-empty-index conn index/default-configs network dbid idx-type))
  ([conn index-configs network dbid idx-type]
   (let [index-config (get index-configs idx-type)
         _            (assert index-config (str "No index config found for index: " idx-type))
         comparator   (:historyComparator index-config)
         _            (assert comparator (str "No index comparator found for index: " idx-type))
         first-flake  (flake/->Flake util/max-long 0 util/max-long 0 true nil) ;; left hand side is the largest flake possible
         child-node   (storage/map->UnresolvedNode
                       {:conn  conn :config index-config :network network :dbid dbid :id :empty :leaf true
                        :first first-flake :rhs nil :size 0 :block 0 :t 0 :tt-id nil :leftmost? true})
         children     (avl/sorted-map-by comparator first-flake child-node)
         idx-node     (index/->IndexNode 0 0 nil children index-config true)]
     ;; mark all indexes as dirty to ensure they get written to disk on first indexing process
     idx-node)))

(defn blank-db
  [conn network dbid schema-cache current-db-fn]
  (assert conn "No conn provided when creating new db.")
  (assert network "No network provided when creating new db.")
  (assert dbid "No dbid provided when creating new db.")
  (let [novelty     (new-novelty-map index/default-configs)
        permissions {:collection {:all? false}
                     :predicate  {:all? true}
                     :root?      true}
        spot        (new-empty-index conn network dbid :spot)
        psot        (new-empty-index conn network dbid :psot)
        post        (new-empty-index conn network dbid :post)
        opst        (new-empty-index conn network dbid :opst)
        tspo        (new-empty-index conn network dbid :tspo)
        stats       {:flakes 0, :size 0, :indexed 0}
        fork        nil
        fork-block  nil
        schema      nil
        settings    nil]
    (->GraphDb conn network dbid 0 -1 nil stats spot psot post opst tspo schema settings index/default-configs schema-cache novelty permissions fork fork-block current-db-fn)))

(defn graphdb?
  [db]
  (instance? GraphDb db))
