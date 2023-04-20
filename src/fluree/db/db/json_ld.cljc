(ns fluree.db.db.json-ld
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
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
            [fluree.db.util.context :as ctx-util]
            [fluree.db.json-ld.commit-data :as commit-data])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {"f:view"  {:root? true}
   "f:modify" {:root? true}})

(defn expand-iri
  "Expands an IRI from the db's context."
  ([db iri]
   (expand-iri db iri nil))
  ([db iri provided-context]
   (json-ld/expand-iri iri (dbproto/-context db provided-context))))

(defn iri->sid
  "Returns subject id or nil if no match.

  iri can be a compact iri."
  [db iri]
  (go-try
   (log/debug "iri->sid iri:" iri)
   (let [iri* (expand-iri db iri)
         sid (some-> (<? (query-range/index-range db :post = [const/$xsd:anyURI iri*]))
                     first
                     flake/s)]
     (log/debug "iri->sid expanded iri:" iri*)
     (log/debug "iri->sid sid:" sid)
     sid)))


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
   (when-let [flake (-> db
                        (query-range/index-range :spot =
                                                 [subject-id const/$xsd:anyURI])
                        <? first)]
     (-> flake flake/o compact-fn))))

;; ================ GraphDB record support fns ================================

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

(defn- graphdb-tag
  "resolves a tags's value given a tag subject id; optionally shortening the
  return value if it starts with the given predicate name"
  ([this tag-id]
   (go-try
    (let [tag-pred-id const/$_tag:id]
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

(defn retrieve-context
  "Returns the parsed context. Caches."
  [default-context context-cache supplied-context]
  (log/debug "retrieve-context - default: " default-context "supplied:" supplied-context)
  (or (get @context-cache supplied-context)
      (let [context    (if supplied-context
                         (if (sequential? supplied-context)
                           (mapv #(if (= "" %)
                                    ;; we need to substitute in the default context
                                    default-context
                                    %)
                                 supplied-context)
                           supplied-context)
                         default-context)
            parsed-ctx (json-ld/parse-context context)]
        (vswap! context-cache assoc supplied-context parsed-ctx)
        parsed-ctx)))

(defn default-context-update
  "Updates default context, so on next commit it will get written in the commit file."
  [db default-context]
  (let [default-context* (ctx-util/mapify-context default-context
                                                  (dbproto/-default-context db))] ;; allows 'extending' existing default context using empty string ""
    (assoc db :default-context default-context*
              :context-cache (volatile! {})
              :new-context? true)))

;; ================ end GraphDB record support fns ============================

;; TODO - conn is included here because current index-range query looks for conn on the db
;; TODO - this can likely be excluded once index-range is changed to get 'conn' from (:conn ledger) where it also exists
(defrecord JsonLdDb [ledger conn method alias branch commit t tt-id stats spot
                     psot post opst tspo schema comparators novelty policy
                     ecount default-context context-cache new-context?]
  dbproto/IFlureeDb
  (-rootdb [this] (graphdb-root-db this))
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
  (-query [this query-map opts] (fql/query this query-map opts))
  (-stage [db json-ld] (jld-transact/stage db json-ld nil))
  (-stage [db json-ld opts] (jld-transact/stage db json-ld opts))
  (-index-update [db commit-index] (index-update db commit-index))
  (-context [_] (retrieve-context default-context context-cache nil))
  (-context [_ context] (retrieve-context default-context context-cache context))
  (-default-context [_] default-context)
  (-default-context-update [db default-context] (default-context-update db default-context)))

#?(:cljs
   (extend-type JsonLdDb
     IPrintWithWriter
     (-pr-writer [db w opts]
       (-write w "#FlureeJsonLdDb ")
       (-write w (pr {:method (:method db) :alias (:alias db)
                      :t      (:t db) :stats (:stats db)
                      :policy (:policy db)}))))
   :clj
   (defmethod print-method JsonLdDb [^JsonLdDb db, ^Writer w]
     (.write w (str "#FlureeJsonLdDb "))
     (binding [*out* w]
       (pr {:method (:method db) :alias (:alias db)
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
  [{:keys [method alias conn] :as ledger} default-context new-context?]
  (let [novelty (new-novelty-map index/default-comparators)
        {spot-cmp :spot
         psot-cmp :psot
         post-cmp :post
         opst-cmp :opst
         tspo-cmp :tspo} index/default-comparators

        spot    (index/empty-branch method alias spot-cmp)
        psot    (index/empty-branch method alias psot-cmp)
        post    (index/empty-branch method alias post-cmp)
        opst    (index/empty-branch method alias opst-cmp)
        tspo    (index/empty-branch method alias tspo-cmp)
        stats   {:flakes 0, :size 0, :indexed 0}
        schema  (vocab/base-schema)
        branch  (branch/branch-meta ledger)]
    (map->JsonLdDb {:ledger          ledger
                    :conn            conn
                    :method          method
                    :alias           alias
                    :branch          (:name branch)
                    :commit          (:commit branch)
                    :t               0
                    :tt-id           nil
                    :stats           stats
                    :spot            spot
                    :psot            psot
                    :post            post
                    :opst            opst
                    :tspo            tspo
                    :schema          schema
                    :comparators     index/default-comparators
                    :novelty         novelty
                    :policy          root-policy-map
                    :default-context default-context
                    :context-cache   (volatile! nil)
                    :new-context?    new-context?
                    :ecount          genesis-ecount})))
