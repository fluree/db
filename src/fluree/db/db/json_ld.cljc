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
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

(defn iri->sid
  "Returns subject id or nil if no match."
  [db iri]
  (go-try
    (some-> (<? (query-range/index-range db :post = [const/$xsd:anyURI iri]))
            first
            flake/s)))

(defn subid
  "Returns subject ID of ident as async promise channel.
  Closes channel (nil) if doesn't exist, or if strict? is true, will return exception."
  [db ident {:keys [strict?]}]
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
                        (throw (ex-info (str "Entid lookup must be a number: " (pr-str ident))
                                        {:status 400
                                         :error  :db/invalid-ident})))]
          (cond
            res
            (async/put! return-chan res)

            (and (nil? res) strict?)
            (async/put! return-chan
                        (ex-info (str "Subject identity does not exist: " (pr-str ident))
                                 {:status 400 :error :db/invalid-subject}))

            :else
            (async/close! return-chan)))

        (catch* e
                (async/put! return-chan
                            (ex-info (str "Error looking up subject id: " (pr-str ident))
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

;; ================ Jsonld record support fns ================================

(defn- jsonld-root-db [this]
  (assoc this :policy root-policy-map))

(defn- jsonld-c-prop [{:keys [schema]} property collection]
  ;; collection properties TODO-deprecate :id property below in favor of :partition
  (assert (#{:name :id :sid :partition :spec :specDoc :base-iri} property)
          (str "Invalid collection property: " (pr-str property)))
  (if (neg-int? collection)
    (get-in schema [:coll "_tx" property])
    (get-in schema [:coll collection property])))

(defn- jsonld-p-prop [{:keys [schema] :as this} property predicate]
  (assert (#{:name :id :iri :type :ref? :idx? :unique :multi :index :upsert :datatype
             :component :noHistory :restrictCollection :spec :specDoc :txSpec
             :txSpecDoc :restrictTag :retractDuplicates :subclassOf :new?} property)
          (str "Invalid predicate property: " (pr-str property)))
  (cond->> (get-in schema [:pred predicate property])
           (= :restrictCollection property) (dbproto/-c-prop this :partition)))

(defn index-update
  "If provided commit-index is newer than db's commit index, updates db by cleaning novelty.
  If it is not newer, returns original db."
  [{:keys [ledger commit] :as db} {data-map :data, :keys [spot post opst tspo] :as commit-index}]
  (let [index-t      (:t data-map)
        newer-index? (and data-map
                          (or (nil? (commit-data/index-t commit))
                              (> index-t (commit-data/index-t commit))))]
    (if newer-index?
      (-> db
          (assoc :commit (assoc commit :index commit-index)
                 :novelty* (idx-proto/-empty-novelty (:indexer ledger) db (- index-t))
                 :spot spot
                 :post post
                 :opst opst
                 :tspo tspo)
          (assoc-in [:stats :indexed] index-t))
      db)))

;; ================ end Jsonld record support fns ============================

(defrecord JsonLdDb [ledger alias branch commit t tt-id stats spot post
                     opst tspo schema comparators novelty policy ecount
                     context-cache]
  dbproto/IFlureeDb
  (-rootdb [this] (jsonld-root-db this))
  (-c-prop [this property collection] (jsonld-c-prop this property collection))
  (-class-prop [this property class]
    (if (= :subclasses property)
      (get @(:subclasses schema) class)
      (get-in schema [:pred class property])))
  (-p-prop [this property predicate] (jsonld-p-prop this property predicate))
  (-subid [this ident] (subid this ident {:strict? false :expand? true}))
  (-subid [this ident opts] (subid this ident opts))
  (-class-ids [this subject] (class-ids this subject))
  (-iri [this subject-id] (iri this subject-id identity))
  (-iri [this subject-id compact-fn] (iri this subject-id compact-fn))
  (-query [this query-map]
    (let [ctx      (ctx-util/extract query-map)]
      (fql/query this ctx query-map)))
  (-stage [db json-ld] (jld-transact/stage db json-ld nil))
  (-stage [db json-ld opts] (jld-transact/stage db json-ld opts))
  (-stage [db fuel-tracker json-ld opts] (jld-transact/stage db fuel-tracker json-ld opts))
  (-index-update [db commit-index] (index-update db commit-index)))

#?(:cljs
   (extend-type JsonLdDb
     IPrintWithWriter
     (-pr-writer [db w opts]
       (-write w "#FlureeJsonLdDb ")
       (-write w (pr {:method      (:method db) :alias (:alias db)
                      :t           (:t db) :stats (:stats db)
                      :policy      (:policy db)})))))

#?(:clj
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

(def identity-flake
  (flake/create const/$xsd:anyURI const/$xsd:anyURI const/iri-id const/$xsd:string 0 true nil))

(def type-flake
  (flake/create const/$rdf:type const/$xsd:anyURI "@type" const/$xsd:string 0 true nil))

(defn create
  [{:keys [method alias conn] :as ledger}]
  (let [novelty       (new-novelty-map index/default-comparators)
        {spot-cmp :spot
         post-cmp :post
         opst-cmp :opst
         tspo-cmp :tspo} index/default-comparators

        spot          (index/empty-branch alias spot-cmp)
        post          (index/empty-branch alias post-cmp)
        opst          (index/empty-branch alias opst-cmp)
        tspo          (index/empty-branch alias tspo-cmp)
        stats         {:flakes 0, :size 0, :indexed 0}
        schema        (vocab/base-schema)
        branch        (branch/branch-meta ledger)]
    (-> (map->JsonLdDb {:ledger          ledger
                        :conn            conn
                        :alias           alias
                        :branch          (:name branch)
                        :commit          (:commit branch)
                        :t               0
                        :tt-id           nil
                        :stats           stats
                        :spot            spot
                        :post            post
                        :opst            opst
                        :tspo            tspo
                        :schema          schema
                        :comparators     index/default-comparators
                        :novelty         novelty
                        :policy          root-policy-map
                        :context-cache   (volatile! nil)
                        :ecount          genesis-ecount})
        (commit-data/update-novelty [identity-flake type-flake]))))
