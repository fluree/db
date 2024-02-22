(ns fluree.db.db.json-ld
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.fql :as fql]
            [fluree.db.index :as index]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try]]
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
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db subject-id]
  (go-try
    (map flake/o
         (-> (dbproto/-rootdb db)
             (query-range/index-range :spot = [subject-id const/$rdf:type])
             <?))))

;; ================ Jsonld record support fns ================================

(defn- jsonld-root-db [this]
  (assoc this :policy root-policy-map))

(defn- jsonld-p-prop [{:keys [schema]} property predicate]
  (assert (#{:name :id :iri :type :ref? :unique :multi :index :upsert :datatype
             :component :noHistory :spec :specDoc :txSpec :txSpecDoc :restrictTag
             :retractDuplicates :subclassOf :new?}
            property)
          (str "Invalid predicate property: " (pr-str property)))
  (get-in schema [:pred predicate property]))

(defn force-index-update
  [{:keys [ledger commit] :as db} {data-map :data, :keys [spot post opst tspo] :as commit-index}]
  (let [index-t (:t data-map)
        commit* (assoc commit :index commit-index)]
    (-> db
        (assoc :commit commit*
               :novelty* (idx-proto/-empty-novelty (:indexer ledger) db index-t)
               :spot spot
               :post post
               :opst opst
               :tspo tspo)
        (assoc-in [:stats :indexed] index-t))))

(defn index-update
  "If provided commit-index is newer than db's commit index, updates db by cleaning novelty.
  If it is not newer, returns original db."
  [{:keys [commit] :as db} {data-map :data, :as commit-index}]
  (let [index-t        (:t data-map)
        commit-index-t (commit-data/index-t commit)
        newer-index?   (and data-map
                            (or (nil? commit-index-t)
                                (> index-t commit-index-t)))]
    (if newer-index?
      (force-index-update db commit-index)
      db)))

;; ================ end Jsonld record support fns ============================

(defrecord JsonLdDb [ledger alias branch commit t tt-id stats spot post opst
                     tspo schema comparators novelty policy namespaces
                     namespace-codes]
  dbproto/IFlureeDb
  (-rootdb [this] (jsonld-root-db this))
  (-class-prop [_this property class]
    (if (= :subclasses property)
      (get @(:subclasses schema) class)
      (get-in schema [:pred class property])))
  (-p-prop [this property predicate] (jsonld-p-prop this property predicate))
  (-class-ids [this subject] (class-ids this subject))
  (-query [this query-map]
    (fql/query this query-map))
  (-stage [db json-ld] (jld-transact/stage db json-ld nil))
  (-stage [db json-ld opts] (jld-transact/stage db json-ld opts))
  (-stage [db fuel-tracker json-ld opts] (jld-transact/stage db fuel-tracker json-ld opts))
  (-index-update [db commit-index] (index-update db commit-index))

  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes)))

#?(:cljs
   (extend-type JsonLdDb
     IPrintWithWriter
     (-pr-writer [db w _opts]
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

(defn create
  [{:keys [alias conn] :as ledger}]
  (let [novelty (new-novelty-map index/default-comparators)
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
    (map->JsonLdDb {:ledger          ledger
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
                    :namespaces      iri/default-namespaces
                    :namespace-codes iri/default-namespace-codes})))
