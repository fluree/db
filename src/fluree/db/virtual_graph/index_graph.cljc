(ns fluree.db.virtual-graph.index-graph
  (:require #?@(:clj [[fluree.db.virtual-graph.bm25.storage]
                      [fluree.db.virtual-graph.bm25.index :as bm25]
                      [fluree.db.virtual-graph.parse :as vg-parse]])
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

#?(:clj (set! *warn-on-reflection* true))

(defn idx-flakes->opts
  [index-flakes]
  (reduce
   (fn [acc idx-flake]
     (cond
       (= (flake/p idx-flake) const/$fluree:virtualGraph-name)
       (assoc acc :vg-name (flake/o idx-flake))

       (and (= (flake/p idx-flake) const/$rdf:type)
            (not= (flake/o idx-flake) const/$fluree:VirtualGraph))
       (update acc :type conj (flake/o idx-flake))

       (= (flake/p idx-flake) const/$fluree:query)
       (try*
         (assoc acc :query (json/parse (flake/o idx-flake) false))
         (catch* e
           (throw (ex-info (str "Invalid query json provided for Bm25 index, unable to parse: " (flake/o idx-flake))
                           {:status 400
                            :error  :db/invalid-index}))))

       :else acc))
   {:id      (flake/s (first index-flakes))
    :type    []
    :vg-name nil
    :query   nil}
   index-flakes))

#?(:clj
   (defn create
     [{:keys [t] :as db} vg-flakes]
     (let [db-vol (volatile! db) ;; needed to potentially add new namespace codes based on query IRIs

           {:keys [type] :as vg-opts}
           (-> (idx-flakes->opts vg-flakes)
               (vg-parse/parse-document-query db-vol)
               (assoc :genesis-t t))

           db* @db-vol
           vg  (cond
                 ;; add vector index and other types of virtual graphs here
                 (bm25/bm25-iri? type)
                 (bm25/new-bm25-index db vg-flakes vg-opts)

                 :else (throw (ex-info "Unrecognized virtual graph creation attempted."
                                       {:status 400
                                        :error  :db/invalid-index})))
           vg* (vg/initialize vg db*)]
       [db* vg*]))

   :cljs
   (defn create
     [_ _]
     (throw (ex-info "Creating BM25 indexes not supported in cljs"
                     {:status 400, :error :db/invalid-index}))))

(defn load-virtual-graph
  [db alias]
  (or (get-in db [:vg alias])
      (throw (ex-info (str "Virtual graph requested: " alias " does not exist for the db.")
                      {:status 400
                       :error  :db/invalid-query}))))

(defn update-vgs
  "Accepts a db that contains virtual graphs, and
  kicks a potential update to each of them with the
  current db, new flakes and, in the case of a stage
  that removed flakes not yet committed, removed flakes.

  Virtual graphs should update asynchronously, but return
  immediately with a new VG record that represents the
  updated state."
  [{:keys [vg] :as db} add remove]
  ;; at least currently, updates to vg are async
  ;; and happen in background.
  (let [vg* (reduce-kv
             (fn [vg* named-graph vg-impl]
               (log/debug "Virtual Graph update started for: " named-graph)
               (assoc vg* named-graph (vg/upsert vg-impl db add remove)))
             {} vg)]
    (assoc db :vg vg*)))

(defn create-virtual-graphs
  "Creates a new virtual graph. If the virtual graph is invalid, an
  exception will be thrown and the transaction will not complete."
  [db add new-vgs]
  (loop [[new-vg & r] new-vgs
         db db]
    (if new-vg
      (let [vg-flakes   (filter #(= (flake/s %) new-vg) add)
            [db* vg]    (create db vg-flakes)
            named-graph (vg/named-graph-alias vg)]
        ;; TODO - VG - ensure alias is not being used, throw if so
        (recur r (assoc-in db* [:vg named-graph] vg)))
      db)))

(defn has-vgs?
  [db]
  (not-empty (:vg db)))

(defn virtual-graph?
  [f]
  (-> f flake/o (= const/$fluree:VirtualGraph)))

(defn extract-vgs
  [fs]
  (->> fs
       (keep (fn [f]
               (when (virtual-graph? f)
                 (flake/s f))))
       set))

(defn check-virtual-graph
  [db add rem]
  ;; TODO - VG - should also check for retractions to "delete" virtual graph
  ;; TODO - VG - check flakes if user updated existing virtual graph
  (let [new-vgs (extract-vgs add)]
    (cond-> db
      (seq new-vgs) (create-virtual-graphs add new-vgs)
      (has-vgs? db) (update-vgs add rem))))
