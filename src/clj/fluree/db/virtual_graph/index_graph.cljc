(ns fluree.db.virtual-graph.index-graph
  (:require [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj [fluree.db.virtual-graph.bm25.index :as bm25])
            #?(:clj [fluree.db.virtual-graph.bm25.storage])
            [fluree.db.virtual-graph.parse :as vg-parse]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

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
   {:type    []
    :vg-name nil
    :query   nil}
   index-flakes))

(defn add-vg-id
  "Adds the full virtual graph IRI to the index options map"
  [{:keys [vg-name] :as idx-opts} db-alias]
  (let [vg-alias (str "##" vg-name)
        vg-id    (str db-alias vg-alias)]
    (assoc idx-opts
           :id vg-id
           :alias vg-alias)))

(defn create
  [{:keys [alias t] :as db} vg-flakes]
  (let [db-vol         (volatile! db) ;; needed to potentially add new namespace codes based on query IRIs
        vg-opts        (-> (idx-flakes->opts vg-flakes)
                           (update :query vg-parse/select-one->select)
                           (vg-parse/parse-document-query db-vol)
                           (add-vg-id alias)
                           (assoc :genesis-t t))
        {:keys [type alias]} vg-opts
        db*            @db-vol
        vg             (cond
                         ;; add vector index and other types of virtual graphs here
                         (bm25/bm25-iri? type) #?(:clj  (bm25/new-bm25-index db vg-flakes vg-opts)
                                                  :cljs (throw (ex-info "BM25 index not supported in cljs"
                                                                        {:status 400
                                                                         :error  :db/invalid-index})))
                         :else (throw (ex-info "Unrecognized virtual graph creation attempted."
                                               {:status 400
                                                :error  :db/invalid-index})))
        initialized-vg (vg/initialize vg db*)]
    [db* alias initialized-vg]))

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
             (fn [vg* vg-alias vg-impl]
               (log/debug "Virtual Graph update started for: " vg-alias)
               (assoc vg* vg-alias (vg/upsert vg-impl db add remove)))
             {} vg)]
    (assoc db :vg vg*)))

(defn create-virtual-graphs
  "Creates a new virtual graph. If the virtual graph is invalid, an
  exception will be thrown and the transaction will not complete."
  [db add new-vgs]
  (loop [[new-vg & r] new-vgs
         db db]
    (if new-vg
      (let [vg-flakes (filter #(= (flake/s %) new-vg) add)
            [db* alias vg-record] (create db vg-flakes)]
        ;; TODO - VG - ensure alias is not being used, throw if so
        (recur r (assoc-in db* [:vg alias] vg-record)))
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
