(ns fluree.db.virtual-graph.index-graph
  (:require #?(:clj [fluree.db.virtual-graph.bm25.index :as bm25])
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph.parse :as vg-parse]
            [fluree.db.virtual-graph.proto :as vgproto]))

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
  [{:keys [vg-name] :as idx-opts} {:keys [alias] :as _db}]
  (let [vg-alias (str "##" vg-name)
        vg-id    (str alias vg-alias)]
    (assoc idx-opts :id vg-id
           :alias vg-alias)))

(defn bm25-idx?
  [idx-rdf-type]
  (some #(= % const/$fluree:index-BM25) idx-rdf-type))

(defn create
  [{:keys [t] :as db} vg-flakes]
  (let [db-vol         (volatile! db) ;; needed to potentially add new namespace codes based on query IRIs
        vg-opts        (-> (idx-flakes->opts vg-flakes)
                           (vg-parse/parse-document-query db-vol)
                           (add-vg-id db)
                           (assoc :genesis-t t))
        {:keys [type alias]} vg-opts
        db*            @db-vol
        vg             (cond
                         ;; add vector index and other types of virtual graphs here
                         (bm25-idx? type) #?(:clj  (bm25/new-bm25-index db vg-flakes vg-opts)
                                             :cljs (throw (ex-info "BM25 index not supported in cljs"
                                                                   {:status 400
                                                                    :error  :db/invalid-index}))))
        _              (when (nil? vg)
                         (throw (ex-info "Unrecognized virtual graph creation attempted."
                                         {:status 400
                                          :error  :db/invalid-index})))
        initialized-vg (vgproto/initialize vg db*)]
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
               (assoc vg* vg-alias (vgproto/upsert vg-impl db add remove)))
             {} vg)]
    (assoc db :vg vg*)))
