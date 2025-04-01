(ns fluree.db.virtual-graph.index-graph
  (:require #?@(:clj [[fluree.db.virtual-graph.bm25.storage]
                      [fluree.db.virtual-graph.bm25.index :as bm25]
                      [fluree.db.virtual-graph.parse :as vg-parse]])
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
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
       (false? (flake/op idx-flake))
       (assoc acc :retraction? true)

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
   {:id          (flake/s (first index-flakes))
    :type        []
    :vg-name     nil
    :retraction? false ;; if we find a 'retract' op flake, flag so we know it is an attempt to update/delete 
    :query       nil}
   index-flakes))

(defn throw-if-defined
  "Ensures a new virtual graph does not reuse an existing virtual
   graph alias, or create a new virtual graph using an existing vg IRI"
  [{:keys [vg] :as db} named-graph vg-sid]
  (when (contains? vg named-graph)
    (throw (ex-info (str "Virtual graph alias: " named-graph " already exists in db.")
                    {:status 400
                     :error  :db/invalid-index})))

  (when (some #(= vg-sid (:id %)) (vals vg))
    (throw (ex-info (str "Virtual graph IRI already exists in db: " (iri/decode-sid db vg-sid))
                    {:status 400
                     :error  :db/invalid-index}))))

#?(:clj
   (defn create
     [{:keys [t] :as db} vg-opts vg-flakes]
     (let [db-vol (volatile! db) ;; needed to potentially add new namespace codes based on query IRIs

           {:keys [type vg-name] :as vg-opts*}
           (-> (vg-parse/parse-document-query vg-opts db-vol)
               (assoc :genesis-t t))

           named-graph (vg/named-graph-str vg-name)

           _ (throw-if-defined db named-graph (:id vg-opts*))

           db* @db-vol
           vg  (cond
                 ;; add vector index and other types of virtual graphs here
                 (bm25/bm25-iri? type)
                 (bm25/new-bm25-index db vg-flakes vg-opts*)

                 :else (throw (ex-info "Unrecognized virtual graph creation attempted."
                                       {:status 400
                                        :error  :db/invalid-index})))]
       (assoc-in db* [:vg named-graph] (vg/initialize vg db*))))

   :cljs
   (defn create
     [_ _ _]
     (throw (ex-info "Creating BM25 indexes not supported in cljs"
                     {:status 400, :error :db/invalid-index}))))

(defn remove-vg?
  [vg-flakes]
  (some #(and (false? (flake/op %))
              (= const/$rdf:type (flake/p %))
              (= const/$fluree:VirtualGraph (flake/o %)))
        vg-flakes))

(defn modify
  [db {:keys [vg-name] :as _vg-opts} vg-flakes]
  (let [named-graph (vg/named-graph-str vg-name)]
    (if (remove-vg? vg-flakes)
      (update db :vg dissoc named-graph)
      (throw (ex-info "Virtual graph update not supported."
                      {:status 400
                       :error  :db/invalid-index})))))

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

(defn update-virtual-graphs
  "Creates a new virtual graph. If the virtual graph is invalid, an
  exception will be thrown and the transaction will not complete."
  [db add vg-sids]
  (loop [[vg-sid & r] vg-sids
         db db]
    (if vg-sid
      (let [vg-flakes   (filter #(= (flake/s %) vg-sid) add)
            {:keys [retraction?] :as vg-opts} (idx-flakes->opts vg-flakes)
            db* (if retraction?
                  (modify db vg-opts vg-flakes)
                  (create db vg-opts vg-flakes))]
        (recur r db*))
      db)))

(defn has-vgs?
  [db]
  (not-empty (:vg db)))

(defn virtual-graph?
  [f]
  (-> f flake/o (= const/$fluree:VirtualGraph)))

(defn extract-vgs
  "Extracts and returns unique SIDs for virtual graphs defined in a stage operation."
  [fs]
  (reduce
   (fn [acc f]
     (if (virtual-graph? f)
       (conj acc (flake/s f))
       acc))
   #{} fs))

(defn check-virtual-graph
  [db add rem]
  (let [vg-sids (extract-vgs add)]
    (cond-> db
      (seq vg-sids) (update-virtual-graphs add vg-sids)
      (has-vgs? db) (update-vgs add rem))))
