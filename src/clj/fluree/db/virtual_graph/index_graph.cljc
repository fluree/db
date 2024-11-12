(ns fluree.db.virtual-graph.index-graph
  (:require [clojure.core.async :as async :refer [go alts! put! promise-chan <! >!]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.virtual-graph.bm25.index :as bm25]
            [fluree.db.virtual-graph.bm25.update :as bm25.update]
            [fluree.db.virtual-graph.bm25.search :as bm25.search]
            [fluree.db.virtual-graph.parse :as vg-parse]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(defprotocol UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes] "Updates the virtual graph with the provided flakes. Returns async chan with new updated VirtualGraph or exception.")
  (initialize [this source-db] "Initialize a new virtual graph based on the provided db - returns promise chan of eventual result")
  (serialize [this] "Returns a JSON serializable representation of the virtual graph (does not serialize to JSON)")
  (deserialize [this source-db data] "Reifies the virtual graph from the provided data structure"))

(defn property-dependencies
  [vg]
  (:property-deps vg))

(defn parsed-query
  [vg]
  (:query-parsed vg))

(defn affected-subjs
  [prop-deps add removes]
  (let [adds (reduce (fn [acc f]
                       (if (prop-deps (flake/p f))
                         (conj acc (flake/s f))
                         acc))
                     #{} add)]
    (if (empty? removes)
      adds
      (reduce (fn [acc f]
                (if (prop-deps (flake/p f))
                  (conj acc (flake/s f))
                  acc))
              adds
              removes))))

(defn bm25-upsert*
  [{:keys [index-state] :as bm25} {:keys [t alias namespaces namespace-codes] :as _db} items-ch]
  (let [{:keys [pending-ch index] :as prior-idx-state} @index-state
        new-pending-ch  (promise-chan)
        new-index-state (atom (assoc prior-idx-state :pending-ch new-pending-ch))]

    ;; following go-block happens asynchronously in the background
    ;; TODO - VG - capture error conditions in async/<! or other opts below and resolve the response with an error.
    (go
      (let [items         (<! items-ch)
            latest-index  (if pending-ch
                            (<! pending-ch)
                            index)
            status-update (fn [status]
                            (swap! new-index-state assoc :pending-status status))
            new-index     (bm25.update/assert-items bm25 latest-index items status-update)]
        ;; reset index state atom once index is complete, remove pending-ch
        (swap! new-index-state (fn [idx-state]
                                 (assoc idx-state :index new-index
                                                  :pending-ch nil)))
        (>! new-pending-ch new-index)))

    ;; new bm25 record returned to get attached to db
    (assoc bm25 :t t
                :namespaces namespaces
                :namespace-codes namespace-codes
                ;; unlikely, but in case db's alias has been changed keep in sync
                :db-alias alias
                :index-state new-index-state)))

(defn bm25-upsert
  [bm25 db add removes]
  (let [prop-deps      (property-dependencies bm25)
        affected-sids  (affected-subjs prop-deps add removes)
        affected-iris  (map #(iri/decode-sid db %) affected-sids)
        pq             (parsed-query bm25)
        iri-var        (-> pq :select :subj)
        iri-values     (map #(hash-map iri-var (where/match-iri %)) affected-iris)
        pq*            (assoc pq :values iri-values)
        upsert-docs-ch (exec/query db nil pq*)]

    (bm25-upsert* bm25 db upsert-docs-ch)))

(defn bm25-initialize
  [{:keys [query-parsed] :as bm25} db]
  (let [index-items-ch (exec/query db nil query-parsed)]
    (bm25-upsert* bm25 db index-items-ch)))

(defn percent-complete
  [index-state]
  (let [{:keys [pending-status]} @index-state]
    (if pending-status
      (int (* 100 (/ (first pending-status) (second pending-status))))
      0)))

(defn score-candidates
  [query-terms vectors avg-length k1 b candidates]
  (reduce
   (fn [acc candidate]
     (let [doc-vec (get vectors candidate)
           score   (bm25.search/calc-doc-score k1 b avg-length query-terms doc-vec)]
       (conj acc {:id    candidate
                  :score score
                  :vec   doc-vec})))
   [] candidates))

(defn search
  [{:keys [stemmer stopwords k1 b index-state] :as bm25} solution error-ch out-ch]
  (go
    (try*
      (let [{::vg-parse/keys [target limit timeout] :as search-params} (vg-parse/get-search-params solution)
            {:keys [pending-ch index]} @index-state

            ;; TODO - check for "sync" options and don't wait for pending-ch if sync is false

            index*      (if pending-ch
                          (let [timeout*   (or timeout 10000)
                                timeout-ch (async/timeout timeout*)
                                [idx ch] (alts! [timeout-ch pending-ch])]
                            (if (= timeout-ch ch)
                              (put! error-ch (ex-info (str "Timeout waiting for BM25 index to sync after "
                                                           timeout* "ms. Index is " (percent-complete index-state)
                                                           "% complete. Please try again later. To configure a "
                                                           "different timeout, set " const/iri-index-timeout " in the virtual "
                                                           "graph query to an integer of milliseconds.")
                                                      {:error  :db/timeout
                                                       :status 408}))
                              idx))
                          index)
            {:keys [vectors item-count avg-length terms]} index*
            query-terms (bm25.search/parse-query target terms item-count stemmer stopwords) ;; parsed terms from query with idf calculated
            candidates  (reduce #(into %1 (:items %2)) #{} query-terms)] ;; reverse index allows us to know which docs contain each query term, put into single set
        (->> candidates
             (score-candidates query-terms vectors avg-length k1 b)
             (sort-by :score #(compare %2 %1))
             (vg-parse/limit-results limit)
             (vg-parse/process-results bm25 solution search-params true)
             (async/onto-chan! out-ch)))
      (catch* e
              (log/error e "Error ranking vectors")
        (>! error-ch e)))))

(defrecord BM25-VirtualGraph
  [stemmer stopwords k1 b index-state initialized genesis-t t
   alias query query-parsed property-deps
   ;; following taken from db - needs to be kept up to date with new db updates
   db-alias namespaces namespace-codes]

  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes))

  UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes]
    (bm25-upsert this source-db new-flakes remove-flakes))
  (initialize [this source-db]
    (bm25-initialize this source-db))
  (serialize [_] {}) ;; TODO - VG - serialize to JSON (plus, call when writing index to store)
  (deserialize [_ source-db data] {}) ;; TODO - VG - deserialize to JSON (plus, reify when reading index from store)

  where/Matcher
  (-match-triple [_ _fuel-tracker solution triple _error-ch]
    (vg-parse/match-search-triple solution triple))

  (-finalize [this _fuel-tracker error-ch solution-ch]
    (vg-parse/finalize (partial search this) error-ch solution-ch))

  (-match-id [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-match-class [_ _fuel-tracker _solution _s-mch _error-ch]
    where/nil-channel)

  (-activate-alias [this _]
    this)

  ;; return db-alias here, as it is used when encoding/decoding IRIs in the search function which is original db-dependent
  (-aliases [_] [db-alias]))


;; TODO - VG - triggering updates only works for queries for single subject, no nested nodes
;; TODO - VG - prevent :select ["*"] syntax from being allowed, need to list properties explicitly
;; TODO - VG - prevent :selectOne from being used, or maybe just util/seq all results
;; TODO - VG - ensure "@id" is one of the selected properties
;; TODO - VG - future feature - weighted properties
(defn new-bm25-index
  [{:keys [namespaces namespace-codes alias] :as _db} index-flakes vg-opts]
  (let [opts (-> (bm25/idx-flakes->opts index-flakes)
                 (merge vg-opts)
                 ;; index-state held as atom, as we need -match-triple, etc. to hold both
                 ;; current index state and future index state... as we don't know
                 ;; yet if 'sync' option is used, but need to return a where/Matcher proto
                 (assoc :t 0
                        :initialized (util/current-time-millis)
                        :index-state (atom bm25/initialized-index)
                        :namespaces namespaces
                        :namespace-codes namespace-codes
                        :db-alias alias))
        bm25 (map->BM25-VirtualGraph opts)]
    bm25))

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
                         (bm25-idx? type) (new-bm25-index db vg-flakes vg-opts))
        _              (when (nil? vg)
                         (throw (ex-info "Unrecognized virtual graph creation attempted."
                                         {:status 400
                                          :error  :db/invalid-index})))
        initialized-vg (initialize vg db*)]
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
               (assoc vg* vg-alias (upsert vg-impl db add remove)))
             {} vg)]
    (assoc db :vg vg*)))
