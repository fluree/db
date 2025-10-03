(ns fluree.db.virtual-graph.bm25.index
  (:require [clojure.core.async :as async :refer [go alts! put! promise-chan <! >!]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.exec.where :as where]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [empty-channel]]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]
            [fluree.db.virtual-graph.bm25.search :as bm25.search]
            [fluree.db.virtual-graph.bm25.stemmer :as stm]
            [fluree.db.virtual-graph.bm25.stopwords :as stopwords]
            [fluree.db.virtual-graph.bm25.storage] ;; Load for multimethod registration
            [fluree.db.virtual-graph.bm25.update :as bm25.update]
            [fluree.db.virtual-graph.parse :as vg-parse])
  (:refer-clojure :exclude [assert]))

(set! *warn-on-reflection* true)

;; TODO - VG - add 'lang' property to pull that out - right now everything is english
(defn idx-flakes->opts-map
  [index-flakes]
  (reduce
   (fn [acc idx-flake]
     (cond
       (= (flake/p idx-flake) const/$fluree:index-b)
       (let [b (flake/o idx-flake)]
         (if (and (number? b) (<= 0 b) (<= b 1))
           (assoc acc :b b)
           (throw (ex-info (str "Invalid B value provided for Bm25 index, must be a number between 0 and 1, but found: " b)
                           {:status 400
                            :error  :db/invalid-index}))))

       (= (flake/p idx-flake) const/$fluree:index-k1)
       (let [k1 (flake/o idx-flake)]
         (if (and (number? k1) (<= 0 k1))
           (assoc acc :k1 k1)
           (throw (ex-info (str "Invalid K1 value provided for Bm25 index, must be a number greater than 0, but found: " k1)
                           {:status 400
                            :error  :db/invalid-index}))))

       :else acc))
   ;; TODO - once protocol is established, can remove :vg-type key
   {:b    0.75
    :k1   1.2
    :lang "en"}
   index-flakes))

(def initialized-index
  ;; there is always a 'ready to use' current index
  {:index          {:vectors    {}
                    :dimensions 0
                    :item-count 0
                    :avg-length 0
                    :terms      {}}
   ;; pending-ch will contain a promise-chan of index state as of immutable db-t value
   ;; once complete, will replace ':current' index with finished pending one
   :pending-ch     nil
   ;; pending-status will contain two-tuple of [items-complete total-items] which can be divided for % complete
   :pending-status nil})

(defn add-stemmer
  [{:keys [lang] :as opts}]
  (assoc opts :stemmer (stm/initialize lang)))

(defn add-stopwords
  [{:keys [lang] :as opts}]
  (assoc opts :stopwords (stopwords/initialize lang)))

(defn idx-flakes->opts
  [index-flakes]
  (-> index-flakes
      (idx-flakes->opts-map)
      (add-stemmer)
      (add-stopwords)))

(defn percent-complete-str
  [index-state]
  (let [{:keys [pending-status]} @index-state
        [processed-n item-count] (or pending-status [0 0])
        percentage (when (and (pos-int? processed-n) (pos-int? item-count))
                     (int (* 100 (/ processed-n item-count))))]
    (cond
      percentage (str "Index is " percentage "% complete.")
      (pos-int? processed-n) (str "Index has processed " processed-n " items of an unknown total to process.")
      (and (zero? processed-n) (zero? item-count)) "Index is 100% complete." ;; when updates have no items to process
      :else "Index is 0% complete.")))

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
    (try
      (let [{::vg-parse/keys [target limit timeout] :as search-params} (vg-parse/get-search-params solution)
            _ (when-not target
                (throw (ex-info "No search target for virtual graph. Did you forget @context in your query?"
                                {:status 400 :error
                                 :db/invalid-query})))
            ;; Guard against empty/blank targets
            _ (when (and (string? target) (str/blank? target))
                (async/onto-chan! out-ch []))
            {:keys [pending-ch index]} @index-state

            ;; TODO - check for "sync" options and don't wait for pending-ch if sync is false

            index*      (if pending-ch
                          (let [timeout*   (or timeout 10000)
                                timeout-ch (async/timeout timeout*)
                                [idx ch] (alts! [timeout-ch pending-ch])]
                            (if (= timeout-ch ch)
                              (put! error-ch (ex-info (str "Timeout waiting for BM25 index to sync after "
                                                           timeout* "ms. " (percent-complete-str index-state)
                                                           " Please try again later. To configure a "
                                                           "different timeout, set " const/iri-index-timeout " in the virtual "
                                                           "graph query to the desired number of milliseconds.")
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
             (vg-parse/process-sparse-results bm25 solution search-params)
             (async/onto-chan! out-ch)))
      (catch Exception e
        (>! error-ch e)))))

(defn bm25-upsert*
  [{:keys [index-state] :as bm25} {:keys [t alias namespaces namespace-codes] :as _db} items-count items-ch]
  (let [{:keys [pending-ch index] :as prior-idx-state} @index-state
        new-pending-ch  (promise-chan)
        new-index-state (atom (assoc prior-idx-state :pending-ch new-pending-ch))]

    ;; following go-block happens asynchronously in the background
    ;; TODO - VG - capture error conditions in async/<! or other opts below and resolve the response with an error.
    (go
      (try
        (let [latest-index  (if pending-ch
                              (<! pending-ch)
                              index)
              status-update (fn [status]
                              (swap! new-index-state assoc :pending-status status))
              new-index     (<! (bm25.update/upsert-items bm25 latest-index items-count items-ch status-update))
              catalog       (:index-catalog bm25)]
          ;; reset index state atom once index is complete, remove pending-ch
          (swap! new-index-state (fn [idx-state]
                                   (assoc idx-state :index new-index
                                          :pending-ch nil)))
          (log/debug "BM25 index update complete, writing to storage...")
          ;; Persist the updated BM25 index to storage
          (when catalog
            (let [updated-bm25 (assoc bm25 :index-state new-index-state :t t)]
              (log/debug "Writing BM25 index to storage for VG:" (:alias updated-bm25))
              (try
                (<! (vg/write-vg catalog updated-bm25))
                (catch Exception e
                  (log/error e "Failed to write BM25 index to storage")))))
          (>! new-pending-ch new-index))
        (catch Exception e
          (log/error e "Error in BM25 index update"))))

    ;; new bm25 record returned to get attached to db
    (assoc bm25 :t t
           :namespaces namespaces
           :namespace-codes namespace-codes
                ;; unlikely, but in case db's alias has been changed keep in sync
           :db-alias alias
           :index-state new-index-state)))

(defn property-dependencies
  [vg]
  (:property-deps vg))

(defn parsed-query
  [vg]
  (:parsed-query vg))

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

(defn upsert-queries
  [db parsed-query affected-iris]
  (let [results-ch (async/chan)
        iri-var    (-> parsed-query :select :subj)]
    (async/go
      (loop [[next-iri & r] affected-iris]
        (if next-iri
          (let [values [{iri-var (where/match-iri next-iri)}]
                pq     (assoc parsed-query :values values)
                result (first (async/<! (exec/query db nil pq)))] ;; inject one IRI into :values, expect one result

            (if (util/exception? result)
              (log/warn "BM25 upsert query failed for IRI:" next-iri "with exception message:" (ex-message result) "Skipping")
              (async/>! results-ch (if (nil? result)
                                     [::bm25.update/retract {const/iri-id next-iri}]
                                     [::bm25.update/upsert result])))
            (recur r))
          (async/close! results-ch))))
    results-ch))

(defn bm25-upsert
  [bm25 db add removes]
  (let [prop-deps      (property-dependencies bm25)
        affected-sids  (affected-subjs prop-deps add removes)
        affected-iris  (map #(iri/decode-sid db %) affected-sids)
        items-count    (count affected-iris)
        pq             (parsed-query bm25)
        upsert-docs-ch (upsert-queries db pq affected-iris)]

    (bm25-upsert* bm25 db items-count upsert-docs-ch)))

(defn bm25-initialize
  [{:keys [parsed-query] :as bm25} db]
  (let [query-result (exec/query db nil parsed-query)
        items-ch     (async/chan 1 (map #(vector ::bm25.update/upsert %)))]
    ;; break up query results into individual document items on a new chan
    (async/pipeline-async 1 items-ch #(async/onto-chan! %2 %1) query-result)
    (bm25-upsert* bm25 db nil items-ch)))

(defrecord BM25-VirtualGraph
           [stemmer stopwords k1 b index-state initialized genesis-t t
            alias vg-name query parsed-query property-deps
   ;; following taken from db - needs to be kept up to date with new db updates
            db-alias namespaces namespace-codes index-catalog]

  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes))

  vg/UpdatableVirtualGraph
  (upsert [this source-db new-flakes remove-flakes]
    (bm25-upsert this source-db new-flakes remove-flakes))
  (initialize [this source-db]
    (go
      (let [initialized-bm25 (bm25-initialize this source-db)
            {:keys [index-state index-catalog]} initialized-bm25
            ;; Wait for the async indexing to complete
            pending-ch (get @index-state :pending-ch)]
        (when pending-ch
          (<! pending-ch))
        ;; Now write to storage
        (when index-catalog
          (try
            (<! (vg/write-vg index-catalog initialized-bm25))
            (log/debug "Successfully wrote initial BM25 index to storage")
            (catch Exception e
              (log/error e "Failed to write initial BM25 index to storage"))))
        initialized-bm25)))

  vg/SyncableVirtualGraph
  (sync [this as-of]
    (vg/sync this as-of {}))
  (sync [this as-of {:keys [timeout] :or {timeout 10000}}]
    (go
      (let [{:keys [pending-ch]} @index-state
            target-t (or as-of t)]  ;; Use current t if as-of is nil
        (cond
          ;; If we're already at or past the requested t, we're synced
          (>= t target-t)
          this

          ;; If there's a pending update, wait for it with timeout
          pending-ch
          (let [timeout-ch (async/timeout timeout)
                [v ch] (alts! [pending-ch timeout-ch])]
            (if (= ch timeout-ch)
              (throw (ex-info (str "Timeout waiting for BM25 index sync. "
                                   (percent-complete-str index-state))
                              {:status 504
                               :error :db/timeout
                               :vg-name vg-name
                               :target-t target-t
                               :current-t t}))
              ;; Successfully synced, return updated VG
              (assoc this :index-state (atom {:index v
                                              :pending-ch nil
                                              :pending-status nil})
                     :t target-t)))

          ;; No pending update and we're not at target t
          :else
          (throw (ex-info "BM25 index not yet updated to requested transaction"
                          {:status 409
                           :error :db/index-behind
                           :vg-name vg-name
                           :target-t target-t
                           :current-t t}))))))

  where/Matcher
  (-match-triple [_ _tracker solution triple _error-ch]
    (vg-parse/match-search-triple solution triple))

  (-finalize [this _tracker error-ch solution-ch]
    (vg-parse/finalize (partial search this) error-ch solution-ch))

  (-match-id [_ _tracker _solution _s-mch _error-ch]
    empty-channel)

  (-match-class [_ _tracker _solution _s-mch _error-ch]
    empty-channel)

  (-match-properties [this tracker solution triples error-ch]
    (where/match-triples this tracker solution triples error-ch))

  ;; activate-alias returns the VG itself when the alias matches
  (-activate-alias [this alias']
    (go
      (when (= alias alias')
        this)))

  ;; return the VG alias
  (-aliases [_]
    [alias])

  dbproto/IFlureeDb
  (-query [_this _tracker _query]
    ;; BM25 virtual graphs handle queries through the where/Matcher protocol
    ;; This method should not be called directly on BM25 instances
    (throw (ex-info "BM25 virtual graphs do not support direct query execution"
                    {:status 400
                     :error :db/unsupported-operation})))

  (-class-ids [_this _tracker _subject-id]
    ;; BM25 indexes don't store class information
    (async/to-chan! []))

  (-index-update [this _commit-index]
    ;; BM25 indexes are updated through the UpdatableVirtualGraph protocol
    ;; This is a no-op for compatibility
    this))

;; TODO - VG - triggering updates only works for queries for single subject, no nested nodes
;; TODO - VG - future feature - weighted properties
;; TODO - VG - drop index
(defn new-bm25-index
  [{:keys [namespaces namespace-codes alias index-catalog] :as db} index-flakes vg-opts]
  (-> (idx-flakes->opts index-flakes)
      (merge vg-opts)
      ;; index-state held as atom, as we need -match-triple, etc. to hold both
      ;; current index state and future index state... as we don't know yet if
      ;; 'sync' option is used, but need to return a where/Matcher proto
      (assoc :t 0
             :initialized (util/current-time-millis)
             :index-state (atom initialized-index)
             :namespaces namespaces
             :namespace-codes namespace-codes
             :db-alias alias
             :index-catalog index-catalog
             :vg-name (:vg-name vg-opts))
      (vg-parse/parse-document-query (volatile! db))
      map->BM25-VirtualGraph))
