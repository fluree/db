(ns fluree.db.api.stream
  "Streaming insert API for processing NDJSON (newline-delimited JSON-LD) input.

   Provides backpressure-aware batch processing with progress reporting.

   Main entry points:
   - stream-insert  - Stage documents into a db without committing
   - stream-insert! - Stage and commit documents in batches with backpressure handling

   JVM-only namespace."
  (:require [clojure.core.async :as async :refer [<! >! chan close! go]]
            [clojure.java.io :as io]
            [fluree.db.api.transact :as transact-api]
            [fluree.db.connection :as connection]
            [fluree.db.ledger :as ledger]
            [fluree.db.ndjson :as ndjson]
            [fluree.db.query.fql.parse :as parse]
            [fluree.db.transact :as transact]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log])
  (:import (java.io BufferedReader Reader InputStream InputStreamReader File)))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Constants and defaults
;;; ---------------------------------------------------------------------------

(def default-batch-threshold 0.7)
(def default-max-batch-lines 10000)
(def default-max-batch-time-ms 5000)
(def default-max-backpressure-wait-ms 300000) ; 5 minutes
(def default-resume-threshold 0.5)
(def backpressure-initial-delay-ms 100)
(def backpressure-max-delay-ms 5000)

;;; ---------------------------------------------------------------------------
;;; Backpressure handling
;;; ---------------------------------------------------------------------------

(defn- backpressure-delay
  "Calculates exponential backoff delay with jitter.
   Returns delay in milliseconds."
  [attempt]
  (let [base-delay (* backpressure-initial-delay-ms (Math/pow 2 attempt))
        capped-delay (min base-delay backpressure-max-delay-ms)
        jitter-factor (+ 0.5 (rand 1.0))] ; 50-150% of base
    (long (* capped-delay jitter-factor))))

(defn- novelty-ratio
  "Returns the current novelty as a ratio of max-novelty (0.0 to 1.0+)."
  [db]
  (let [novelty-size (get-in db [:novelty :size] 0)
        max-novelty (:reindex-max-bytes db 100000000)]
    (if (pos? max-novelty)
      (/ (double novelty-size) (double max-novelty))
      0.0)))

(defn- at-max-novelty?
  "Safe wrapper for checking if db is at max novelty.
   Returns false if db doesn't have the expected structure."
  [db]
  (let [novelty-size (get-in db [:novelty :size])
        max-novelty (:reindex-max-bytes db)]
    (and (some? novelty-size)
         (some? max-novelty)
         (> novelty-size max-novelty))))

(defn wait-for-indexing
  "Waits for novelty to drop below the resume threshold.

   Returns a channel that:
   - Resolves to the current db when safe to resume
   - Resolves to an exception if timeout is exceeded

   Options:
     :max-wait-ms - maximum time to wait (default 300000ms / 5 minutes)
     :resume-threshold - novelty ratio to resume at (default 0.5)
     :progress-ch - channel to emit backpressure progress events"
  [ledger {:keys [max-wait-ms resume-threshold progress-ch]
           :or   {max-wait-ms      default-max-backpressure-wait-ms
                  resume-threshold default-resume-threshold}}]
  (go-try
    (loop [attempt 0
           waited  0]
      (let [db (ledger/current-db ledger)
            ratio (novelty-ratio db)]
        (cond
          ;; Safe to resume
          (< ratio resume-threshold)
          (do
            (when progress-ch
              (>! progress-ch {:type       :backpressure
                              :action     :resumed
                              :novelty-ratio ratio
                              :waited-ms  waited}))
            db)

          ;; Timeout exceeded
          (> waited max-wait-ms)
          (throw (ex-info "Backpressure timeout waiting for indexing"
                          {:status        503
                           :error         :db/backpressure-timeout
                           :waited-ms     waited
                           :max-wait-ms   max-wait-ms
                           :novelty-ratio ratio}))

          ;; Wait and retry
          :else
          (let [delay (backpressure-delay attempt)]
            (when progress-ch
              (>! progress-ch {:type          :backpressure
                              :action        :waiting
                              :novelty-ratio ratio
                              :waited-ms     waited
                              :next-delay-ms delay}))
            (<! (async/timeout delay))
            (recur (inc attempt) (+ waited delay))))))))

;;; ---------------------------------------------------------------------------
;;; Batch management
;;; ---------------------------------------------------------------------------

(defn- should-commit-batch?
  "Determines if the current batch should be committed.

   A batch should commit when ANY of these conditions are met:
   - Novelty threshold exceeded (default 70% of max)
   - Line count exceeded (default 10000)
   - Time elapsed exceeded (default 5000ms)
   - Force flag is set (end of stream)"
  [db batch-stats {:keys [batch-threshold max-batch-lines max-batch-time-ms force?]
                   :or   {batch-threshold   default-batch-threshold
                          max-batch-lines   default-max-batch-lines
                          max-batch-time-ms default-max-batch-time-ms}}]
  (let [{:keys [count duration]} batch-stats
        ratio (novelty-ratio db)]
    (or force?
        (>= ratio batch-threshold)
        (>= count max-batch-lines)
        (>= duration max-batch-time-ms))))

;;; ---------------------------------------------------------------------------
;;; Input handling
;;; ---------------------------------------------------------------------------

(defn- coerce-to-reader
  "Coerces various input types to a BufferedReader."
  ^BufferedReader [input]
  (cond
    (instance? BufferedReader input)
    input

    (instance? Reader input)
    (BufferedReader. ^Reader input)

    (instance? InputStream input)
    (BufferedReader. (InputStreamReader. ^InputStream input "UTF-8"))

    (string? input)
    (BufferedReader. (io/reader (File. ^String input)))

    :else
    (throw (ex-info "Unsupported input type for stream-insert"
                    {:status 400
                     :error  :db/invalid-input
                     :type   (type input)}))))

;;; ---------------------------------------------------------------------------
;;; Progress reporting
;;; ---------------------------------------------------------------------------

(defn- emit-progress
  "Emits a progress event to the progress channel if provided."
  [progress-ch event]
  (when progress-ch
    (async/put! progress-ch event)))

(defn- make-progress-event
  "Creates a progress event map."
  [state extra]
  (merge {:type              :progress
          :lines-read        (:lines-read state)
          :lines-staged      (:lines-staged state)
          :batches-committed (:batches-committed state)
          :state             (:current-state state)}
         extra))

;;; ---------------------------------------------------------------------------
;;; Core streaming logic
;;; ---------------------------------------------------------------------------

(defn- stage-batch
  "Stages a batch of documents into the database.

   Returns a channel containing staged-db or an exception."
  [db docs parsed-opts]
  (go-try
    ;; Wrap docs in @graph for batch insert, including context if present
    (let [context (:context parsed-opts)
          rdf (cond-> {"@graph" docs}
                context (assoc "@context" context))
          parsed-txn (parse/parse-insert-txn rdf parsed-opts)
          result (<? (transact/stage-triples db parsed-txn))]
      ;; stage-triples returns db directly when not tracking,
      ;; or {:db db ...} when tracking
      (if (map? result) (or (:db result) result) result))))

(defn- process-batch
  "Processes and commits a batch of documents.

   Returns a channel containing the committed db or an exception."
  [ledger db docs opts state]
  (go-try
    (let [{:keys [parsed-opts progress-ch]} opts
          batch-num (inc (:batches-committed state))

          ;; Stage the batch
          staged-db (<? (stage-batch db docs parsed-opts))

          ;; Commit the batch
          committed-db (<? (ledger/commit! ledger staged-db {}))]

      (when progress-ch
        (>! progress-ch {:type          :batch-committed
                         :batch-num     batch-num
                         :lines-in-batch (count docs)
                         :t             (:t committed-db)}))

      committed-db)))

(defn stream-insert-ch
  "Low-level streaming insert that returns a channel of events.

   Parameters:
     conn      - Fluree connection
     ledger-id - Target ledger alias or address
     input     - java.io.Reader, InputStream, or file path string
     opts      - Options map (see stream-insert! for full options)

   Returns a channel that emits:
     - Progress events (when :progress-ch not provided)
     - Final result map on completion"
  [conn ledger-id input opts]
  (let [out-ch (chan 1)
        {:keys [context batch-threshold max-batch-lines max-batch-time-ms
                max-backpressure-wait-ms error-mode progress-ch]
         :or   {batch-threshold           default-batch-threshold
                max-batch-lines           default-max-batch-lines
                max-batch-time-ms         default-max-batch-time-ms
                max-backpressure-wait-ms  default-max-backpressure-wait-ms
                error-mode                :fail}} opts
        parsed-opts (transact-api/prep-opts {:context context})
        bp-opts {:max-wait-ms      max-backpressure-wait-ms
                 :resume-threshold default-resume-threshold
                 :progress-ch      progress-ch}
        batch-opts {:batch-threshold   batch-threshold
                    :max-batch-lines   max-batch-lines
                    :max-batch-time-ms max-batch-time-ms}
        full-opts (assoc opts
                         :parsed-opts parsed-opts
                         :bp-opts bp-opts
                         :batch-opts batch-opts)]

    (go
      (try
        (let [reader (coerce-to-reader input)
              doc-ch (ndjson/reader->doc-ch reader {:context context})
              ledger (<? (connection/load-ledger conn ledger-id))
              accumulator (ndjson/create-batch-accumulator
                           {:max-batch-lines max-batch-lines})]

          (loop [db (ledger/current-db ledger)
                 state {:lines-read        0
                        :lines-staged      0
                        :batches-committed 0
                        :errors            []
                        :current-state     :running}]
            (if-let [item (<! doc-ch)]
              ;; Process next document
              (if (instance? Throwable item)
                ;; Handle read/parse error
                (case error-mode
                  :fail
                  (>! out-ch {:status          :failed
                              :error           item
                              :lines-processed (:lines-staged state)
                              :batches-committed (:batches-committed state)
                              :final-t         (:t db)})

                  :skip
                  (do
                    (log/warn "Skipping error in NDJSON stream:" (ex-message item))
                    (recur db (update state :lines-read inc)))

                  :collect
                  (recur db (-> state
                                (update :lines-read inc)
                                (update :errors conj {:line-num (-> item ex-data :line-num)
                                                      :error    (ex-message item)}))))

                ;; Process valid document
                (let [{:keys [doc line-num]} item
                      _ ((:add-doc accumulator) doc line-num)
                      state' (-> state
                                 (update :lines-read inc)
                                 (update :lines-staged inc))
                      batch-stats ((:stats accumulator))]

                  ;; Check if we need to commit
                  (if (should-commit-batch? db batch-stats batch-opts)
                    ;; Commit batch
                    (let [batch ((:flush accumulator))
                          docs (:docs batch)
                          ;; Check for backpressure before staging
                          db' (if (at-max-novelty? db)
                                (do
                                  (emit-progress progress-ch
                                                 (make-progress-event
                                                  (assoc state' :current-state :backpressure) {}))
                                  (<? (wait-for-indexing ledger bp-opts)))
                                db)
                          ;; Stage and commit the batch
                          committed-db (<? (process-batch
                                            ledger db' docs
                                            full-opts state'))]
                      (recur committed-db
                             (-> state'
                                 (update :batches-committed inc)
                                 (assoc :current-state :running))))

                    ;; Continue accumulating
                    (recur db state'))))

              ;; End of stream - flush remaining batch
              (let [batch ((:flush accumulator))
                    docs (:docs batch)]
                (if (seq docs)
                  ;; Final batch to commit
                  (let [;; Check for backpressure
                        db' (if (at-max-novelty? db)
                              (<? (wait-for-indexing ledger bp-opts))
                              db)
                        committed-db (<? (process-batch
                                          ledger db' docs
                                          full-opts state))]
                    (>! out-ch {:status            :success
                                :lines-processed   (:lines-staged state)
                                :batches-committed (inc (:batches-committed state))
                                :final-t           (:t committed-db)
                                :errors            (when (= error-mode :collect)
                                                     (:errors state))}))
                  ;; No remaining documents
                  (>! out-ch {:status            (if (seq (:errors state)) :partial :success)
                              :lines-processed   (:lines-staged state)
                              :batches-committed (:batches-committed state)
                              :final-t           (:t db)
                              :errors            (when (= error-mode :collect)
                                                   (:errors state))}))))))
        (catch Exception e
          (>! out-ch {:status :failed
                      :error  e}))
        (finally
          (close! out-ch))))

    out-ch))

(defn stream-insert
  "Stages NDJSON data into a database without committing.

   This is useful for staging multiple batches before a single commit,
   or for preview/validation scenarios.

   Parameters:
     db      - Database value to stage into
     input   - java.io.Reader, InputStream, or file path string
     opts    - Options map:
       :context - Shared JSON-LD context (optional)
       :error-mode - :fail (default), :skip, or :collect

   Returns a channel resolving to:
     {:db staged-db
      :stats {:lines-staged n}
      :errors [...]}  ; if error-mode is :collect"
  ([db input] (stream-insert db input {}))
  ([db input opts]
   (go-try
     (let [{:keys [context error-mode]
            :or   {error-mode :fail}} opts
           reader (coerce-to-reader input)
           doc-ch (ndjson/reader->doc-ch reader {:context context})
           parsed-opts (transact-api/prep-opts {:context context})]

       (loop [current-db db
              staged     0
              errors     []]
         (if-let [item (<! doc-ch)]
           (if (instance? Throwable item)
             ;; Handle error based on mode
             (case error-mode
               :fail   (throw item)
               :skip   (recur current-db staged errors)
               :collect (recur current-db staged
                               (conj errors {:line-num (-> item ex-data :line-num)
                                             :error    (ex-message item)})))

             ;; Stage document
             (let [{:keys [doc]} item
                   parsed-txn (parse/parse-insert-txn doc parsed-opts)
                   result (<? (transact/stage-triples current-db parsed-txn))
                   ;; stage-triples returns db directly when not tracking,
                   ;; or {:db db ...} when tracking
                   staged-db (if (map? result) (or (:db result) result) result)]
               (recur staged-db (inc staged) errors)))

           ;; Done
           {:db     current-db
            :stats  {:lines-staged staged}
            :errors (when (= error-mode :collect) errors)}))))))

(defn stream-insert!
  "Streams NDJSON data into a ledger with automatic batching and backpressure.

   Parameters:
     conn       - Fluree connection
     ledger-id  - Target ledger alias or address
     input      - java.io.Reader, InputStream, or file path string
     opts       - Options map:
       :context              - Shared JSON-LD context (optional)
       :batch-threshold      - Novelty ratio to trigger commit (default 0.7)
       :max-batch-lines      - Max lines per batch (default 10000)
       :max-batch-time-ms    - Time-based flush interval (default 5000)
       :max-backpressure-wait-ms - Max wait for indexing (default 300000)
       :error-mode           - :fail (default), :skip, :collect
       :progress-ch          - Channel for progress events (optional)

   Returns channel resolving to:
     {:status :success | :partial | :failed
      :lines-processed n
      :batches-committed n
      :final-t n
      :errors [...]}  ; if :error-mode is :collect"
  ([conn ledger-id input]
   (stream-insert! conn ledger-id input {}))
  ([conn ledger-id input opts]
   (stream-insert-ch conn ledger-id input opts)))
