(ns fluree.db.ndjson
  "NDJSON (Newline-Delimited JSON-LD) parsing utilities.

   Provides pull-based streaming of NDJSON files with proper backpressure handling.

   Supports three context modes:
   1. Shared context provided via opts - applied to all lines
   2. First line is context-only ({@context: {...}} with no data) - applied to subsequent lines
   3. Inline context per document - each line has its own @context

   JVM-only namespace."
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [clojure.string :as str]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log])
  (:import (java.io BufferedReader Reader InputStream InputStreamReader)))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; Line reading
;;; ---------------------------------------------------------------------------

(defn- read-next-line
  "Reads the next non-empty line from the reader. Returns [line-num content] or nil at EOF.
   Skips blank lines and lines containing only whitespace, but tracks actual file line numbers."
  [^BufferedReader reader line-num]
  (loop [current-line-num line-num]
    (when-let [line (.readLine reader)]
      (let [trimmed (str/trim line)]
        (if (str/blank? trimmed)
          (recur (inc current-line-num))
          [current-line-num trimmed])))))

(defn reader->line-ch
  "Creates a channel that emits lines from a BufferedReader with pull-based flow control.

   The channel has a buffer of 1 to provide natural backpressure - the reader
   will only read the next line when the previous one has been consumed.

   Each emitted value is a map with:
     :line-num - 1-based line number (tracks actual file lines, including skipped blanks)
     :content  - the trimmed line content (string)

   Options:
     :buffer-size - channel buffer size (default 1)

   Returns a channel that closes when EOF is reached or reader is closed."
  ([reader] (reader->line-ch reader {}))
  ([reader {:keys [buffer-size] :or {buffer-size 1}}]
   (let [^BufferedReader br (if (instance? BufferedReader reader)
                              reader
                              (BufferedReader. ^Reader reader))
         out-ch (async/chan buffer-size)]
     (go-loop [line-num 1]
       (if-let [result (try
                         (read-next-line br line-num)
                         (catch Exception e
                           (log/error e "Error reading NDJSON line" line-num)
                           (>! out-ch (ex-info "Error reading NDJSON line"
                                               {:line-num line-num
                                                :status   400
                                                :error    :db/ndjson-read-error}
                                               e))
                           nil))]
         (let [[actual-line-num content] result]
           (>! out-ch {:line-num actual-line-num
                       :content  content})
           (recur (inc actual-line-num)))
         (async/close! out-ch)))
     out-ch)))

(defn input-stream->line-ch
  "Convenience function to create a line channel from an InputStream.
   Uses UTF-8 encoding."
  ([^InputStream input-stream]
   (input-stream->line-ch input-stream {}))
  ([^InputStream input-stream opts]
   (reader->line-ch (InputStreamReader. input-stream "UTF-8") opts)))

;;; ---------------------------------------------------------------------------
;;; JSON parsing
;;; ---------------------------------------------------------------------------

(defn parse-line
  "Parses a single NDJSON line into a Clojure data structure.

   Returns the parsed JSON-LD document on success.
   Throws an exception on parse error with line number context."
  [line-str line-num]
  (try
    (json/parse line-str false) ; don't keywordize to preserve @context, @id etc.
    (catch Exception e
      (throw (ex-info "Invalid JSON on NDJSON line"
                      {:line-num line-num
                       :status   400
                       :error    :db/ndjson-parse-error
                       :content  (subs line-str 0 (min 100 (count line-str)))}
                      e)))))

(defn context-only-line?
  "Returns true if the parsed JSON contains only @context and no data.
   This indicates a shared context line that applies to subsequent documents."
  [parsed]
  (and (map? parsed)
       (contains? parsed "@context")
       (= 1 (count parsed))))

;;; ---------------------------------------------------------------------------
;;; Context management
;;; ---------------------------------------------------------------------------

(defn merge-contexts
  "Merges a shared context with a document's inline context.

   If the document has no context, returns the shared context.
   If the document has its own context, it takes precedence (JSON-LD semantics)."
  [shared-ctx doc-ctx]
  (cond
    (nil? doc-ctx)        shared-ctx
    (nil? shared-ctx)     doc-ctx
    (sequential? doc-ctx) (into [shared-ctx] (if (sequential? doc-ctx) doc-ctx [doc-ctx]))
    (map? doc-ctx)        [shared-ctx doc-ctx]
    :else                 [shared-ctx doc-ctx]))

(defn context-for-document
  "Determines the effective context for a document.

   Parameters:
     opts-context - context provided via options
     shared-context - context from first line (if context-only)
     doc - the parsed document

   Returns the effective context to use for JSON-LD expansion."
  [opts-context shared-context doc]
  (let [doc-context (get doc "@context")]
    (-> opts-context
        (merge-contexts shared-context)
        (merge-contexts doc-context))))

(defn prepare-document
  "Prepares a document for insertion by ensuring it has the effective context.

   If the document already has an @context, it's merged with shared context.
   If the document has no @context, the shared context is added."
  [opts-context shared-context doc]
  (let [effective-ctx (context-for-document opts-context shared-context doc)]
    (if effective-ctx
      (assoc doc "@context" effective-ctx)
      doc)))

;;; ---------------------------------------------------------------------------
;;; Streaming document channel
;;; ---------------------------------------------------------------------------

(defn- process-first-line
  "Processes the first line to determine if it's a context-only line.

   Returns a map with:
     :shared-context - parsed context if first line was context-only, nil otherwise
     :first-doc - the first document if first line was data, nil if context-only"
  [first-line opts-context]
  (let [parsed (parse-line (:content first-line) (:line-num first-line))]
    (if (context-only-line? parsed)
      {:shared-context (get parsed "@context")
       :first-doc      nil}
      {:shared-context nil
       :first-doc      (prepare-document opts-context nil parsed)})))

(defn line-ch->doc-ch
  "Transforms a line channel into a document channel.

   Handles context detection from first line and applies context merging.

   Each emitted value is either:
   - A map with :line-num and :doc for successful parsing
   - A Throwable for parse errors

   The consumer is responsible for handling errors according to its error mode.
   Parse errors do NOT close the channel - processing continues with remaining lines.

   Options:
     :context - shared context to apply to all documents

   Returns a channel of prepared documents and/or errors."
  ([line-ch] (line-ch->doc-ch line-ch {}))
  ([line-ch {:keys [context] :as _opts}]
   (let [out-ch (async/chan 1)]
     (go
       (if-let [first-line (<! line-ch)]
         (if (instance? Throwable first-line)
           ;; Read error from line-ch - emit and close
           (do
             (>! out-ch first-line)
             (async/close! out-ch))
           (let [first-result (try
                                (process-first-line first-line context)
                                (catch Exception e
                                  {:error e}))]
             (if-let [e (:error first-result)]
               ;; First line parse error - emit error and close (can't determine context)
               (do
                 (>! out-ch e)
                 (async/close! out-ch))
               (let [{:keys [shared-context first-doc]} first-result]
                 ;; If first line was a document (not context-only), emit it
                 (when first-doc
                   (>! out-ch {:line-num (:line-num first-line)
                               :doc      first-doc}))
                 ;; Process remaining lines
                 (loop []
                   (if-let [line (<! line-ch)]
                     (if (instance? Throwable line)
                       ;; Read error from line-ch - emit and close
                       (do
                         (>! out-ch line)
                         (async/close! out-ch))
                       ;; Parse line - emit result (doc or error) and continue
                       (let [result (try
                                      {:line-num (:line-num line)
                                       :doc      (-> (:content line)
                                                     (parse-line (:line-num line))
                                                     (->> (prepare-document context shared-context)))}
                                      (catch Exception e
                                        (ex-info "Error parsing NDJSON line"
                                                 {:line-num (:line-num line)
                                                  :status   400
                                                  :error    :db/ndjson-parse-error}
                                                 e)))]
                         (>! out-ch result)
                         (recur)))
                     (async/close! out-ch)))))))
         (async/close! out-ch)))
     out-ch)))

(defn reader->doc-ch
  "Convenience function to create a document channel directly from a Reader.

   Combines reader->line-ch and line-ch->doc-ch.

   Options:
     :context - shared context to apply to all documents
     :buffer-size - line channel buffer size (default 1)"
  ([reader] (reader->doc-ch reader {}))
  ([reader opts]
   (-> reader
       (reader->line-ch opts)
       (line-ch->doc-ch opts))))

;;; ---------------------------------------------------------------------------
;;; Batch accumulation
;;; ---------------------------------------------------------------------------

(defn create-batch-accumulator
  "Creates a stateful batch accumulator for streaming inserts.

   Options:
     :max-batch-lines - max documents per batch (default 10000)
     :batch-novelty-fn - function to check if batch should commit based on novelty

   Returns a map with:
     :add-doc - function to add a document, returns {:flush? bool :batch [docs]}
     :flush   - function to get current batch and reset
     :stats   - function to get current stats"
  [{:keys [max-batch-lines]
    :or   {max-batch-lines 10000}}]
  (let [state (atom {:docs       []
                     :line-nums  []
                     :count      0
                     :start-time (System/currentTimeMillis)})]
    {:add-doc
     (fn [doc line-num]
       (let [{:keys [count]} (swap! state
                                    (fn [s]
                                      (-> s
                                          (update :docs conj doc)
                                          (update :line-nums conj line-num)
                                          (update :count inc))))]
         {:flush? (>= count max-batch-lines)
          :count  count}))

     :flush
     (fn []
       (let [batch @state]
         (reset! state {:docs       []
                        :line-nums  []
                        :count      0
                        :start-time (System/currentTimeMillis)})
         batch))

     :current
     (fn []
       @state)

     :stats
     (fn []
       (let [{:keys [count start-time]} @state]
         {:count    count
          :duration (- (System/currentTimeMillis) start-time)}))}))
