(ns fluree.db.flake.index.hyperloglog
  "HyperLogLog sketch persistence for NDV (Number of Distinct Values) tracking.

   Sketches are stored at fixed t-based paths:
   - <ledger>/index/stats-sketches/values/<ns-code>_<name>_<t>.hll
   - <ledger>/index/stats-sketches/subjects/<ns-code>_<name>_<t>.hll

   This enables predictable loading without tracking addresses."
  (:require [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log :include-macros true]))

(defn sketch-filename
  "Generate filename for a sketch file.
   type is either :values or :subjects"
  [ledger-name ^fluree.db.json_ld.iri.SID sid-obj type t]
  (let [ns-code (.-namespace_code sid-obj)
        name    (.-name sid-obj)
        subdir  (case type
                  :values "values"
                  :subjects "subjects")]
    (str ledger-name "/index/stats-sketches/" subdir "/" ns-code "_" name "_" t ".hll")))

(defn- write-bytes-to-path
  "Write raw bytes to a fixed path in storage (not content-addressed).
   Uses storage/write-bytes which properly handles path-based storage."
  [catalog path bytes]
  (go-try
    (let [default-key (keyword "fluree.db.storage" "default")
          store       (storage/get-content-store catalog default-key)]
      (<? (storage/write-bytes store path bytes)))))

(defn- read-bytes-from-path
  "Read raw bytes from a fixed path in storage (not content-addressed).
   Returns bytes or nil if file does not exist."
  [catalog path]
  (go-try
    (try*
      (let [default-key (keyword "fluree.db.storage" "default")
            store       (storage/get-content-store catalog default-key)]
        (<? (storage/read-bytes store path)))
      (catch* e
        (log/debug "read-bytes-from-path error for path:" path)
        nil))))

(defn write-sketches
  "Write statistics sketches to storage using fixed t-based filenames.
   Only writes sketches for properties where :last-modified-t = current t (modified in this index).
   Each property's values and subjects sketches are stored as raw bytes.
   Format: <ledger-name>/index/stats-sketches/values/<ns-code>_<name>_<t>.hll
           <ledger-name>/index/stats-sketches/subjects/<ns-code>_<name>_<t>.hll
   Returns set of old sketch file paths to add to garbage collection.

   old-sketch-t-map: Map of {sid -> old-t} for properties that had previous sketch files."
  [{:keys [storage] :as _index-catalog} alias t old-sketch-t-map current-properties-map sketches]
  (go-try
    (when (seq sketches)
      (let [ledger-name (util.ledger/ledger-base-name alias)
            ;; Only write sketches for properties modified in this index
            modified-sids (filter #(= t (:last-modified-t (get current-properties-map %)))
                                  (keys sketches))
            old-paths (atom #{})]
        (loop [[sid-obj & rest-sids] modified-sids]
          (when sid-obj
            (let [sketch-data (get sketches sid-obj)
                  {:keys [values subjects]} sketch-data
                  old-t (get old-sketch-t-map sid-obj)]
              (try*
                ;; Write values sketch at new t (raw bytes)
                (let [path (sketch-filename ledger-name sid-obj :values t)]
                  (<? (write-bytes-to-path storage path values)))
                ;; Write subjects sketch at new t (raw bytes)
                (let [path (sketch-filename ledger-name sid-obj :subjects t)]
                  (<? (write-bytes-to-path storage path subjects)))

                ;; Add old sketch paths to garbage (if they existed at a different t)
                ;; Paths must use fluree:file:// prefix to match other garbage items
                (when (and old-t (not= old-t t))
                  (swap! old-paths conj
                         (str "fluree:file://" (sketch-filename ledger-name sid-obj :values old-t))
                         (str "fluree:file://" (sketch-filename ledger-name sid-obj :subjects old-t))))
                (catch* e
                  (log/error e "Error writing sketch for" sid-obj)))
              (recur rest-sids))))
        @old-paths))))

(defn read-property-sketches
  "Read sketches for a specific property from the previous index.
   Returns map with :values and :subjects sketch byte arrays, or nil if not found."
  [{:keys [storage] :as _index-catalog} ledger-name sid-obj prev-t]
  (go-try
    (when (and prev-t (pos? prev-t))
      (let [values-path   (sketch-filename ledger-name sid-obj :values prev-t)
            subjects-path (sketch-filename ledger-name sid-obj :subjects prev-t)
            ;; Read raw bytes directly
            values-sketch   (try*
                              (<? (read-bytes-from-path storage values-path))
                              (catch* e
                                (log/debug "Failed to read values sketch for property" sid-obj "at t" prev-t
                                           "- may be new property or legacy index")
                                nil))
            subjects-sketch (try*
                              (<? (read-bytes-from-path storage subjects-path))
                              (catch* e
                                (log/debug "Failed to read subjects sketch for property" sid-obj "at t" prev-t
                                           "- may be new property or legacy index")
                                nil))]
        (cond-> {}
          (some? values-sketch)   (assoc :values values-sketch)
          (some? subjects-sketch) (assoc :subjects subjects-sketch))))))

(defn load-previous-sketches
  "Load all sketches from the previous index for properties that exist in novelty.
   Returns map of {sid {:values ... :subjects ...}} for properties found in previous index."
  [index-catalog ledger-name prev-indexed-t novelty-property-sids]
  (go-try
    (when (and prev-indexed-t (pos? prev-indexed-t))
      (loop [[sid & rest-sids] novelty-property-sids
             result {}]
        (if sid
          (let [sketches (<? (read-property-sketches index-catalog ledger-name sid prev-indexed-t))]
            (recur rest-sids
                   (if (seq sketches)
                     (assoc result sid sketches)
                     result)))
          result)))))

(defn load-sketches-by-last-modified
  "Load sketches for ALL properties using their :last-modified-t from the properties map.
   This ensures we load sketches even for properties not in current novelty.
   For migration: if property lacks :last-modified-t, uses prev-indexed-t as fallback.
   Returns map of {sid {:values ... :subjects ...}} for properties found in storage."
  [index-catalog ledger-name properties-map prev-indexed-t]
  (go-try
    (loop [[sid & rest-sids] (keys properties-map)
           result {}]
      (if sid
        (let [prop-data (get properties-map sid)
              last-t    (or (:last-modified-t prop-data) prev-indexed-t)] ; Migration fallback
          (if (and last-t (pos? last-t))
            (let [sketches (<? (read-property-sketches index-catalog ledger-name sid last-t))]
              (recur rest-sids
                     (if (seq sketches)
                       (assoc result sid sketches)
                       result)))
            ;; No t available at all (brand new property), skip loading
            (recur rest-sids result)))
        result))))

