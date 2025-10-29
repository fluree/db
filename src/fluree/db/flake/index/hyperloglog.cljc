(ns fluree.db.flake.index.hyperloglog
  "HyperLogLog sketch persistence for NDV (Number of Distinct Values) tracking.

   Sketches are stored at fixed t-based paths:
   - <ledger>/index/stats-sketches/values/<ns-code>_<name>_<t>.hll
   - <ledger>/index/stats-sketches/subjects/<ns-code>_<name>_<t>.hll

   This enables predictable loading without tracking addresses."
  (:require [clojure.string :as str]
            [fluree.db.storage :as storage]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.filesystem :as fs]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log :include-macros true]))

(defn sketch-filename
  "Generate filename for a sketch file.
   type is either :values or :subjects"
  [ledger-name sid-obj type t]
  (let [ns-code (.-namespace_code sid-obj)
        name    (.-name sid-obj)
        subdir  (case type
                  :values "values"
                  :subjects "subjects")]
    (str ledger-name "/index/stats-sketches/" subdir "/" ns-code "_" name "_" t ".hll")))

(defn- write-json-to-path
  "Write JSON data to a fixed path in storage (not content-addressed).
   Data should be a map that will be serialized to JSON.
   Uses storage/write-bytes which properly handles path-based storage."
  [catalog path data]
  (go-try
    (let [default-key (keyword "fluree.db.storage" "default")
          store       (storage/get-content-store catalog default-key)
          json-str    (json/stringify data)
          bytes       (bytes/string->UTF8 json-str)]
      (<? (storage/write-bytes store path bytes)))))

(defn- read-json-from-path
  "Read JSON data from a fixed path in storage (not content-addressed).
   Returns the parsed JSON as a Clojure data structure with string keys."
  [catalog path]
  (go-try
    (try*
      (let [default-key (keyword "fluree.db.storage" "default")
            store       (storage/get-content-store catalog default-key)]
        (when-let [bytes (<? (storage/read-bytes store path))]
          (json/parse bytes false)))
      (catch* e
        (log/debug "read-json-from-path error for path:" path)
        nil))))

(defn write-sketches
  "Write statistics sketches to storage using fixed t-based filenames.
   Each property's values and subjects sketches are stored in JSON format with base64 encoding.
   Format: <ledger-name>/sketches/values/<ns-code>_<name>_<t>.hll
           <ledger-name>/sketches/subjects/<ns-code>_<name>_<t>.hll
   Returns nil (no need to track addresses with fixed filenames)."
  [{:keys [storage] :as _index-catalog} alias t sketches]
  (go-try
    (when (seq sketches)
      (let [ledger-name (util.ledger/ledger-base-name alias)
            sketch-seq  (seq sketches)]
        (loop [[[sid-obj {:keys [values subjects]}] & rest-sketches] sketch-seq]
          (when sid-obj
            (try*
              ;; Write values sketch
              (let [path (sketch-filename ledger-name sid-obj :values t)
                    sketch-b64 #?(:clj (.encodeToString (java.util.Base64/getEncoder) values)
                                  :cljs (.toString (.from js/Buffer values) "base64"))
                    data {:sid sid-obj :t t :sketch sketch-b64}]
                (<? (write-json-to-path storage path data)))
              ;; Write subjects sketch
              (let [path (sketch-filename ledger-name sid-obj :subjects t)
                    sketch-b64 #?(:clj (.encodeToString (java.util.Base64/getEncoder) subjects)
                                  :cljs (.toString (.from js/Buffer subjects) "base64"))
                    data {:sid sid-obj :t t :sketch sketch-b64}]
                (<? (write-json-to-path storage path data)))
              (catch* e
                (log/error e "Error writing sketch for" sid-obj)))
            (recur rest-sketches)))
        nil))))

(defn read-property-sketches
  "Read sketches for a specific property from the previous index.
   Returns map with :values and :subjects sketch byte arrays, or nil if not found."
  [{:keys [storage] :as _index-catalog} ledger-name sid-obj prev-t]
  (go-try
    (when (and prev-t (pos? prev-t))
      (let [values-path   (sketch-filename ledger-name sid-obj :values prev-t)
            subjects-path (sketch-filename ledger-name sid-obj :subjects prev-t)
            values-data   (try*
                            (<? (read-json-from-path storage values-path))
                            (catch* e
                              (log/debug "Failed to read values sketch for property" sid-obj "at t" prev-t
                                         "- may be new property or legacy index")
                              nil))
            subjects-data (try*
                            (<? (read-json-from-path storage subjects-path))
                            (catch* e
                              (log/debug "Failed to read subjects sketch for property" sid-obj "at t" prev-t
                                         "- may be new property or legacy index")
                              nil))

            ;; Decode base64 strings back to byte arrays
            ;; Note: JSON parsing returns string keys, not keyword keys
            values-b64      (get values-data "sketch")
            subjects-b64    (get subjects-data "sketch")
            values-sketch   (when values-b64
                              #?(:clj (.decode (java.util.Base64/getDecoder) ^String values-b64)
                                 :cljs (.from js/Buffer values-b64 "base64")))
            subjects-sketch (when subjects-b64
                              #?(:clj (.decode (java.util.Base64/getDecoder) ^String subjects-b64)
                                 :cljs (.from js/Buffer subjects-b64 "base64")))]
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

(defn delete-sketch-file
  "Delete a single sketch file from storage.
   Returns true if deleted, false if file didn't exist or error occurred.
   Errors are logged but not thrown (idempotent, graceful failure)."
  [{:keys [storage] :as _index-catalog} path]
  (go-try
    (try*
      (let [default-key (keyword "fluree.db.storage" "default")
            store       (storage/get-content-store storage default-key)
            root        (:root store)
            full-path   (str/join "/" [root path])
            result      (<? (fs/delete-file full-path))]
        (= :deleted result))
      (catch* e
        ;; File may not exist (new property, already deleted, etc) - this is OK
        (log/debug "Could not delete sketch file (may not exist):" path)
        false))))

(defn delete-property-sketches
  "Delete both values and subjects sketch files for a property at a given t.
   Returns map with :values-deleted and :subjects-deleted booleans.
   Gracefully handles missing files (returns false for that type)."
  [index-catalog ledger-name sid-obj t]
  (go-try
    (let [values-path   (sketch-filename ledger-name sid-obj :values t)
          subjects-path (sketch-filename ledger-name sid-obj :subjects t)
          values-deleted   (<? (delete-sketch-file index-catalog values-path))
          subjects-deleted (<? (delete-sketch-file index-catalog subjects-path))]
      {:values-deleted values-deleted
       :subjects-deleted subjects-deleted})))

(defn delete-sketches-for-index
  "Delete all sketch files for properties in an index at time t.
   Returns map with :deleted-count (total files deleted) and :total-count (total files attempted).
   Gracefully handles missing files and logs summary."
  [index-catalog ledger-name property-sids t]
  (go-try
    (loop [[sid & rest-sids] (seq property-sids)
           deleted-count 0
           total-count 0]
      (if sid
        (let [{:keys [values-deleted subjects-deleted]} (<? (delete-property-sketches index-catalog ledger-name sid t))
              deleted-count* (+ deleted-count
                                (if values-deleted 1 0)
                                (if subjects-deleted 1 0))
              total-count* (+ total-count 2)] ; always attempt both values and subjects
          (recur rest-sids deleted-count* total-count*))
        {:deleted-count deleted-count
         :total-count total-count}))))
