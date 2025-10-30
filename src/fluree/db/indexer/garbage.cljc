(ns fluree.db.indexer.garbage
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.flake.index.hyperloglog :as hll-persist]
            [fluree.db.flake.index.storage :as storage]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn garbage-meta-map
  "Garbage file metadata that will be passed to the
  garbage cleaning function that allows it to report
  on status and have filename addresses."
  [{:keys [prev-index garbage] :as _idx-root}]
  (when garbage ;; first index will not have garbage
    (assoc garbage
      ;; retain not the current index file, but the previous one
      ;; where the garbage in this file originated
           :t (:t prev-index)
           :index (:address prev-index))))

(defn trace-idx-roots
  [index-catalog index-address]
  (go-try
    (loop [next-idx-root (<! (storage/read-db-root index-catalog index-address))
           garbage       []]
      (if (or (nil? next-idx-root) ;; no more idx-roots
              (util/exception? next-idx-root)) ;; if idx-root already deleted, will be exception
        garbage
        (let [garbage-meta  (garbage-meta-map next-idx-root)
              prev-idx-addr (get-in next-idx-root [:prev-index :address])
              garbage*      (if garbage-meta
                              (conj garbage garbage-meta)
                              garbage)]
          (recur (when prev-idx-addr ;; first index won't have a prev-index
                   (<! (storage/read-db-root index-catalog prev-idx-addr)))
                 garbage*))))))

(defn clean-garbage-record
  "Cleans up a complete garbage file, which will contain
  many index segment garbage items within it. Garbage file
  looks like:
  {:alias 'ledger-alias',
   :branch 'main',
   :garbage ['fluree:file://.../index/segment/1.json', ...]
   :t 1}

   Also deletes sketch files associated with this index by reading the index root
   to determine which properties exist and deleting their sketch files."
  [index-catalog {:keys [address index t] :as _garbage-map}]
  (go-try
    (let [{:keys [alias branch garbage]} (<? (storage/read-garbage index-catalog address))]
      (log/info "Removing" (count garbage) "unused index segments (garbage) from ledger"
                alias "branch" branch "from index-t of" t)

      ;; First delete all index segment files
      (doseq [garbage-item garbage]
        ;; note if the file was already deleted there could be an exception.
        ;; this might happen if the server shutdown in the middle of a garbage
        ;; exceptions are logged downstream, so just swallow exception here
        ;; and keep processing.
        (<! (storage/delete-garbage-item index-catalog garbage-item)))

      ;; Delete sketch files for this index
      ;; Read the index root to get properties with their :last-modified-t, then delete sketch files
      (let [index-root    (try*
                            (<? (storage/read-db-root index-catalog index))
                            (catch* _e
                              (log/warn "Could not read index root for sketch deletion, index may already be deleted:" index)
                              nil))
            properties-map (when index-root
                             (get-in index-root [:stats :properties]))
            ledger-name   (util.ledger/ledger-base-name alias)]
        (when (seq properties-map)
          (let [{:keys [deleted-count total-count]}
                (<? (hll-persist/delete-sketches-by-last-modified index-catalog ledger-name properties-map))]
            (log/info "Deleted" deleted-count "of" total-count "sketch files from index-t" t
                      "for ledger" alias))))

      ;; Delete main garbage record
      (<! (storage/delete-garbage-item index-catalog address))

      ;; Then delete the parent index root
      (<! (storage/delete-garbage-item index-catalog index)))))

(defn remove-cleaned
  "Returns an updated to-clean garbage list with any already deleted garbage removed.

  After garbage has been cleaned at least once, we'll still have a pointer
  to the oldest garbage file in the oldest index root, but that will have already
  been removed. This will not be the case for the first time garbage is cleaned."
  [index-catalog to-clean]
  (go
    (loop [to-clean* to-clean]
      (let [next-garbage (first to-clean*)]
        (if (and next-garbage
                 (nil? (<! (storage/read-garbage index-catalog (:address next-garbage)))))
          (recur (rest to-clean*))
          to-clean*)))))

(defn clean-garbage*
  [index-catalog index-address max-indexes]
  (go
    (if (nat-int? max-indexes)
      (let [all-garbage (<! (trace-idx-roots index-catalog index-address))
            to-clean    (if (util/exception? all-garbage)
                          (log/error all-garbage "Garbage collection error, unable to trace index roots with error:" (ex-message all-garbage))
                          (->> all-garbage ;; garbage will be in order of newest to oldest
                               (drop max-indexes)
                               (sort-by :t))) ;; clean oldest 't' value first
            to-clean*   (<! (remove-cleaned index-catalog to-clean))
            start-time  (util/current-time-millis)]
        (if (empty? to-clean*)
          (log/debug "Clean-garbage called, but no garbage to clean.")
          (do
            (log/info "Starting garbage collection of oldest"
                      (count to-clean*) "indexes.")
            (doseq [next-garbage to-clean*]
              (<! (clean-garbage-record index-catalog next-garbage)))
            (log/info "Finished garbage collection of oldest"
                      (count to-clean*) "indexes in"
                      (- (util/current-time-millis) start-time) "ms.")
            :done)))
      ;; Unexpected setting. In async chan, don't throw.
      (log/error (str "Garbage collection: Setting for max-old-indexes should be >=0, instead received: " max-indexes
                      "Unable to garbage collect.")))))

(defn clean-garbage
  "Cleans up garbage data for old indexes, but retains
  the most recent `max-indexes` indexes.

  Note that any db's held as a variable will rely on the index-root
  from when they were pulled from the ledger via (fluree/db <ledger>).
  If these db vars are held over time, you might want to adjust this
  setting such that old index-roots are not garbage collected during that
  expected timeframe. The frequency of new indexes being created is
  dependent on the frequency and size of updates that is ledger-specific
  against the ledger's 'reindex-min-bytes' setting."
  [{:keys [index-catalog commit] :as _db} max-indexes]
  (let [index-address (-> commit :index :address)]
    (clean-garbage* index-catalog index-address max-indexes)))
