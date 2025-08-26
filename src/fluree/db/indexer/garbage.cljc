(ns fluree.db.indexer.garbage
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.flake.index.storage :as storage]
            [fluree.db.indexer.cuckoo :as cuckoo]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [go-try <?]]
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

(defn check-other-branches
  "Check if garbage items are used by other branches.
  Returns only items that can be safely deleted.
  If other-filters is provided, uses it; otherwise loads them."
  [index-catalog ledger-alias current-branch garbage-items & [other-filters]]
  (go-try
    (let [filters (or other-filters
                      (<? (cuckoo/load-other-branch-filters
                           index-catalog ledger-alias current-branch)))]
      (if (empty? filters)
        ;; No other branches, safe to delete all
        garbage-items
        ;; Check each item against other branch filters
        (remove #(cuckoo/any-branch-uses? filters %)
                garbage-items)))))

(defn clean-garbage-record
  "Cleans up a complete garbage file, which will contain
  many index segment garbage items within it. 
  
  If other-branch-filters is provided, uses it for checking instead of loading.
  If garbage-data is provided, uses it instead of reading from storage."
  [index-catalog {:keys [address index t] :as _garbage-map} & [other-branch-filters garbage-data]]
  (go
    (let [{:keys [alias branch garbage]} (or garbage-data
                                             (<! (storage/read-garbage index-catalog address)))
          ;; Extract ledger name from alias (remove branch part if present)
          [ledger-name _] (util.ledger/ledger-parts alias)
          ;; Normalize branch name - nil means "main"
          branch-name (or branch "main")

          ;; ALWAYS remove ALL garbage from this branch's cuckoo filter
          ;; This ensures we don't block other branches from deleting these segments
          _ (when (and (:storage index-catalog) (seq garbage))
              (let [filter (<! (cuckoo/read-filter index-catalog ledger-name branch-name))]
                (when filter
                  (let [filter' (cuckoo/batch-remove-chain filter garbage)]
                    (<! (cuckoo/write-filter index-catalog ledger-name branch-name t filter'))))))

          ;; Check which items can actually be deleted from disk
          ;; Use provided filters if available, otherwise load them
          can-delete (<! (check-other-branches index-catalog ledger-name branch-name garbage other-branch-filters))
          retained (- (count garbage) (count can-delete))]

      (if (pos? retained)
        (log/info "Checking" (count garbage) "garbage segments from ledger"
                  alias "branch" branch "t" t
                  "- Retained" retained "segments still in use by other branches"
                  "- Deleting" (count can-delete) "segments from disk")
        (log/info "Removing" (count can-delete) "unused index segments (garbage) from ledger"
                  alias "branch" branch "from index-t of" t))

      ;; Delete only segments not used by other branches from disk
      (doseq [garbage-item can-delete]
        ;; note if the file was already deleted there could be an exception.
        ;; this might happen if the server shutdown in the middle of a garbage
        ;; exceptions are logged downstream, so just swallow exception here
        ;; and keep processing.
        (<! (storage/delete-garbage-item index-catalog garbage-item)))

      ;; delete main garbage record (even if some items were retained)
      (<! (storage/delete-garbage-item index-catalog address))

      ;; then delete the parent index root
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
            ;; Cache other-branch filters per ledger to avoid repeated I/O
            (let [ledger-filter-cache (atom {})]
              (doseq [next-garbage to-clean*]
                ;; Read garbage record once
                (let [garbage-data (<! (storage/read-garbage index-catalog (:address next-garbage)))
                      {:keys [alias]} garbage-data
                      [ledger-name _] (util.ledger/ledger-parts alias)
                      ;; Get cached filters or load them once for this ledger
                      other-filters (or (get @ledger-filter-cache ledger-name)
                                        (let [filters (<! (cuckoo/load-other-branch-filters
                                                           index-catalog ledger-name nil))]
                                          (swap! ledger-filter-cache assoc ledger-name filters)
                                          filters))]
                  (<! (clean-garbage-record index-catalog next-garbage other-filters garbage-data)))))
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
