(ns fluree.db.indexer.garbage
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.flake.index.storage :as storage]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
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
  many garbage items within it (index segments and sketch files). Garbage file
  looks like:
  {:alias 'ledger-alias',
   :branch 'main',
   :garbage ['fluree:file://.../index/segment/1.json',
             'fluree:file://.../stats-sketches/values/ex_email_1.hll', ...]
   :t 1}

   All garbage items (segments and sketches) are deleted by iterating through the list."
  [index-catalog {:keys [address index t] :as _garbage-map}]
  (go-try
    (let [{:keys [alias branch garbage]} (<? (storage/read-garbage index-catalog address))]
      (log/info "Removing" (count garbage) "unused index items (garbage) from ledger"
                alias "branch" branch "from index-t of" t)

      ;; Delete all garbage items (index segments and sketch files)
      (doseq [garbage-item garbage]
        ;; note if the file was already deleted there could be an exception.
        ;; this might happen if the server shutdown in the middle of a garbage
        ;; exceptions are logged downstream, so just swallow exception here
        ;; and keep processing.
        (<! (storage/delete-garbage-item index-catalog garbage-item)))

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
                          (do (log/error! ::garbage-tracing-error all-garbage {:index-address index-address})
                              (log/error all-garbage "Garbage collection error, unable to trace index roots with error:" (ex-message all-garbage)))
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
      (do (log/error! ::garbage-invalid-max-indexes nil
                      {:max-indexes max-indexes
                       :msg (str "Garbage collection: Setting for max-old-indexes should be >=0, instead received: " max-indexes
                                 "Unable to garbage collect.")})
          (log/error (str "Garbage collection: Setting for max-old-indexes should be >=0, instead received: " max-indexes
                          "Unable to garbage collect."))))))

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
  [{:keys [index-catalog commit] :as db} max-indexes]
  (let [index-address (-> commit :index :address)]
    (log/info! ::clean-garbage {:ledger-alias (:alias db) :max-indexes max-indexes})
    (clean-garbage* index-catalog index-address max-indexes)))
