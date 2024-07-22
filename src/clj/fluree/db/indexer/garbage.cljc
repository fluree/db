(ns fluree.db.indexer.garbage
  (:require [clojure.core.async :as async :refer [<! go]]
            [fluree.db.indexer.storage :as storage]
            [fluree.db.util.async #?(:clj :refer :cljs :refer-macros) [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn garbage-meta-map
  "Garbage file metadata that will be passed to the
  garbage cleaning function that allows it to report
  on status and have filename addresses."
  [{:keys [prev-index garbage] :as idx-root}]
  (when garbage ;; first index will not have garbage
    (assoc garbage
      ;; retain not the current index file, but the previous one
      ;; where the garbage in this file originated
      :t (:t prev-index)
      :index (:address prev-index))))

(defn trace-idx-roots
  [conn commit]
  (go-try
   (loop [next-idx-root (<! (storage/read-db-root conn (-> commit :index :address)))
          garbage       []]
     (if (or (nil? next-idx-root) ;; no more idx-roots
             (util/exception? next-idx-root)) ;; if idx-root already deleted, will be exception
       garbage
       (let [garbage-meta  (garbage-meta-map next-idx-root)
             prev-idx-addr (get-in next-idx-root [:prev-index :address])
             garbage*      (if garbage-meta
                             (conj garbage garbage-meta)
                             garbage)]
         (recur (<! (storage/read-db-root conn prev-idx-addr))
                garbage*))))))

(defn clean-garbage-record
  "Cleans up a complete garbage file, which will contain
  many index segment garbage items within it. Garbage file
  looks like:
  {:alias 'ledger-alias',
   :branch 'main',
   :garbage ['fluree:file://.../index/segment/1.json', ...]
   :t 1}"
  [conn {:keys [address index t] :as _garbage-map}]
  (go
    (let [{:keys [alias branch garbage]} (<! (storage/read-garbage conn address))]
      (log/info "Removing" (count garbage) "unused index segments (garbage) from ledger"
                alias "branch" branch "from index-t of" t)

      ;; first delete all index segment files
      (doseq [garbage-item garbage]
        ;; note if the file was already deleted there could be an exception.
        ;; this might happen if the server shutdown in the middle of a garbage
        ;; exceptions are logged downstream, so just swallow exception here
        ;; and keep rocessing.
        (<! (storage/delete-garbage-item conn garbage-item)))

      ;; delete main garbage record
      (<! (storage/delete-garbage-item conn address))

      ;; then delete the parent index root
      (<! (storage/delete-garbage-item conn index)))))

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
  [{:keys [conn commit] :as _db} max-indexes]
  (go
    (if (nat-int? max-indexes)
      (let [all-garbage (<? (trace-idx-roots conn commit))
            to-clean    (->> all-garbage
                             (drop max-indexes)
                             (sort-by :t)) ;; clean oldest 't' value first
            start-time  (util/current-time-millis)]
        (if (empty? to-clean)
          (log/debug "Clean-garbage called, but no garbage to clean.")
          (do
            (log/info "Starting garbage collection of oldest"
                      (count to-clean) "indexes.")
            (doseq [next-garbage to-clean]
              (<! (clean-garbage-record conn next-garbage)))
            (log/info "Finished garbage collection of oldest"
                      (count to-clean) "indexes in"
                      (- (util/current-time-millis) start-time) "ms.")
            :done)))
      ;; Unexpected setting. In async chan, don't throw.
      (ex-info (str "Setting for max-old-indexes should be >=0, instead received: " max-indexes)
               {:status 500 :error :db/unexpected-error}))))
