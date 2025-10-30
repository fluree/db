(ns fluree.db.flake.index.novelty
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.hyperloglog :as hll-persist]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.indexer.garbage :as garbage]
            [fluree.db.indexer.hll :as hll]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(def ^:dynamic *overflow-bytes* const/default-overflow-bytes)
(defn overflow-leaf?
  [{:keys [flakes]}]
  (> (flake/size-bytes flakes) *overflow-bytes*))

(def ^:dynamic *underflow-bytes* 50000)
(defn underflow-leaf?
  [{:keys [flakes]}]
  (< (flake/size-bytes flakes) *underflow-bytes*))

(def ^:dynamic *overflow-children* 500)
(defn overflow-children?
  [children]
  (> (count children) *overflow-children*))

(defn min-novelty?
  "Returns true if ledger is beyond novelty-min threshold."
  [db]
  (let [novelty-size (get-in db [:novelty :size])
        min-novelty  (:reindex-min-bytes db)]
    (> novelty-size min-novelty)))

(defn max-novelty?
  "Returns true if ledger is beyond novelty-max threshold."
  [db]
  (let [novelty-size (get-in db [:novelty :size])
        max-novelty  (:reindex-max-bytes db)]
    (> novelty-size max-novelty)))

(defn dirty?
  "Returns `true` if the index for `db` of type `idx` is out of date, or if `db`
  has any out of date index if `idx` is unspecified. Returns `false` otherwise."
  ([db idx]
   (-> db
       :novelty
       (get idx)
       seq
       boolean))
  ([db]
   (->> index/types
        (some (partial dirty? db))
        boolean)))

(defn some-update-after?
  [t nodes]
  (->> nodes
       (map :t)
       (some (fn [node-t]
               (< t node-t)))
       boolean))

(defn reconstruct-branch
  [{:keys [comparator], :as branch} t child-nodes]
  (let [children    (index/child-map comparator child-nodes)
        size        (->> child-nodes
                         (map :size)
                         (reduce +))
        leftmost?   (->> children first val :leftmost? true?)
        first-flake (->> children first key)
        rhs         (->> children flake/last val :rhs)
        new-id      (random-uuid)]
    (assoc branch
           :id new-id
           :t t
           :children children
           :size size
           :leftmost? leftmost?
           :first first-flake
           :rhs rhs)))

(defn update-branch
  [{branch-t :t, :as branch} t child-nodes]
  (if (some-update-after? branch-t child-nodes)
    (reconstruct-branch branch t child-nodes)
    branch))

(defn update-sibling-leftmost
  [[maybe-leftmost & not-leftmost]]
  (into [maybe-leftmost]
        (map (fn [non-left-node]
               (assoc non-left-node
                      :leftmost? false)))
        not-leftmost))

(defn rebalance-children
  [branch t child-nodes]
  (let [target-count (/ *overflow-children* 2)]
    (->> child-nodes
         (partition-all target-count)
         (map (fn [kids]
                (reconstruct-branch branch t kids)))
         update-sibling-leftmost)))

(defn rebalance-leaf
  "Splits leaf nodes if the combined size of its flakes is greater than
  `*overflow-bytes*`."
  [{:keys [flakes leftmost? rhs] :as leaf}]
  (if (overflow-leaf? leaf)
    (let [target-size (/ *overflow-bytes* 2)
          [fflake & remaining] flakes]
      (log/debug "Rebalancing index leaf:"
                 (select-keys leaf [:id :ledger-alias]))
      (loop [[f & r]   remaining
             cur-size  (flake/size-flake fflake)
             cur-first fflake
             leaves    []]
        (if (empty? r)
          (let [subrange  (flake/subrange flakes >= cur-first)
                last-leaf (-> leaf
                              (assoc :flakes subrange
                                     :first cur-first
                                     :rhs rhs
                                     :leftmost? (and (empty? leaves)
                                                     leftmost?))
                              (dissoc :id))]
            (conj leaves last-leaf))
          (let [flake-size (flake/size-flake f)
                new-size   (+ cur-size flake-size)]
            (if (> new-size target-size)
              (let [subrange (flake/subrange flakes >= cur-first < f)
                    new-leaf (-> leaf
                                 (assoc :flakes subrange
                                        :first cur-first
                                        :rhs f
                                        :leftmost? (and (empty? leaves)
                                                        leftmost?))
                                 (dissoc :id))]
                (recur r flake-size f (conj leaves new-leaf)))
              (recur r new-size cur-first leaves))))))
    [leaf]))

(defn update-leaf
  [leaf t novelty]
  (if-let [new-flakes (-> leaf
                          (index/novelty-subrange t t novelty)
                          not-empty)]
    (let [new-leaves (-> leaf
                         (dissoc :id)
                         (index/add-flakes new-flakes)
                         rebalance-leaf)]
      (map (fn [l]
             (assoc l
                    :id (random-uuid)
                    :t t))
           new-leaves))
    [leaf]))

(defn push-node
  [stack node]
  (conj stack (index/unresolve node)))

(defn push-all-nodes
  [stack nodes]
  (into stack (map index/unresolve) nodes))

(defn transduce-nodes
  [xf result nodes]
  (reduce (fn [res node]
            (xf res node))
          result nodes))

(defn integrate-novelty
  "Returns a transducer that transforms a stream of index nodes in depth first
  order by incorporating the novelty flakes into the nodes, rebalancing the
  leaves so that none is bigger than *overflow-bytes*, and rebalancing the
  branches so that none have more children than *overflow-children*. Maintains a
  'lifo' stack to preserve the depth-first order of the transformed stream."
  [t novelty]
  (fn [xf]
    (let [stack (volatile! [])]
      (fn
        ;; Initialization: do nothing but initialize the nested transformer by
        ;; calling its initializing fn.
        ([]
         (xf))

        ;; Iteration:
        ;;   1. Incorporate each successive node with its corresponding novelty
        ;;      flakes.
        ;;   2. Rebalance both leaves and branches if they become too large after
        ;;      adding novelty by splitting them.
        ;;   3. Iterate each resulting node with the nested transformer.
        ([result node]
         (if (index/leaf? node)
           (let [leaves (update-leaf node t novelty)]
             (vswap! stack push-all-nodes leaves)
             (transduce-nodes xf result leaves))

           (loop [child-nodes []
                  stack*      @stack
                  result*     result]
             (let [child (peek stack*)]
               (if (and child
                        (index/descendant? node child))     ; all of a resolved
                                                            ; branch's children
                                                            ; should be at the top
                                                            ; of the stack
                 (recur (conj child-nodes child)
                        (vswap! stack pop)
                        result*)
                 (if (overflow-children? child-nodes)
                   (let [new-branches (rebalance-children node t child-nodes)]
                     (vswap! stack push-all-nodes new-branches)
                     (transduce-nodes xf result* new-branches))
                   (let [branch (update-branch node t child-nodes)]
                     (vswap! stack push-node branch)
                     (xf result* branch))))))))

        ;; Completion: If there is only one node left in the stack, then it's
        ;; the root and we're done, so we call the nested transformer's
        ;; completion arity.
        ;;
        ;; If there is more than one node left in the stack, then the root was
        ;; split because it overflowed. We first make a new root that is the
        ;; parent of the nodes resulting from the split, then we check if that
        ;; new root overflows.
        ;;
        ;; If the new root does overflow, we iterate all of the newly split
        ;; nodes with the nested transformer and repeat the process. If the new
        ;; root does not overflow, we iterate the new root before calling the
        ;; nested transformer's completion arity.
        ([result]
         (let [remaining-nodes @stack]
           (vreset! stack [])
           (if (or (empty? remaining-nodes)
                   (= (count remaining-nodes) 1))
             (xf result)
             (loop [child-nodes   remaining-nodes
                    root-template (peek remaining-nodes)
                    result*       result]
               (if (overflow-children? child-nodes)
                 (let [new-branches (rebalance-children root-template t child-nodes)
                       child-nodes* (map index/unresolve new-branches)
                       result**     (transduce-nodes xf result* new-branches)]
                   (recur child-nodes*
                          (first child-nodes*)
                          result**))
                 (let [root-node (reconstruct-branch root-template t child-nodes)]
                   (-> result
                       (xf root-node)
                       xf)))))))))))

(defn preserve-id
  "Stores the original id of a node under the `::old-id` key if the `node` was
  resolved, leaving unresolved nodes unchanged. Useful for keeping track of the
  original id for modified nodes during the indexing process for garbage
  collection"
  [{:keys [id] :as node}]
  (cond-> node
    (index/resolved? node) (assoc ::old-id id)))

(defn update-child-ids
  "When using IPFS, we don't know what the leaf id will be until written, therefore
  branches need to get updated with final leaf ids.

  Takes original node, and map of temp left ids to final leaf ids for updating children."
  [temp->final-ids {:keys [children] :as branch-node}]
  (let [children* (reduce-kv
                   (fn [acc k v]
                     (if-let [updated-id (get temp->final-ids (:id v))]
                       (assoc acc k (assoc v :id updated-id))
                       acc))
                   children children)]
    (assoc branch-node :children children*)))

(defn update-node-id
  [node write-response]
  (assoc node :id (:address write-response)))

(defn notify-new-index-file
  "Sends new file update into the changes notification async channel
  if it exists. This is used to synchronize files across consensus, otherwise
  a changes-ch won't be present and this won't be used."
  [write-response file-type t changes-ch]
  (go
    (when changes-ch
      (let [event {:event     :new-index-file
                   :file-type file-type
                   :data      write-response
                   :address   (:address write-response)
                   :t         t}]
        (log/debug "Broadcasting new index file:" file-type "address:" (:address write-response))
        (>! changes-ch event)
        (log/debug "Broadcast success:" file-type "address:" (:address event))
        true))))

(defn write-node
  "Writes `node` to storage, and puts any errors onto the `error-ch`"
  [{:keys [index-catalog alias] :as _db} idx node updated-ids changes-ch error-ch]
  (go
    (let [node         (dissoc node ::old-id)
          t            (:t node)
          display-node (select-keys node [:id :ledger-alias])]
      (try*
        (if (index/leaf? node)
          (do (log/debug "Writing index leaf:" display-node)
              (let [write-response (<? (index-storage/write-leaf index-catalog alias idx node))]
                (<! (notify-new-index-file write-response :leaf t changes-ch))
                (update-node-id node write-response)))

          (do (log/debug "Writing index branch:" display-node)
              (let [node*          (update-child-ids updated-ids node)
                    write-response (<? (index-storage/write-branch index-catalog alias idx node*))]
                (<! (notify-new-index-file write-response :branch t changes-ch))
                (update-node-id node* write-response))))

        (catch* e
          (log/error e
                     "Error writing novel index node:" display-node)
          (async/>! error-ch e))))))

(defn- compute-stats-from-novelty
  "Core logic for computing property and class counts from novelty flakes.
   Increments for assertions (op=true), decrements for retracts (op=false).

   Maintains HLL sketches for NDV (Number of Distinct Values) tracking:
   - NDV(values|p): distinct object values per property (monotone, assertion-only)
   - NDV(subjects|p): distinct subjects per property (monotone, assertion-only)

   Uses transients for performance during the loop, converting back to persistent maps at end.

   Returns {:properties {sid -> {:count n :ndv-values n :ndv-subjects n}}
            :classes {sid -> {:count n}}
            :sketches {sid -> {:values sketch :subjects sketch}}}"
  [novelty-flakes prev-properties prev-classes prev-sketches]
  ;; Convert to transients for efficient updates during loop
  (loop [[f & r] novelty-flakes
         properties (transient prev-properties)
         classes (transient prev-classes)
         sketches (transient prev-sketches)]
    (if f
      (let [s     (flake/s f)
            p     (flake/p f)
            o     (flake/o f)
            delta (if (flake/op f) 1 -1)

            ;; Get current property map (or default), update count, clamp to non-negative
            prop-map    (get properties p {:count 0})
            new-count   (max 0 (+ (:count prop-map 0) delta))
            properties* (assoc! properties p (assoc prop-map :count new-count))

            ;; Update HLL sketches (only on assertions - monotone NDV)
            sketches* (if (flake/op f)
                        (let [prop-sketch (get sketches p {:values (hll/create-sketch)
                                                           :subjects (hll/create-sketch)})
                              values-sketch (hll/add-value (:values prop-sketch) o)
                              subjects-sketch (hll/add-value (:subjects prop-sketch) s)]
                          (assoc! sketches p {:values values-sketch
                                              :subjects subjects-sketch}))
                        sketches)

            ;; Update class counts (clamp to non-negative)
            classes* (if (flake/class-flake? f)
                       (let [class-sid   o
                             class-map   (get classes class-sid {:count 0})
                             new-count   (max 0 (+ (:count class-map 0) delta))]
                         (assoc! classes class-sid (assoc class-map :count new-count)))
                       classes)]
        (recur r properties* classes* sketches*))

      ;; Convert back to persistent maps and extract NDV integers
      (let [properties-persistent (persistent! properties)
            classes-persistent    (persistent! classes)
            sketches-persistent   (persistent! sketches)
            properties-with-ndv
            (reduce-kv (fn [props sid sketch-data]
                         (update props sid assoc
                                 :ndv-values (hll/cardinality (:values sketch-data))
                                 :ndv-subjects (hll/cardinality (:subjects sketch-data))))
                       properties-persistent
                       sketches-persistent)]
        {:properties properties-with-ndv
         :classes classes-persistent
         :sketches sketches-persistent}))))

(defn compute-novelty-stats
  "Computes property and class counts from novelty flakes in a separate thread/go block.
   Returns a channel that will contain the computed statistics.

   Uses async/thread on JVM for true parallelism (not limited by go block thread pool).
   Falls back to go block on ClojureScript."
  [novelty-flakes prev-properties prev-classes prev-sketches]
  #?(:clj
     (async/thread
       (compute-stats-from-novelty novelty-flakes prev-properties prev-classes prev-sketches))
     :cljs
     (go
       (compute-stats-from-novelty novelty-flakes prev-properties prev-classes prev-sketches))))

(defn add-computed-fields
  "Add computed selectivity estimates to properties map for O(1) optimizer lookups.

   Computes and rounds selectivity estimates to integers (clamped to at least 1):
   - :selectivity-value = max(1, ceil(count / ndv-values)) - estimates results for (?s p o) patterns
   - :selectivity-subject = max(1, ceil(count / ndv-subjects)) - estimates results for (s p ?o) patterns

   Computing ceil and max here (once during indexing) avoids repeating the calculation
   on every query optimization."
  [properties]
  (reduce-kv
   (fn [props sid prop-data]
     (let [count        (or (:count prop-data) 0)
           ndv-values   (or (:ndv-values prop-data) 0)
           ndv-subjects (or (:ndv-subjects prop-data) 0)
           ;; Compute selectivity, ceil, and clamp to min 1 - all in one place
           sel-value    (if (pos? ndv-values)
                          (max 1 (long (Math/ceil (/ (double count) (double ndv-values)))))
                          count)
           sel-subject  (if (pos? ndv-subjects)
                          (max 1 (long (Math/ceil (/ (double count) (double ndv-subjects)))))
                          count)]
       (assoc props sid
              (assoc prop-data
                     :selectivity-value sel-value
                     :selectivity-subject sel-subject))))
   {}
   properties))

(defn current-stats
  "Compute current property and class statistics."
  [db]
  (let [indexed-stats     (get db :stats {})
        indexed-properties (get indexed-stats :properties {})
        indexed-classes    (get indexed-stats :classes {})
        indexed-sketches   (get indexed-stats :sketches {})
        spot-novelty       (get-in db [:novelty :spot])]
    (if (not-empty spot-novelty)
      ;; Synchronous computation for both FlakeDB and AsyncDB
      (let [novelty-updates (compute-stats-from-novelty spot-novelty indexed-properties indexed-classes indexed-sketches)
            properties      (add-computed-fields (:properties novelty-updates))]
        (assoc indexed-stats
               :properties properties
               :classes    (:classes novelty-updates)
               :sketches   (:sketches novelty-updates)))
      ;; No novelty, return indexed stats as-is
      indexed-stats)))

(defn write-resolved-nodes
  [db idx changes-ch error-ch index-ch]
  (go-loop [stats     {:idx idx, :novel 0, :unchanged 0, :garbage #{}, :updated-ids {}}
            last-node nil]
    (if-let [{::keys [old-id] :as node} (<! index-ch)]
      (if (index/resolved? node)
        (let [updated-ids  (:updated-ids stats)
              written-node (<! (write-node db idx node updated-ids changes-ch error-ch))
              stats*  (-> stats
                          (update :novel inc)
                          (assoc-in [:updated-ids (:id node)] (:id written-node))
                          (cond-> (not= old-id :empty) (update :garbage conj old-id)))]
          (recur stats*
                 written-node))
        (recur (update stats :unchanged inc)
               node))
      (assoc stats :root (index/unresolve last-node)))))

(defn refresh-index
  [{:keys [index-catalog] :as db} changes-ch error-ch {::keys [idx t novelty root]}]
  (let [refresh-xf (comp (map preserve-id)
                         (integrate-novelty t novelty))
        novel?     (fn [node]
                     (seq (index/novelty-subrange node t t novelty)))]
    (->> (index/tree-chan index-catalog root novel? 1 refresh-xf error-ch)
         (write-resolved-nodes db idx changes-ch error-ch))))

(defn extract-root
  [{:keys [novelty t] :as db} idx]
  (let [index-root    (get db idx)
        index-novelty (get novelty idx)]
    {::idx          idx
     ::root         index-root
     ::novelty      index-novelty
     ::t            t}))

(defn tally
  [db-status {:keys [idx root garbage] :as _tally-data}]
  (-> db-status
      (update :db assoc idx root)
      (update :indexes conj idx)
      (update :garbage into garbage)))

(defn refresh-all
  ([db error-ch]
   (refresh-all db nil error-ch))
  ([db changes-ch error-ch]
   (go-try
     ;; First, load previous sketches from disk before computing stats
     (let [spot-novelty      (get-in db [:novelty :spot])
           prev-properties-mem (get-in db [:stats :properties] {})
           prev-classes        (get-in db [:stats :classes] {})
           prev-sketches-mem   (get-in db [:stats :sketches] {})

           ;; Extract property SIDs from novelty to track which were modified
           novelty-property-sids (into #{} (map flake/p) spot-novelty)

           ;; Load previous index root to get properties with :last-modified-t
           prev-indexed-t       (get-in db [:stats :indexed])
           index-catalog        (:index-catalog db)
           ledger-name          (util.ledger/ledger-base-name (:alias db))

           ;; Load the previous index root (if it exists) to get properties with :last-modified-t
           ;; The db being indexed may already have a previous index loaded
           prev-index-from-db   (get-in db [:index])
           prev-index-address   (:address prev-index-from-db)
           _ (log/info "Loading previous index root from address:" prev-index-address)
           prev-index-root      (when prev-index-address
                                  (try*
                                    (<? (index-storage/read-db-root index-catalog prev-index-address))
                                    (catch* e
                                      (log/debug "Could not load previous index root for :last-modified-t values:" (ex-message e))
                                      nil)))
           prev-properties      (if prev-index-root
                                  ;; Use properties from previous index root (has :last-modified-t)
                                  (do
                                    (log/info "Loaded previous index root, properties count:" (count (get-in prev-index-root [:stats :properties] {})))
                                    (get-in prev-index-root [:stats :properties] {}))
                                  ;; Fallback to in-memory properties (may not have :last-modified-t)
                                  (do
                                    (log/info "No previous index root found, using in-memory properties")
                                    prev-properties-mem))

           loaded-sketches      (<? (hll-persist/load-sketches-by-last-modified index-catalog ledger-name prev-properties prev-indexed-t))

           ;; Merge loaded sketches with in-memory sketches (in-memory takes precedence)
           prev-sketches        (merge loaded-sketches prev-sketches-mem)

           ;; Now kick off parallel stats computation with merged sketches
           stats-ch             (compute-novelty-stats spot-novelty prev-properties prev-classes prev-sketches)
           stats-timeout-ms     (or (get-in db [:stats-compute-timeout-ms]) 5000)
           default-stats        {:properties prev-properties
                                 :classes prev-classes
                                 :sketches prev-sketches}

           ;; Wait for index to complete
           index-result (<? (->> index/types
                                 (map (partial extract-root db))
                                 (map (partial refresh-index db changes-ch error-ch))
                                 async/merge
                                 (async/reduce tally {:db      db
                                                      :indexes []
                                                      :garbage #{}})))

           ;; Wait for stats with timeout fallback
           [stats-result winner-ch] (async/alts! [stats-ch (async/timeout stats-timeout-ms)])

           {:keys [properties classes sketches]}
           (if (= winner-ch stats-ch)
             ;; Stats completed successfully (or nil if errored)
             (or stats-result default-stats)
             ;; Timeout - use previous stats
             default-stats)]

         ;; Log warning if we fell back to previous stats
       (when (not= winner-ch stats-ch)
         (log/warn "Stats computation timeout, using previous stats"
                   {:timeout-ms stats-timeout-ms
                    :novelty-size (get-in db [:novelty :size])
                    :ledger-alias (:alias db)
                    :t (:t db)}))

       (when (and (= winner-ch stats-ch) (nil? stats-result))
         (log/warn "Stats computation failed (channel closed), using previous stats"
                   {:novelty-size (get-in db [:novelty :size])
                    :ledger-alias (:alias db)
                    :t (:t db)}))

         ;; Update :last-modified-t for properties and collect old t-values for garbage
       (let [current-t (:t db)
             old-sketch-t-map (atom {}) ;; Map of {sid -> old-t} for properties being updated
             properties-with-last-t
             (reduce-kv (fn [props sid prop-data]
                          (let [was-in-novelty (contains? novelty-property-sids sid)
                                existing-last-t (:last-modified-t prop-data)
                                prev-prop-data (get prev-properties sid)
                                prev-last-t (:last-modified-t prev-prop-data)
                                  ;; Determine the correct last-modified-t:
                                  ;; - If in novelty: current-t
                                  ;; - If has last-t already: keep it
                                  ;; - If no last-t (migration): use prev-indexed-t
                                new-last-t (cond
                                             was-in-novelty current-t
                                             existing-last-t existing-last-t
                                             prev-indexed-t prev-indexed-t
                                             :else current-t)] ; fallback for brand new properties
                              ;; If property is being modified and had a previous t-value, record it for garbage
                            (when (and was-in-novelty prev-last-t (not= prev-last-t current-t))
                              (log/debug "Recording old sketch for" sid "from t=" prev-last-t "to t=" current-t)
                              (swap! old-sketch-t-map assoc sid prev-last-t))
                            (assoc props sid
                                   (assoc prop-data :last-modified-t new-last-t))))
                        {}
                        properties)]
         (-> index-result
             (assoc :properties properties-with-last-t)
             (assoc :old-sketch-t-map @old-sketch-t-map)  ;; Pass map of old t-values for garbage
             (assoc :classes classes)
             (assoc :sketches sketches)
             (assoc :novelty-property-sids novelty-property-sids)))))))  ;; Pass along for write-sketches

(defn refresh
  [{:keys [novelty t alias] :as db} changes-ch max-old-indexes]
  (go-try
    (if (dirty? db)
      (let [start-time-ms (util/current-time-millis)
            novelty-size  (:size novelty)
            init-stats    {:ledger-alias alias
                           :t            t
                           :novelty-size novelty-size
                           :start-time   (util/current-time-iso)}
            error-ch   (async/chan)
            refresh-ch (refresh-all db changes-ch error-ch)]
        (log/info "Refreshing Index:" init-stats)
        (async/alt!
          error-ch
          ([e]
           (throw e))

          refresh-ch
          ([{:keys [garbage properties old-sketch-t-map classes sketches], refreshed-db :db, :as _status}]
           (let [;; Add computed fields to properties for O(1) optimizer lookups
                 properties-with-computed (add-computed-fields properties)

                 {:keys [index-catalog alias] :as refreshed-db*}
                 (-> refreshed-db
                     (assoc-in [:stats :indexed] t)
                     (assoc-in [:stats :properties] properties-with-computed)
                     (assoc-in [:stats :classes] classes)
                     (assoc-in [:stats :sketches] sketches))

                 ;; Write statistics sketches to fixed paths and collect old paths for garbage
                 old-sketch-paths (when (seq sketches)
                                    (<? (hll-persist/write-sketches index-catalog alias t
                                                                    old-sketch-t-map properties sketches)))

                 ;; Add old sketch paths to garbage collection
                 garbage-with-sketches (into garbage old-sketch-paths)

                ;; TODO - ideally issue garbage/root writes to RAFT together
                ;;        as a tx, currently requires waiting for both
                ;;        through raft sync
                 garbage-res   (when (seq garbage-with-sketches)
                                 (let [write-res (<? (index-storage/write-garbage index-catalog alias t garbage-with-sketches))]
                                   (<! (notify-new-index-file write-res :garbage t changes-ch))
                                   write-res))

                 ;; No need to update db with sketches pointer - using fixed filenames
                 refreshed-db**  refreshed-db*

                 db-root-res   (<? (index-storage/write-db-root index-catalog refreshed-db** (:address garbage-res)))
                 _             (<! (notify-new-index-file db-root-res :root t changes-ch))

                 index-address (:address db-root-res)
                 index-id      (str "fluree:index:sha256:" (:hash db-root-res))
                 commit-index  (commit-data/new-index (-> refreshed-db* :commit :data)
                                                      index-id
                                                      index-address
                                                      (select-keys refreshed-db* index/types))
                 indexed-db    (dbproto/-index-update refreshed-db* commit-index)
                 duration      (- (util/current-time-millis) start-time-ms)
                 end-stats     (assoc init-stats
                                      :end-time (util/current-time-iso)
                                      :duration duration
                                      :address (:address db-root-res)
                                      :garbage (:address garbage-res))]
             (log/info "Index refresh complete:" end-stats)
            ;; kick off automatic garbage collection in the background
             (garbage/clean-garbage indexed-db max-old-indexes)

             indexed-db))))
      db)))
