(ns fluree.db.flake.index.novelty
  (:require [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.cache :as cache]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.index :as index]
            [fluree.db.flake.index.stats :as stats]
            [fluree.db.flake.index.storage :as index-storage]
            [fluree.db.indexer.garbage :as garbage]
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
   (->> (index/indexes-for db)
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
  (let [children    (->> child-nodes
                         (sort-by :first comparator)
                         (index/child-map comparator))
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

(defn merge-with-unchanged-children
  "Merges updated children (from the stack) with unchanged children from the
   original branch.

   Uses ::old-id on updated children to identify which original children have
   been replaced."
  [{:keys [children] :as _branch} updated-children]
  (if (empty? children)
    updated-children
    (let [replaced-ids (into #{} (keep ::old-id) updated-children)]
      (->> children
           vals
           (remove #(contains? replaced-ids (:id %)))
           (into updated-children)))))

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
                last-size (+ cur-size (if f (flake/size-flake f) 0))
                last-leaf (-> leaf
                              (assoc :flakes subrange
                                     :first cur-first
                                     :rhs rhs
                                     :size last-size
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
                                        :size cur-size
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

           (loop [updated-children []
                  stack*           @stack
                  result*          result]
             (let [child (peek stack*)]
               (if (and child
                        (index/descendant? node child))     ; all of a resolved
                                                            ; branch's children
                                                            ; should be at the top
                                                            ; of the stack
                 (recur (conj updated-children child)
                        (vswap! stack pop)
                        result*)
                 (let [all-children (merge-with-unchanged-children node updated-children)]
                   (if (overflow-children? all-children)
                     (let [new-branches (rebalance-children node t all-children)]
                       (vswap! stack push-all-nodes new-branches)
                       (transduce-nodes xf result* new-branches))
                     (let [branch (update-branch node t all-children)]
                       (vswap! stack push-node branch)
                       (xf result* branch)))))))))

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
                 (let [root-node (-> (reconstruct-branch root-template t child-nodes)
                                     (assoc :rhs nil :leftmost? true))]
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

(defn- update-property-count
  "Update count for a single property from novelty flakes.
   Does NOT update NDV - keeps indexed NDV values as-is.
   This is used for ledger-info to provide fast, accurate counts without loading sketches."
  [property-flakes prev-prop-data]
  (let [delta (reduce (fn [acc f]
                        (if (flake/op f)
                          (inc acc)
                          (dec acc)))
                      0
                      property-flakes)
        new-count (max 0 (+ (:count prev-prop-data 0) delta))]
    (assoc prev-prop-data :count new-count)))

(defn- compute-counts-from-novelty
  "Update property and class counts from novelty, keeping NDV/selectivity from indexed stats.
   Also updates class property details (types, ref-classes, langs) from novelty.
   This is a fast, synchronous computation for ledger-info.
   NDV values require loading sketches from disk (expensive), so we keep indexed NDV as-is.

   Returns {:properties {sid -> {:count n :ndv-values n :ndv-subjects n :selectivity-* n}}
            :classes {sid -> {:count n :properties {prop -> {:types {...} :ref-classes {...} :langs {...}}}}}}"
  [novelty-flakes prev-properties prev-classes]
  (let [property-groups (partition-by flake/p novelty-flakes)
        ;; First pass: update property counts and class counts
        updated-counts
        (reduce
         (fn [acc property-flakes]
           (let [p (flake/p (first property-flakes))
                 prev-prop-data (get prev-properties p {:count 0})

                 property-data (update-property-count property-flakes prev-prop-data)

                 classes* (if (flake/class-flake? (first property-flakes))
                            (stats/update-class-counts property-flakes (:classes acc))
                            (:classes acc))]

             {:properties (assoc (:properties acc) p property-data)
              :classes classes*}))
         {:properties prev-properties
          :classes prev-classes}
         property-groups)

        ;; Second pass: update class property details (types, ref-classes, langs)
        updated-class-props (stats/compute-class-property-stats-from-novelty
                             novelty-flakes
                             (:classes updated-counts))]

    {:properties (:properties updated-counts)
     :classes updated-class-props}))

(defn current-stats
  "Compute current property and class statistics for ledger-info.

   Updates counts by replaying novelty (fast, exact).
   Keeps NDV and selectivity from last index (approximations, but avoids loading sketches from disk)."
  [db]
  (let [indexed-stats     (get db :stats {})
        indexed-properties (get indexed-stats :properties {})
        indexed-classes    (get indexed-stats :classes {})
        post-novelty       (get-in db [:novelty :post])]
    (if (not-empty post-novelty)
      ;; Update counts from novelty, keep indexed NDV/selectivity
      (let [novelty-updates (compute-counts-from-novelty post-novelty indexed-properties indexed-classes)]
        (assoc indexed-stats
               :properties (:properties novelty-updates)
               :classes    (:classes novelty-updates)))
      ;; No novelty, return indexed stats as-is
      indexed-stats)))

(defn cached-current-stats
  "Returns current-stats using connection's LRU cache.

   Cache key: [::ledger-stats ledger-alias t]
   This ensures stats are computed once per ledger state and shared across:
   - ledger-info API calls
   - f:onClass policy optimization

   Returns a channel containing the stats."
  [db]
  (let [lru-cache (-> db :index-catalog :cache)
        cache-key [::ledger-stats (:alias db) (:t db)]]
    (cache/lru-lookup
     lru-cache
     cache-key
     (fn [_]
       (async/go
         (log/debug "Computing class->property stats"
                    {:ledger (:alias db) :t (:t db)})
         (current-stats db))))))

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
      (assoc stats :root (-> last-node
                             (assoc :rhs nil :leftmost? true)
                             index/unresolve)))))

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

(defn compute-stats-async
  "Computes HLL-based statistics asynchronously with property-by-property processing.
   Each property is: loaded from disk → updated → written back to disk immediately.

   Returns a channel with {:properties {...} :classes {...} :old-sketch-paths #{...}}
   NO sketches in return value - they're written to disk during processing.

   If stats computation fails, returns previous stats with empty old-sketch-paths."
  [db]
  (go-try
    (let [post-novelty (get-in db [:novelty :post])]
      (if (empty? post-novelty)
        {:properties (get-in db [:stats :properties] {})
         :classes (get-in db [:stats :classes] {})
         :old-sketch-paths #{}}

        (let [{:keys [index-catalog alias t]} db
              ledger-name (util.ledger/ledger-base-name alias)
              prev-properties (get-in db [:stats :properties] {})
              prev-classes (get-in db [:stats :classes] {})]
          (<? (stats/compute-stats-with-writes index-catalog ledger-name t post-novelty
                                               prev-properties prev-classes)))))))

(defn refresh-all
  ([db error-ch]
   (refresh-all db nil error-ch))
  ([db changes-ch error-ch]
   (go-try
     ;; Check if this is a v1 index - if so, skip stats computation entirely
     ;; v1 indexes should not generate stats until fully reindexed
     (let [start-ms            (util/current-time-millis)
           ledger-alias        (:alias db)
           t                   (:t db)
           prev-index-version  (get-in db [:commit :index :v])
           is-v1-index?        (and prev-index-version (< prev-index-version 2))
           track-class-stats?  (get db :track-class-stats true)

           ;; Kick off stats computation in parallel (v2 only)
           stats-ch (when-not is-v1-index?
                      (compute-stats-async db))

           ;; Kick off class property tracking in parallel (v2 only, if enabled)
           class-props-ch (when (and (not is-v1-index?)
                                     track-class-stats?)
                            (stats/compute-class-property-stats-async db))

           ;; Run index refresh (always required)
           index-result (<? (->> (index/indexes-for db)
                                 (map (partial extract-root db))
                                 (map (partial refresh-index db changes-ch error-ch))
                                 async/merge
                                 (async/reduce tally {:db      db
                                                      :indexes []
                                                      :garbage #{}})))

           index-done-ms (util/current-time-millis)
           _ (log/info "refresh-all PHASE: index-refresh complete"
                       {:ledger ledger-alias :t t :elapsed-ms (- index-done-ms start-ms)})

           ;; Collect stats results (or use empty stats for v1)
           stats-result (if is-v1-index?
                          (do
                            (log/info "Skipping statistics computation for v1 index (will be enabled after full reindex)")
                            {:properties {}
                             :classes {}
                             :old-sketch-paths #{}})
                          (<? stats-ch))

           hll-done-ms (util/current-time-millis)
           _ (log/info "refresh-all PHASE: HLL-stats complete"
                       {:ledger ledger-alias :t t
                        :elapsed-ms (- hll-done-ms start-ms)
                        :wait-after-index-ms (- hll-done-ms index-done-ms)
                        :property-count (count (:properties stats-result))})

           class-props-result (if class-props-ch
                                (<? class-props-ch)
                                {})

           class-done-ms (util/current-time-millis)
           _ (when track-class-stats?
               (log/info "refresh-all PHASE: class-properties complete"
                         {:ledger ledger-alias :t t
                          :elapsed-ms (- class-done-ms start-ms)
                          :wait-after-hll-ms (- class-done-ms hll-done-ms)
                          :class-count (count class-props-result)}))

           merged-classes (merge-with merge (:classes stats-result) class-props-result)]

       (merge index-result (assoc stats-result :classes merged-classes))))))

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
          ([{:keys [garbage properties old-sketch-paths classes], refreshed-db :db, :as _status}]
           (let [;; Add computed fields to properties for O(1) optimizer lookups
                 properties-with-computed (add-computed-fields properties)

                 {:keys [index-catalog alias] :as refreshed-db*}
                 (-> refreshed-db
                     (assoc-in [:stats :indexed] t)
                     (assoc-in [:stats :properties] properties-with-computed)
                     (assoc-in [:stats :classes] classes))

                 garbage-with-sketches (into garbage (or old-sketch-paths #{}))

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

                 prev-idx-v    (get-in refreshed-db* [:commit :index :v])
                 index-version (if (get-in refreshed-db* [:commit :index :data :t])
                                 (or prev-idx-v 1)
                                 2)

                 commit-index  (commit-data/new-index (-> refreshed-db* :commit :data)
                                                      index-id
                                                      index-address
                                                      index-version
                                                      (index/select-roots refreshed-db*))
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
