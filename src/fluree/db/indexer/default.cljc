(ns fluree.db.indexer.default
  (:require [fluree.db.indexer.proto :as idx-proto]
            [fluree.db.index :as index]
            [fluree.db.storage.core :as storage]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

;; default indexer

#?(:clj (set! *warn-on-reflection* true))

(def ^:dynamic *overflow-bytes* 500000)
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

(defn novelty-min?
  "Returns true if ledger is beyond novelty-min threshold."
  [reindex-min db]
  (let [novelty-size (get-in db [:novelty :size])]
    (> novelty-size reindex-min)))

(defn novelty-max?
  "Returns true if ledger is beyond novelty-max threshold."
  [reindex-max db]
  (let [novelty-size (get-in db [:novelty :size])]
    (> novelty-size reindex-max)))

(defn remove-watch-event
  "Removes watch event id"
  [state-atom watch-id]
  (swap! state-atom update :watchers dissoc watch-id))

(defn remove-all-watch-events
  "Removes all watch events (useful when closing indexer)"
  [state-atom]
  (swap! state-atom assoc :watchers {}))

(defn add-watch-event
  "Add new watch event"
  [state-atom watch-id callback]
  (when-not watch-id
    (throw (ex-info "Attempt to add index watch fn without a watch-id."
                    {:status 500 :error :db/indexing})))
  (when-not (fn? callback)
    (throw (ex-info (str "Indexer watch event attempting to be added is not a callback fn. Id: " watch-id)
                    {:status 500 :error :db/indexing})))
  (swap! state-atom assoc-in [:watchers watch-id] callback))

(defn send-watch-event
  "Sends index watch event data which is assumed to always be a map."
  [state-atom event-data]
  (when-not (map? event-data)
    (throw (ex-info (str "Index event data not a map as expected. Provided: " event-data)
                    {:status 500 :error :db/indexing})))
  (let [watchers (:watchers @state-atom)]
    (doseq [[watch-id watch-fn] watchers]
      (try*
        (watch-fn (assoc event-data :watch-id watch-id))
        (catch* e
                (log/warn "Closing index watch function due to exception: " (ex-message e))
                (remove-watch-event state-atom watch-id))))))

(defn format-watch-event
  "This ensures consistent formatting of watch events."
  [event-type event-meta]
  (assoc event-meta :event event-type))

(defn close
  "Closing indexer, sends events to all watchers and clears state atom."
  [{:keys [state-atom] :as indexer}]
  (idx-proto/-push-event indexer (format-watch-event :close nil))
  (remove-all-watch-events state-atom))

(defn lock-indexer
  "Generates a new indexing state if indexing wasn't already in-process.

  Returns three-tuple of lock status (true if successful, else false), indexing-id, and
  indexing promise channel that indexing result will be delivered to upon completion."
  [state-atom t]
  (let [id            (util/current-time-millis)
        state*        (swap! state-atom
                             (fn [{:keys [indexing] :as state}]
                               (if (:id indexing)           ;; if id exists, already indexing
                                 state
                                 ;; not indexing, establish
                                 (let [indexing-state {:id     id
                                                       :t      t
                                                       :chan   (async/promise-chan)
                                                       :status {}}]
                                   (assoc state :indexing indexing-state)))))
        ;; newly generated id value will be idential to what is in revised state if lock was successful.
        lock-success? (= id (get-in state* [:indexing :id]))]
    [lock-success? (get-in state* [:indexing :id]) (get-in state* [:indexing :chan])]))

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
               (> t node-t)))                               ; t gets smaller as it moves forward!
       boolean))

(defn update-branch
  [{:keys [network ledger-id comparator], branch-t :t, :as branch} idx t child-nodes]
  (if (some-update-after? branch-t child-nodes)
    (let [children    (apply index/child-map comparator child-nodes)
          size        (->> child-nodes
                           (map :size)
                           (reduce +))
          first-flake (->> children first key)
          rhs         (->> children flake/last val :rhs)
          new-id      (storage/random-branch-id network ledger-id idx)]
      (assoc branch
        :id new-id
        :t t
        :children children
        :size size
        :first first-flake
        :rhs rhs))
    branch))

(defn update-sibling-leftmost
  [[maybe-leftmost & not-leftmost]]
  (into [maybe-leftmost]
        (map (fn [non-left-node]
               (assoc non-left-node
                 :leftmost? false)))
        not-leftmost))

(defn rebalance-children
  [branch idx t child-nodes]
  (let [target-count (/ *overflow-children* 2)]
    (->> child-nodes
         (partition-all target-count)
         (map (fn [kids]
                (update-branch branch idx t kids)))
         update-sibling-leftmost)))

(defn filter-predicates
  [preds & flake-sets]
  (if (seq preds)
    (->> flake-sets
         (apply concat)
         (filter (fn [f]
                   (contains? preds (flake/p f)))))
    []))

(defn rebalance-leaf
  "Splits leaf nodes if the combined size of its flakes is greater than
  `*overflow-bytes*`."
  [{:keys [flakes leftmost? rhs] :as leaf}]
  (if (overflow-leaf? leaf)
    (let [target-size (/ *overflow-bytes* 2)]
      (log/debug "Rebalancing index leaf:"
                 (select-keys leaf [:id :network :ledger-id]))
      (loop [[f & r] flakes
             cur-size  0
             cur-first f
             leaves    []]
        (if (empty? r)
          (let [subrange  (flake/subrange flakes >= cur-first)
                last-leaf (-> leaf
                              (assoc :flakes subrange
                                     :first cur-first
                                     :rhs rhs)
                              (dissoc :id :leftmost?))]
            (conj leaves last-leaf))
          (let [new-size (-> f flake/size-flake (+ cur-size) long)]
            (if (> new-size target-size)
              (let [subrange (flake/subrange flakes >= cur-first < f)
                    new-leaf (-> leaf
                                 (assoc :flakes subrange
                                        :first cur-first
                                        :rhs f
                                        :leftmost? (and (empty? leaves)
                                                        leftmost?))
                                 (dissoc :id))]
                (recur r 0 f (conj leaves new-leaf)))
              (recur r new-size cur-first leaves))))))
    [leaf]))

(defn update-leaf
  [{:keys [network ledger-id] :as leaf} idx t novelty remove-preds]
  (let [new-flakes (index/novelty-subrange leaf t novelty)
        to-remove  (filter-predicates remove-preds (:flakes leaf) new-flakes)]
    (if (or (seq new-flakes) (seq to-remove))
      (let [new-leaves (-> leaf
                           (dissoc :id)
                           (index/add-flakes new-flakes)
                           (index/rem-flakes to-remove)
                           rebalance-leaf)]
        (map (fn [l]
               (assoc l
                 :id (storage/random-leaf-id network ledger-id idx)
                 :t t))
             new-leaves))
      [leaf])))

(defn integrate-novelty
  "Returns a transducer that transforms a stream of index nodes in depth first
  order by incorporating the novelty flakes into the nodes, removing flakes with
  predicates in remove-preds, rebalancing the leaves so that none is bigger than
  *overflow-bytes*, and rebalancing the branches so that none have more children
  than *overflow-children*. Maintains a 'lifo' stack to preserve the depth-first
  order of the transformed stream."
  [idx t novelty remove-preds]
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
           (let [leaves (update-leaf node idx t novelty remove-preds)]
             (vswap! stack into leaves)
             result)

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
                        (xf result* child))
                 (if (overflow-children? child-nodes)
                   (let [new-branches (rebalance-children node idx t child-nodes)
                         result**     (reduce xf result* new-branches)]
                     (recur new-branches
                            stack*
                            result**))
                   (let [branch (update-branch node idx t child-nodes)]
                     (vswap! stack conj branch)
                     result*)))))))

        ;; Completion: Flush the stack iterating each remaining node with the
        ;; nested transformer before calling the nested transformer's completion
        ;; fn on the iterated result.
        ([result]
         (loop [stack*  @stack
                result* result]
           (if-let [node (peek stack*)]
             (recur (vswap! stack pop)
                    (unreduced (xf result* node)))
             (xf result*))))))))

(defn preserve-id
  "Stores the original id of a node under the `::old-id` key if the `node` was
  resolved, leaving unresolved nodes unchanged. Useful for keeping track of the
  original id for modified nodes during the indexing process for garbage
  collection purposes"
  [{:keys [id] :as node}]
  (cond-> node
          (index/resolved? node) (assoc ::old-id id)))

(defn update-branch-ids
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

(defn write-node
  "Writes `node` to storage, and puts any errors onto the `error-ch`"
  [conn idx {:keys [id network ledger-id] :as node} error-ch updated-ids]
  (let [node         (dissoc node ::old-id)
        display-node (select-keys node [:id :network :ledger-id])]
    (async/go
      (try*
        (if (index/leaf? node)
          (do (log/debug "Writing index leaf:" display-node)
              (<? (storage/write-leaf conn network ledger-id idx id node)))
          (do (log/debug "Writing index branch:" display-node)
              (->> node
                   (update-branch-ids updated-ids)
                   (storage/write-branch conn network ledger-id idx id)
                   <?)))
        (catch* e
                (log/error e
                           "Error writing novel index node:" display-node)
                (async/>! error-ch e))))))


(defn write-resolved-nodes
  [conn idx error-ch index-ch]
  (async/go-loop [stats {:idx idx, :novel 0, :unchanged 0, :garbage #{} :updated-ids {}}
                  last-node nil]
    (if-let [{::keys [old-id] :as node} (async/<! index-ch)]
      (if (index/resolved? node)
        (let [written-node (async/<! (write-node conn idx node error-ch (:updated-ids stats)))
              stats*       (cond-> stats
                                   (not= old-id :empty) (update :garbage conj old-id)
                                   true (update :novel inc)
                                   true (assoc-in [:updated-ids (:id node)] (:id written-node)))]
          (recur stats*
                 written-node))
        (recur (update stats :unchanged inc)
               node))
      (assoc stats :root last-node))))


(defn refresh-index
  [conn error-ch {::keys [idx t novelty remove-preds root]}]
  (let [refresh-xf (comp (map preserve-id)
                         (integrate-novelty idx t novelty remove-preds))
        novel?     (fn [node]
                     (or (seq remove-preds)
                         (seq (index/novelty-subrange node t novelty))))]
    (->> (index/tree-chan conn root novel? (constantly true) 1 refresh-xf error-ch)
         (write-resolved-nodes conn idx error-ch))))

(defn extract-root
  [{:keys [novelty t] :as db} remove-preds idx]
  (let [index-root    (get db idx)
        index-novelty (get novelty idx)]
    {::idx          idx
     ::root         index-root
     ::novelty      index-novelty
     ::t            t
     ::remove-preds remove-preds}))


(defn tally
  [db-status {:keys [idx root garbage] :as _tally-data}]
  (-> db-status
      (update :db assoc idx root)
      (update :indexes conj idx)
      (update :garbage into garbage)))


(defn refresh-all
  ([db error-ch]
   (refresh-all db #{} error-ch))
  ([{:keys [conn] :as db} remove-preds error-ch]
   (->> index/types
        (map (partial extract-root db remove-preds))
        (map (partial refresh-index conn error-ch))
        async/merge
        (async/reduce tally {:db db, :indexes [], :garbage #{}}))))

(defn empty-novelty
  [db]
  (let [cleared (reduce (fn [db* idx]
                          (update-in db* [:novelty idx] empty))
                        db index/types)]
    (assoc-in cleared [:novelty :size] 0)))

(defn refresh
  ([indexer db]
   (refresh db {:status "ready"}))
  ([indexer
    {:keys [novelty block t network ledger-id] :as db}
    {:keys [ecount remove-preds]}]
   (go-try
     (let [start-time-ms (util/current-time-millis)
           novelty-size  (:size novelty)
           init-stats    {:network      network
                          :ledger-id    ledger-id
                          :t            t
                          :block        block
                          :novelty-size novelty-size
                          :start-time   (util/current-time-iso)}]
       (if (or (dirty? db)
               (seq remove-preds))
         (do (log/info "Refreshing Index:" init-stats)
             (let [error-ch   (async/chan)
                   refresh-ch (refresh-all db remove-preds error-ch)]
               (async/alt!
                 error-ch
                 ([e]
                  (throw e))

                 refresh-ch
                 ([{:keys [garbage], refreshed-db :db, :as status}]
                  (let [indexed-db  (-> refreshed-db
                                        empty-novelty
                                        (assoc-in [:stats :indexed] block))
                        ;; TODO - ideally issue garbage/root writes to RAFT together
                        ;;        as a tx, currently requires waiting for both
                        ;;        through raft sync
                        db-root-res (<? (storage/write-db-root indexed-db ecount))
                        garbage-res (when (seq garbage)
                                      (<? (storage/write-garbage indexed-db garbage)))]

                    (let [duration  (- (util/current-time-millis) start-time-ms)
                          end-stats (assoc init-stats
                                      :end-time (util/current-time-iso)
                                      :duration duration)]
                      (log/info "Index refresh complete:" end-stats))
                    (log/warn "IDX COMPLETE: " {:root    db-root-res
                                                :garbage garbage-res})
                    (assoc-in indexed-db [:commit :index] {:root    db-root-res
                                                           :garbage garbage-res}))))))
         db)))))

(defn do-index
  "Performs an index operation and returns a promise-channel of the latest db once complete"
  [indexer db remove-preds]
  (let [[lock? idx-id pc] (lock-indexer (:state-atom indexer) (:t db))]
    (when lock?
      ;; when we have a lock, reindex and put updated db onto pc.
      (async/go
        (idx-proto/-push-event indexer (format-watch-event :index-start
                                                           {:t    (:t db)
                                                            :id   idx-id
                                                            :chan pc}))
        (let [indexed-db (<? (refresh indexer db {:remove-preds remove-preds}))]
          (async/put! pc indexed-db)
          (idx-proto/-push-event indexer (format-watch-event :index-finished {:t    (:t db)
                                                                              :id   idx-id
                                                                              :chan pc})))))
    pc))


(defrecord IndexerDefault [reindex-min-bytes reindex-max-bytes state-atom]
  idx-proto/iIndex
  (-index? [_ db] (novelty-min? reindex-min-bytes db))
  (-halt? [_ db] (novelty-max? reindex-max-bytes db))
  (-index [indexer db remove-preds] (do-index indexer db remove-preds))
  (-add-watch [_ watch-id callback] (add-watch-event state-atom watch-id callback))
  (-remove-watch [_ watch-id] (remove-watch-event state-atom watch-id))
  (-push-event [_ event-data] (send-watch-event state-atom event-data))
  (-close [indexer] (close indexer))
  (-status [indexer] :TODO)
  (-status [indexer index-id] :TODO)
  (-reindex [indexer db] :TODO))


(defn new-state-atom
  []
  (atom
    {:watchers {}                                           ;; map of watcher ids to fns
     :indexing {:id     nil                                 ;; running indexing job
                :t      nil
                :chan   nil
                :status nil}
     :indexed  {:id      nil                                ;; last completed indexing job
                :t       nil
                :garbage []
                :chan    nil}
     :queued-t {}                                           ;; map of indexing timestamp-ids to 't' values
     }))



(defn create
  "Creates a new indexer."
  [{:keys [reindex-min-bytes reindex-max-bytes
           state-atom
           idx-file-write idx-file-read idx-file-delete]
    :or   {reindex-min-bytes 100000                         ;; 100 kb
           reindex-max-bytes 1000000                        ;; 1 mb
           state-atom        (new-state-atom)}}]
  (let [options {:reindex-min-bytes reindex-min-bytes
                 :reindex-max-bytes reindex-max-bytes
                 :state-atom        state-atom
                 :idx-file-write    idx-file-write
                 :idx-file-read     idx-file-read
                 :idx-file-delete   idx-file-delete}]
    (map->IndexerDefault options)))


