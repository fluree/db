(ns fluree.db.indexer.default
  (:require [fluree.db.index :as index]
            [fluree.db.indexer.storage :as storage]
            [fluree.db.indexer.garbage :as garbage]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.commit-data :as commit-data]))

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
  [db reindex-min-bytes]
  (let [novelty-size (get-in db [:novelty :size])]
    (> novelty-size reindex-min-bytes)))

(defn novelty-max?
  "Returns true if ledger is beyond novelty-max threshold."
  [db reindex-max]
  (let [novelty-size (get-in db [:novelty :size])]
    (> novelty-size reindex-max)))

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
  (let [children    (apply index/child-map comparator child-nodes)
        size        (->> child-nodes
                         (map :size)
                         (reduce +))
        first-flake (->> children first key)
        rhs         (->> children flake/last val :rhs)
        new-id      (random-uuid)]
    (assoc branch
           :id new-id
           :t t
           :children children
           :size size
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
                 (select-keys leaf [:id :ledger-alias]))
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
  [leaf t novelty]
  (if-let [new-flakes (-> leaf
                          (index/novelty-subrange t novelty)
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
        ;; the root. We iterate it with the nested transformer before calling
        ;; the nested transformer's completion arity. If there is more than one
        ;; node left in the stack, then the root was split because it
        ;; overflowed, so we make a new root, iterate all remaining nodes
        ;; including the new root, and then call the nested transformer's
        ;; completing arity.
        ([result]
         (if-let [remaining-nodes (not-empty @stack)]
           (do (vreset! stack [])
               (if (= (count remaining-nodes) 1)
                 (xf result)
                 (let [root-template (first remaining-nodes)
                       root-node     (reconstruct-branch root-template t remaining-nodes)]
                   (-> result
                       (xf root-node)
                       xf))))
           (xf result)))))))

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
      (>! changes-ch {:event     :new-index-file
                      :file-type file-type
                      :data      write-response
                      :address   (:address write-response)
                      :t         t})
      true)))

(defn write-node
  "Writes `node` to storage, and puts any errors onto the `error-ch`"
  [db idx node updated-ids changes-ch error-ch]
  (go
    (let [node         (dissoc node ::old-id)
          t            (:t node)
          display-node (select-keys node [:id :ledger-alias])]
      (try*
        (if (index/leaf? node)
          (do (log/debug "Writing index leaf:" display-node)
              (let [write-response (<? (storage/write-leaf db idx node))]
                (<! (notify-new-index-file write-response :leaf t changes-ch))
                (update-node-id node write-response)))

          (do (log/debug "Writing index branch:" display-node)
              (let [node*          (update-child-ids updated-ids node)
                    write-response (<? (storage/write-branch db idx node*))]
                (<! (notify-new-index-file write-response :branch t changes-ch))
                (update-node-id node* write-response))))

        (catch* e
                (log/error e
                           "Error writing novel index node:" display-node)
                (async/>! error-ch e))))))


(defn write-resolved-nodes
  [db idx changes-ch error-ch index-ch]
  (go-loop [stats     {:idx idx, :novel 0, :unchanged 0, :garbage #{}, :updated-ids {}}
            last-node nil]
    (if-let [{::keys [old-id] :as node} (<! index-ch)]
      (if (index/resolved? node)
        (let [updated-ids  (:updated-ids stats)
              written-node (<! (write-node db idx node updated-ids changes-ch error-ch))
              stats*       (cond-> stats
                             (not= old-id :empty) (update :garbage conj old-id)
                             true                 (update :novel inc)
                             true                 (assoc-in [:updated-ids (:id node)] (:id written-node)))]
          (recur stats*
                 written-node))
        (recur (update stats :unchanged inc)
               node))
      (assoc stats :root (index/unresolve last-node)))))


(defn refresh-index
  [{:keys [conn] :as db} changes-ch error-ch {::keys [idx t novelty root]}]
  (let [refresh-xf (comp (map preserve-id)
                         (integrate-novelty t novelty))
        novel?     (fn [node]
                     (seq (index/novelty-subrange node t novelty)))]
    (->> (index/tree-chan conn root novel? 1 refresh-xf error-ch)
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
   (->> index/types
        (map (partial extract-root db))
        (map (partial refresh-index db changes-ch error-ch))
        async/merge
        (async/reduce tally {:db db, :indexes [], :garbage #{}}))))

(defn refresh
  [{:keys [novelty t alias] :as db} changes-ch max-old-indexes]
  (go-try
    (let [start-time-ms (util/current-time-millis)
          novelty-size  (:size novelty)
          init-stats    {:ledger-alias alias
                         :t            t
                         :novelty-size novelty-size
                         :start-time   (util/current-time-iso)}]
      (if (dirty? db)
        (do (log/info "Refreshing Index:" init-stats)
            (let [error-ch   (async/chan)
                  refresh-ch (refresh-all db changes-ch error-ch)]
              (async/alt!
                error-ch
                ([e]
                 (throw e))

                refresh-ch
                ([{:keys [garbage], refreshed-db :db, :as _status}]
                 (let [refreshed-db* (assoc-in refreshed-db [:stats :indexed] t)
                       ;; TODO - ideally issue garbage/root writes to RAFT together
                       ;;        as a tx, currently requires waiting for both
                       ;;        through raft sync
                       garbage-res   (when (seq garbage)
                                       (let [write-res (<? (storage/write-garbage refreshed-db* garbage))]
                                         (<! (notify-new-index-file write-res :garbage t changes-ch))
                                         write-res))
                       db-root-res   (<? (storage/write-db-root refreshed-db* (:address garbage-res)))
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
                   ;; kick off automatic garbage collection
                   (async/thread
                    (garbage/clean-garbage indexed-db max-old-indexes))
                   indexed-db)))))
        db))))
