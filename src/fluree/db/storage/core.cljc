(ns fluree.db.storage.core
  (:require [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [clojure.data.avl :as avl]
            [fluree.db.util.log :as log]
            [fluree.db.index :as index]
            [fluree.db.dbproto :as dbproto]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            #?(:clj [fluree.db.util.async :refer [<? go-try]])
            #?(:clj [clojure.java.io :as io])
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.schema :as schema])
  #?(:cljs (:require-macros [fluree.db.util.async :refer [<? go-try]])
     :clj
           (:import (fluree.db.flake Flake))))

(declare ->UnresolvedNode)

#?(:clj
   (defn block-storage-path
     "For a ledger server, will return the storage path it is using for blocks for a given ledger."
     [conn network dbid]
     (let [storage-path (-> conn :meta :file-storage-path)]
       (when storage-path
         (io/file storage-path network dbid "block")))))

(defn storage-exists?
  "Returns truthy if the provided key exists in storage."
  [conn key]
  (let [storage-exists-fn (:storage-exists conn)]
    (storage-exists-fn key)))

(defn storage-read
  "Reads raw bytes from storage based on provided key.
  Returns core async channel with eventual response"
  [conn key]
  (let [storage-read-fn (:storage-read conn)]
    (storage-read-fn key)))

(defn storage-write
  "Writes raw bytes to storage based with provided key."
  [conn key val]
  (let [storage-write-fn (:storage-write conn)]
    (storage-write-fn key val)))

(defn serde
  "Returns serializer from connection."
  [conn]
  (:serializer conn))

(defn ledger-root-key
  [network ledger-id block]
  (str network "_" ledger-id "_root_" (util/zero-pad block 15)))

(defn ledger-garbage-key
  [network ledger-key block]
  (str network "_" ledger-key "_garbage_" block))

(defn ledger-node-key
  [network ledger-id idx-type base-id node-type]
  (str network "_" ledger-id "_" (name idx-type) "_" base-id "-" node-type))

(defn ledger-block-key
  [network ledger-id block]
  (str network "_" ledger-id "_block_" (util/zero-pad block 15)))

(defn ledger-block-file-path
  [network ledger-id block]
  (str network "/" ledger-id "/block/" (util/zero-pad block 15)))

(defn read-block
  "Returns a core async channel with the requested block."
  [conn network ledger-id block]
  (go-try
    (let [key  (ledger-block-key network ledger-id block)
          data (<? (storage-read conn key))]
      (when data
        (serdeproto/-deserialize-block (serde conn) data)))))

(defn read-block-version
  "Returns a core async channel with the requested block."
  [conn network ledger-id block version]
  (go-try
    (let [key  (str (ledger-block-key network ledger-id block) "--v" version)
          data (<? (storage-read conn key))]
      (when data
        (serdeproto/-deserialize-block (serde conn) data)))))

(defn write-block-version
  "Block data should look like:

  {:block  block (long)
   :flakes flakes
   :hash hash
   :sigs sigs
   :txns   {tid (tx-id, string)  {:cmd    command (JSON string)
                                  :sig    signature (string}]}
  "
  [conn network dbid block-data version]
  (go-try
    (let [persisted-data (select-keys block-data [:block :t :flakes])
          key            (str (ledger-block-key network dbid (:block persisted-data)) "--v" version)
          ser            (serdeproto/-serialize-block (serde conn) persisted-data)]
      (<? (storage-write conn key ser)))))

(defn write-block
  "Block data should look like:

  {:block  block (long)
   :flakes flakes
   :hash hash
   :sigs sigs
   :txns   {tid (tx-id, string)  {:cmd    command (JSON string)
                                  :sig    signature (string}]}
  "
  [conn network dbid block-data]
  (go-try
    (let [persisted-data (select-keys block-data [:block :t :flakes])
          key            (ledger-block-key network dbid (:block persisted-data))
          ser            (serdeproto/-serialize-block (serde conn) persisted-data)]
      (<? (storage-write conn key ser)))))

(defn child-data
  "Given a child, unresolved node, extracts just the data that will go into storage."
  [child]
  (select-keys child [:id :leaf :first :rhs :size]))

(defn write-history
  [conn history his-key next-his-key]
  (go-try
    (let [data {:flakes history
                :his    next-his-key}
          ser  (serdeproto/-serialize-leaf (serde conn) data)]
      (<? (storage-write conn his-key ser)))))

(defn write-leaf
  "Writes a leaf plus its history.

  Writes history first, and only on successful history write then writes leaf.

  Returns leaf's key"
  [conn network dbid idx-type id flakes history]
  (go-try
    (let [leaf-key      (ledger-node-key network dbid idx-type id "l")
          his-key       (str leaf-key "-his")
          data          {:flakes flakes
                         :his    his-key}
          ser           (serdeproto/-serialize-leaf (serde conn) data)
          write-his-ch  (write-history conn history his-key nil)
          write-leaf-ch (storage-write conn leaf-key ser)]
      ;; write history and leaf node in parallel
      (<? write-his-ch)
      (<? write-leaf-ch)
      leaf-key)))

(defn write-branch-data
  "Serializes final data for branch and writes it to provided key"
  [conn key data]
  (go-try
    (let [ser (serdeproto/-serialize-branch (serde conn) data)]
      (<? (storage-write conn key ser))
      key)))

(defn write-branch
  "Returns core async channel with index key"
  [conn network dbid idx-type id children]
  (let [branch-key (ledger-node-key network dbid idx-type id "b")
        child-vals (mapv #(child-data (val %)) children)
        rhs        (:rhs (last child-vals))
        data       {:children child-vals
                    :rhs      rhs}]
    (write-branch-data conn branch-key data)))

(defn write-garbage
  "Writes garbage record out for latest index."
  [db {:keys [garbage] :as progress}]
  (go-try
    (let [{:keys [conn network dbid block]} db
          garbage-key (ledger-garbage-key network dbid block)
          data        {:dbid    dbid
                       :block   block
                       :garbage garbage}
          ser         (serdeproto/-serialize-garbage (serde conn) data)]
      (<? (storage-write conn garbage-key ser))
      garbage-key)))

(defn write-db-root
  ([db]
   (write-db-root db nil))
  ([db custom-ecount]
   (go-try
     (let [{:keys [conn network dbid block t ecount stats spot psot post opst fork fork-block]} db
           db-root-key (ledger-root-key network dbid block)
           data        {:dbid      dbid
                        :block     block
                        :t         t
                        :ecount    (or custom-ecount ecount)
                        :stats     (select-keys stats [:flakes :size])
                        :spot      (child-data spot)
                        :psot      (child-data psot)
                        :post      (child-data post)
                        :opst      (child-data opst)
                        :timestamp (util/current-time-millis)
                        :prevIndex (or (:indexed stats) 0)
                        :fork      fork
                        :forkBlock fork-block}
           ser         (serdeproto/-serialize-db-root (serde conn) data)]
       (<? (storage-write conn db-root-key ser))
       db-root-key))))

;; TODO - sorting is temporary... place into node in correct order
(defn reify-history
  [conn key error-fn]
  (let [return-ch (async/promise-chan)]
    (go
      (try*
        (let [data (<! (storage-read conn key))]
          (if (or (nil? data) (instance? #?(:clj Throwable :cljs js/Error) data))
            (async/close! return-ch)
            (->> (serdeproto/-deserialize-leaf (serde conn) data)
                 :flakes
                 (sort flake/cmp-flakes-history)
                 (async/put! return-ch))))
        (catch* e
                (error-fn)
                (async/put! return-ch e)
                (async/close! return-ch))))
    ;; return promise chan immediately
    return-ch))

(defn source-novelty-t
  "Given a novelty set, a first-flake and rhs flake boundary,
  returns novelty subrange as a collection.

  If through-t is specified, will return novelty only through the
  specified t."
  ([novelty first-flake rhs leftmost?]
   (source-novelty-t novelty first-flake rhs leftmost? nil))
  ([novelty first-flake rhs leftmost? through-t]
   (let [novelty-subrange (cond
                            ;; standard case.. both left and right boundaries
                            (and rhs (not leftmost?)) (avl/subrange novelty > first-flake <= rhs)

                            ;; right only boundary
                            (and rhs leftmost?) (avl/subrange novelty <= rhs)

                            ;; left only boundary
                            (and (nil? rhs) (not leftmost?)) (avl/subrange novelty > first-flake)

                            ;; no boundary
                            (and (nil? rhs) leftmost?) novelty)]
     (if through-t
       (reduce
         (fn [novelty-set ^Flake f]
           (if (< (.-t ^Flake f) through-t)
             (disj novelty-set f)
             novelty-set))
         novelty-subrange novelty-subrange)
       novelty-subrange))))

(defn resolve-t
  [node t idx-novelty rhs leftmost? remove-preds error-fn]
  (let [result-ch (async/promise-chan)]
    (go
      (try*
        (let [base-node   (<? (dbproto/-resolve node))
              first-flake (dbproto/-first-flake base-node)
              node-t      (:t base-node)
              source      (cond
                            (> node-t t) :novelty
                            (< node-t t) :history
                            (= node-t t) :none)
              coll        (case source
                            :novelty (source-novelty-t idx-novelty first-flake rhs leftmost? t)
                            :history (->> (<? (dbproto/-resolve-history node))
                                          (take-while #(<= (.-t ^Flake %) t)))
                            :none [])
              ;; either conjoin flakes or disjoin them depending on if source if from history of novelty
              conj?       (case source
                            :novelty (fn [^Flake f] (true? (.-op f)))
                            :history (fn [^Flake f] (false? (.-op f)))
                            :none nil)
              flakes      (doall (reduce
                                   (fn [acc ^Flake f]
                                     (cond ((or remove-preds #{}) (.-p f))
                                           (disj acc f)

                                           (conj? f) (conj acc f)

                                           :else (disj acc f)))
                                   (:flakes base-node) coll))
              resolved-t  (assoc base-node :flakes flakes)]
          (async/put! result-ch resolved-t))
        (catch* e
                (error-fn)
                (async/put! result-ch e)
                (async/close! result-ch))))
    ;; return promise chan immediately
    result-ch))

(defn resolve-history-range
  "Gets a history slice of a node with the oldest 't' from-t, to the
  most recent 't', to-t.

  Returns sorted set in novelty's sort order (spot, psot, post, or opst)"
  [node from-t to-t idx-novelty leftmost?]
  (go
    (try*
      (let [node-t      (:t node)
            history     (when (or (nil? from-t) (<= node-t from-t)) ;; don't pull history if requested 't' range is entirely in novelty
                          (cond->> (<? (dbproto/-resolve-history node))
                                   (> to-t node-t) (drop-while #(< (.-t ^Flake %) to-t))
                                   from-t (take-while #(<= (.-t ^Flake %) from-t))))
            first-flake (dbproto/-first-flake node)
            rhs         (dbproto/-rhs node)
            _           (when-not (:leaf node)
                          (throw (ex-info (str "resolve-history-range called on index node: " (:id node))
                                          {:status 500 :error :db/unexpected-error})))
            novelty     (source-novelty-t idx-novelty first-flake rhs leftmost? to-t)]
        (into novelty history))
      (catch* e
              (log/error e)
              (throw e)))))

(defn resolve-to-t
  [node id tempid rhs leftmost? t tt-id idx-novelty conn fast-foward-db? remove-preds]
  ;; TODO - need to always ensure 't' is not beyond the current DB 't'
  ;; TODO - else could cache a "future" result before we are at that time
  (if (or fast-foward-db? (= :empty id))
    (resolve-t node t idx-novelty rhs leftmost? remove-preds nil)

    (let [object-cache (:object-cache conn)]
      (if (not (empty? remove-preds))
        (do (object-cache [id t tt-id tempid] nil)
            (object-cache
              [id t tt-id tempid]
              (fn [_] (resolve-t node t idx-novelty rhs leftmost? remove-preds
                                 (fn [] (object-cache [id t tt-id tempid] nil))))))
        (object-cache
          [id t tt-id tempid]
          (fn [_] (resolve-t node t idx-novelty rhs leftmost? remove-preds
                             (fn [] (object-cache [id t tt-id tempid] nil)))))))))


(defn read-branch
  "Reads and deserializes branch node."
  [conn key]
  (go-try (let [data (storage-read conn key)]
            (when data
              (serdeproto/-deserialize-branch (serde conn) (<? data))))))


(defn reify-branch
  "Should throw if no result... should never be the case."
  [conn config network dbid key block t tt-id leftmost? tempid error-fn]
  (let [return-ch (async/promise-chan)]
    ;; kick of retrieval/reification process
    (async/go
      (try*
        (let [data        (<? (read-branch conn key))
              _           (when (nil? data)
                            (throw (ex-info (str "Unable to retrieve key from storage: " key)
                                            {:status 500 :error :db/storage-error})))
              _           (when (util/exception? data)
                            (throw data))
              {:keys [children rhs]} data
              {:keys [comparator]} config
              child-nodes (map-indexed (fn [idx {:keys [id leaf first rhs size] :as child}]
                                         (let [at-leftmost? (and leftmost? (zero? idx))]
                                           (->UnresolvedNode conn config network dbid id leaf first rhs size block t tt-id at-leftmost? tempid))) children)
              idx-node    (index/->IndexNode block t
                                             rhs
                                             ;; child nodes are in a sorted map with {<lastFlake> <UnresolvedNode>} as k/v
                                             (apply avl/sorted-map-by comparator (interleave (map :first child-nodes) child-nodes))
                                             config
                                             leftmost?)]
          (async/put! return-ch idx-node))
        (catch* e
                (error-fn)
                (async/put! return-ch e)
                (async/close! return-ch))))
    ;; return promise-chan immediately
    return-ch))


(defn read-leaf
  "Reads and deserializes a leaf node"
  [conn key]
  (go-try
    (let [data (storage-read conn key)]
      (when data
        (serdeproto/-deserialize-leaf (serde conn) (<? data))))))


(defn reify-leaf
  "Should throw if no result... should never be the case."
  [conn config key block t rhs error-fn]
  (assert (:comparator config) (str "Cannot reify leaf, config does not have a comparator. Config: " (pr-str config)))
  (let [return-ch (async/promise-chan)]
    ;; kick of retrieval/reification process
    (async/go
      (try*
        (let [leaf (async/<! (read-leaf conn key))
              _    (when (nil? leaf)
                     (throw (ex-info (str "Unable to retrieve key from storage: " key)
                                     {:status 500 :error :db/storage-error})))
              _    (when (util/exception? leaf)
                     (throw leaf))
              {:keys [flakes his]} leaf
              {:keys [comparator]} config
              node (index/data-node block t (apply flake/sorted-set-by comparator flakes) rhs config)]
          (async/put! return-ch node))
        (catch* e
                (error-fn)
                (async/put! return-ch e)
                (async/close! return-ch))))
    ;; return promise-chan immediately
    return-ch))


;; TODO - create an EmptyNode type so don't need to check for :empty in .-id
;; block needs to be passed down from the parent
(defrecord UnresolvedNode [conn config network dbid id leaf first rhs size block t tt-id leftmost? tempid]
  dbproto/IResolve
  (-first-flake [_] first)
  (-rhs [_] rhs)
  (-resolve [_]
    ;; returns async promise chan
    (if (= :empty id)
      (let [pc (async/promise-chan)]
        (async/put! pc (index/data-node 0 0 (flake/sorted-set-by (:comparator config)) nil config))
        pc)
      (let [object-cache (:object-cache conn)]
        (object-cache
          [id tempid]
          (fn [_]
            (if leaf
              (reify-leaf conn config id block t rhs (fn [] (object-cache [id tempid] nil)))
              (reify-branch conn config network dbid id block t tt-id leftmost? tempid (fn [] (object-cache [id tempid] nil)))))))))
  (-resolve-history [_]
    ;; will return a core-async promise channel
    (let [history-id   (str id "-his")
          object-cache (:object-cache conn)
          ;; clear cache if an error occurs
          error-fn     (fn [] (object-cache history-id nil))]
      (object-cache
        history-id
        (fn [_] (reify-history conn history-id error-fn)))))
  (-resolve-to-t [this to-t idx-novelty]
    (resolve-to-t this id tempid rhs leftmost? to-t tt-id idx-novelty conn false #{}))
  (-resolve-to-t [this to-t idx-novelty fast-foward-db?]
    (resolve-to-t this id tempid rhs leftmost? to-t tt-id idx-novelty conn fast-foward-db? #{}))
  (-resolve-to-t [this to-t idx-novelty fast-foward-db? remove-preds]
    (resolve-to-t this id tempid rhs leftmost? to-t tt-id idx-novelty conn fast-foward-db? remove-preds))
  (-resolve-history-range [node from-t to-t]
    ;; returns core async channel
    (resolve-history-range node from-t to-t nil leftmost?))
  (-resolve-history-range [node from-t to-t idx-novelty]
    (resolve-history-range node from-t to-t idx-novelty leftmost?)))


#?(:clj
   (defmethod print-method UnresolvedNode [^UnresolvedNode node, ^java.io.Writer w]
     (.write w (str "#FlureeUnresolvedNode "))
     (binding [*out* w]
       (pr {:network (:network node) :dbid (:dbid node) :id (:id node) :leaf (:leaf node) :first (:first node) :rhs (:rhs node) :size (:size node)
            :block   (:block node) :t (:t node) :leftmost? (:leftmost? node) :tempid (:tempid node) :config (:config node)}))))


(defn reify-index-root
  "Turns each index root node into an unresolved node."
  [conn index-configs network dbid index index-data block t]
  (let [cfg (or (get index-configs index)
                (throw (ex-info (str "Internal error reifying db root index: " (pr-str index))
                                {:status 500
                                 :error  :db/unexpected-error})))]
    (map->UnresolvedNode (assoc index-data :conn conn
                                           :config cfg
                                           :network network
                                           :dbid dbid
                                           :block block :t t
                                           :leftmost? true))))


(defn reify-db-root
  "Constructs db from blank-db, and ensure index roots have proper config as unresolved nodes."
  [conn blank-db root-data]
  (let [{:keys [network dbid index-configs]} blank-db
        {:keys [block t ecount stats]} root-data
        db* (assoc blank-db :block block
                            :t t
                            :ecount ecount
                            :stats (assoc stats :indexed block))]
    (reduce
      (fn [db idx]
        (assoc db idx (reify-index-root conn index-configs network dbid idx (get root-data idx) block t)))
      db* [:spot :psot :post :opst])))


(defn read-garbage
  "Returns a all data for a db index root of a given block."
  [conn network dbid block]
  (go-try
    (let [key  (ledger-garbage-key network dbid block)
          data (storage-read conn key)]
      (when data
        (serdeproto/-deserialize-garbage (serde conn) (<? data))))))


(defn read-db-root
  "Returns all data for a db index root of a given block."
  [conn network dbid block]
  (go-try
    (let [key  (ledger-root-key network dbid block)
          data (storage-read conn key)]
      (when data
        (serdeproto/-deserialize-db-root (serde conn) (<? data))))))


(defn reify-db
  "Reifies db at specified index point. If unable to read db-root at index, throws."
  [conn network dbid blank-db index]
  (go-try
    (let [db-root (read-db-root conn network dbid index)]
      (when-not db-root
        (throw (ex-info (str "Database " network "/" dbid " could not be loaded at index point: " index ".")
                        {:status 400
                         :error  :db/unavailable})))
      (let [db  (reify-db-root conn blank-db (<? db-root))
            db* (assoc db :schema (<? (schema/schema-map db)))]
        (assoc db* :settings (<? (schema/setting-map db*)))))))


;; TODO - should look to add some parallelism to block fetches
(defn block-range
  "Returns a channel that will contains blocks in specified range."
  ([conn network dbid start] (block-range conn network dbid start nil))
  ([conn network dbid start end]
   (log/trace "Block-range request: " network dbid start end)
   (go-try
     (assert (>= end start) "Block range should be in ascending order, from earliest (smallest) block to most recent (largest) block.")
     (let [parallelism (:parallelism conn)]
       (loop [block  start
              result []]
         (let [res (<! (read-block conn network dbid block))]
           (cond (or (nil? res) (instance? #?(:clj Throwable :cljs js/Error) res))
                 result

                 (= block end)
                 (conj result res)

                 :else
                 (recur (inc block) (conj result res)))))))))

(defn block
  "Reads a single block from storage"
  [conn network dbid block]
  (go-try
    (-> (<? (block-range conn network dbid block block))
        (first))))
