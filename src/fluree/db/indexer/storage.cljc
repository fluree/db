(ns fluree.db.indexer.storage
  (:require [fluree.db.serde.protocol :as serdeproto]
            [fluree.db.flake :as flake]
            [clojure.string :as str]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.index :as index]
            [clojure.core.async :refer [go <!] :as async]
            [fluree.db.util.async #?(:clj :refer :cljs :refer-macros) [<? go-try]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.conn.proto :as conn-proto]))

#?(:clj (set! *warn-on-reflection* true))

(defn serde
  "Returns serializer from connection."
  [conn]
  (:serializer conn))

(defn ledger-root-key
  [ledger-alias t]
  (str ledger-alias "_root_" (util/zero-pad t 15)))

(defn ledger-garbage-prefix
  [ledger-alias]
  (str/join "_" [ledger-alias "garbage"]))

(defn ledger-garbage-key
  [ledger-alias t]
  (let [pre (ledger-garbage-prefix ledger-alias)]
    (str/join "_" [pre t])))

(defn child-data
  "Given a child, unresolved node, extracts just the data that will go into
  storage."
  [child]
  (select-keys child [:id :leaf :first :rhs :size]))

(defn notify-new-index-file
  "Sends new file update into the changes notification async channel
  if it exists. This is used to synchronize files across consensus, otherwise
  a changes-ch won't be present and this won't be used."
  [changes-ch written-node write-response]
  (when changes-ch
    (async/go
      (let [file-type (cond
                        (contains? written-node :children) :branch
                        (contains? written-node :flakes) :leaf
                        (contains? written-node :garbage) :garbage
                        (contains? written-node :ecount) :root)]
        (async/>! changes-ch {:event     :new-index-file
                              :file-type file-type
                              :data      write-response
                              :address   (:address write-response)
                              :t         (:t written-node)})))))

(defn write-leaf
  "Writes `leaf` to storage under the provided `leaf-id`, computing a new id if
  one isn't provided. Returns the leaf map with the id used attached uner the
  `:id` key"
  [{:keys [conn ledger] :as _db} idx-type changes-ch leaf]
  (go-try
    (let [ser   (serdeproto/-serialize-leaf (serde conn) leaf)
          res   (<? (conn-proto/-index-file-write conn ledger idx-type ser))
          leaf' (assoc leaf :id (:address res))]
      (notify-new-index-file changes-ch leaf' res)
      leaf')))

(defn write-branch-data
  "Serializes final data for branch and writes it to provided key.
  Returns two-tuple of response output and raw bytes written."
  [{:keys [conn ledger] :as _db} idx-type data]
  (go-try
    (let [ser (serdeproto/-serialize-branch (serde conn) data)
          res (<? (conn-proto/-index-file-write conn ledger idx-type ser))]
      res)))

(defn write-branch
  "Writes `branch` to storage under the provided `branch-id`, computing a new id
  if one isn't provided. Returns the branch map with the id used attached uner
  the `:id` key"
  [db idx-type changes-ch {:keys [children] :as branch}]
  (go-try
    (let [child-vals (->> children
                          (map val)
                          (mapv child-data))
          data       {:children child-vals}
          res        (<? (write-branch-data db idx-type data))
          branch'    (assoc branch :id (:address res))]
      (notify-new-index-file changes-ch branch' res)
      branch')))

(defn write-garbage
  "Writes garbage record out for latest index."
  [db changes-ch garbage]
  (go-try
    (let [{:keys [conn ledger ledger-alias t]} db
          t'       (- t) ;; use positive t integer
          data     {:ledger-alias ledger-alias
                    :block     t'
                    :garbage   garbage}
          ser      (serdeproto/-serialize-garbage (serde conn) data)
          res      (<? (conn-proto/-index-file-write conn ledger :garbage ser))
          garbage' (assoc data :address (:address res))]
      (notify-new-index-file changes-ch garbage' res)
      garbage')))

(defn extract-schema-root
  "Transform the schema cache for serialization by turning every predicate into a tuple of [pid datatype]."
  [{:keys [schema]}]
  (->> (:pred schema)
       (reduce (fn [root [k {:keys [datatype]}]]
                 (if (number? k)
                   (if datatype
                     (conj root [k datatype])
                     (conj root [k]))
                   root))
               [])))

(defn write-db-root
  [db changes-ch custom-ecount]
  (go-try
    (let [{:keys [conn ledger commit t ecount stats spot psot post opst
                  tspo fork fork-block schema]} db
          t'        (- t)
          ledger-alias (:id commit)
          preds     (extract-schema-root db)
          data      {:ledger-alias ledger-alias
                     :t         t'
                     :ecount    (or custom-ecount ecount)
                     :preds     preds
                     :stats     (select-keys stats [:flakes :size])
                     :spot      (child-data spot)
                     :psot      (child-data psot)
                     :post      (child-data post)
                     :opst      (child-data opst)
                     :tspo      (child-data tspo)
                     :timestamp (util/current-time-millis)
                     :prevIndex (or (:indexed stats) 0)
                     :fork      fork
                     :forkBlock fork-block}
          ser       (serdeproto/-serialize-db-root (serde conn) data)
          res       (<? (conn-proto/-index-file-write conn ledger :root ser))]
      (notify-new-index-file changes-ch data res)
      res)))


(defn read-branch
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (conn-proto/-index-file-read conn key))]
      (serdeproto/-deserialize-branch serializer data))))

(defn read-leaf
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (conn-proto/-index-file-read conn key))]
      (serdeproto/-deserialize-leaf serializer data))))

(defn reify-index-root
  "Turns each index root node into an unresolved node."
  [_conn {:keys [ledger-alias comparators t]} index index-data]
  (let [cmp (or (get comparators index)
                (throw (ex-info (str "Internal error reifying db index root: "
                                     (pr-str index))
                                {:status 500
                                 :error  :db/unexpected-error})))]
    (cond-> index-data
            (:rhs index-data) (update :rhs flake/parts->Flake)
            (:first index-data) (update :first flake/parts->Flake)
            true (assoc :comparator cmp
                        :ledger-alias ledger-alias
                        :t t
                        :leftmost? true))))


(defn reify-db-root
  "Constructs db from blank-db, and ensure index roots have proper config as unresolved nodes."
  [conn blank-db root-data]
  (let [{:keys [t ecount stats preds]} root-data
        db* (assoc blank-db :t (- t)
                            :preds preds
                            :ecount ecount
                            :stats (assoc stats :indexed t))]
    (reduce
      (fn [db idx]
        (let [idx-root (reify-index-root conn db idx (get root-data idx))]
          (assoc db idx idx-root)))
      db* index/types)))


(defn read-garbage
  "Returns garbage file data for a given index t."
  [conn ledger-alias t]
  (go-try
    (let [key  (ledger-garbage-key ledger-alias t)
          data (<? (conn-proto/-index-file-read conn key))]
      (when data
        (serdeproto/-deserialize-garbage (serde conn) data)))))


(defn read-db-root
  "Returns all data for a db index root of a given t."
  ([conn idx-address]
   (go-try
     (let [data (<? (conn-proto/-index-file-read conn idx-address))]
       (when data
         (serdeproto/-deserialize-db-root (serde conn) data)))))
  ([conn ledger-alias block]
   (go-try
     (let [key  (ledger-root-key ledger-alias block)
           data (<? (conn-proto/-index-file-read conn key))]
       (when data
         (serdeproto/-deserialize-db-root (serde conn) data))))))


(defn reify-db
  "Reifies db at specified index point. If unable to read db-root at index,
  throws."
  ([conn blank-db idx-address]
   (go-try
     (let [db-root (<? (read-db-root conn idx-address))]
       (if-not db-root
         (throw (ex-info (str "Database " (:address blank-db)
                              " could not be loaded at index point: "
                              idx-address ".")
                         {:status 400
                          :error  :db/unavailable}))
         (let [db     (reify-db-root conn blank-db db-root)
               schema (<? (vocab/load-schema db))
               db*    (assoc db :schema schema)]
           db*))))))

(defn fetch-child-attributes
  [conn {:keys [id comparator leftmost?] :as branch}]
  (go-try
    (if-let [{:keys [children]} (<? (read-branch conn id))]
      (let [branch-metadata (select-keys branch [:comparator :ledger-alias
                                                 :t :tt-id :tempid])
            child-attrs     (map-indexed (fn [i child]
                                           (-> branch-metadata
                                               (assoc :leftmost? (and leftmost?
                                                                      (zero? i)))
                                               (merge child)))
                                         children)
            child-entries   (mapcat (juxt :first identity)
                                    child-attrs)]
        (apply flake/sorted-map-by comparator child-entries))
      (throw (ex-info (str "Unable to retrieve index branch with id "
                           id " from storage.")
                      {:status 500, :error :db/storage-error})))))

(defn fetch-leaf-flakes
  [conn {:keys [id comparator]}]
  (go-try
    (if-let [{:keys [flakes] :as leaf} (<? (read-leaf conn id))]
      (apply flake/sorted-set-by comparator flakes)
      (throw (ex-info (str "Unable to retrieve leaf node with id: "
                           id " from storage")
                      {:status 500, :error :db/storage-error})))))

(defn resolve-index-node
  ([conn node]
   (resolve-index-node conn node nil))
  ([conn {:keys [comparator leaf] :as node} error-fn]
   (assert comparator "Cannot resolve index node; configuration does not have a comparator.")
   (let [return-ch (async/chan)]
     (go
       (try*
         (let [[k data] (if leaf
                          [:flakes (<? (fetch-leaf-flakes conn node))]
                          [:children (<? (fetch-child-attributes conn node))])]
           (async/put! return-ch
                       (assoc node k data)))
         (catch* e
                 (log/error e "Error resolving index node")
                 (when error-fn
                   (try*
                     (error-fn)
                     (catch* e (log/error e "Error executing error-fn in resolve-index-node!"))))
                 (async/put! return-ch e)
                 (async/close! return-ch))))
     return-ch)))

(defn resolve-empty-leaf
  [{:keys [comparator] :as node}]
  (let [ch         (async/chan)
        empty-set  (flake/sorted-set-by comparator)
        empty-node (assoc node :flakes empty-set)]
    (async/put! ch empty-node)
    ch))

(defn resolve-empty-branch
  [{:keys [comparator ledger-alias] :as node}]
  (let [ch         (async/chan)
        child      (index/empty-leaf ledger-alias comparator)
        children   (flake/sorted-map-by comparator child)
        empty-node (assoc node :children children)]
    (async/put! ch empty-node)
    ch))

(defn resolve-empty-node
  [node]
  (if (index/resolved? node)
    (doto (async/chan)
      (async/put! node))
    (if (index/leaf? node)
      (resolve-empty-leaf node)
      (resolve-empty-branch node))))
