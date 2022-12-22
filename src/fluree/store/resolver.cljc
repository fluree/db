(ns fluree.store.resolver
  (:require [clojure.core.async :refer [go <!] :as async]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.async #?(:clj :refer :cljs :refer-macros) [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log]
            [fluree.db.serde.protocol :as serdeproto]
            [fluree.store.protocols :as store-proto]))

(defn read-branch
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (store-proto/read conn key))]
      (serdeproto/-deserialize-branch serializer data))))

(defn fetch-child-attributes
  [conn {:keys [id comparator leftmost?] :as branch}]
  (go-try
    (if-let [{:keys [children]} (<? (read-branch conn id))]
      (let [branch-metadata (select-keys branch [:comparator :network :ledger-id
                                                 :block :t :tt-id :tempid])
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

(defn read-leaf
  [{:keys [serializer] :as conn} key]
  (go-try
    (when-let [data (<? (store-proto/read conn key))]
      (serdeproto/-deserialize-leaf serializer data))))

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
   (resolve-index-node node nil))
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
                   (error-fn
                     (async/put! return-ch e)
                     (async/close! return-ch))))))
     return-ch)))

(defn resolve-empty-leaf
  [{:keys [comparator] :as node}]
  (let [ch         (async/chan)
        empty-set  (flake/sorted-set-by comparator)
        empty-node (assoc node :flakes empty-set)]
    (async/put! ch empty-node)
    ch))

(defn lookup-cache
  [cache-atom k value-fn]
  (if (nil? value-fn)
    (swap! cache-atom cache/evict k)
    (when-let [v (get @cache-atom k)]
      (swap! cache-atom cache/hit k)
      v)))

(defn default-object-cache-factory
  "Generates a default object cache."
  [cache-size]
  (cache/lru-cache-factory {} :threshold cache-size))

(defn default-async-cache-fn
  "Default asynchronous object cache to use for ledger."
  [cache-atom]
  (fn [k value-fn]
    (let [out (async/chan)]
      (if-let [v (lookup-cache cache-atom k value-fn)]
        (async/put! out v)
        (async/go
          (let [v (async/<! (value-fn k))]
            (when-not (util/exception? v)
              (swap! cache-atom cache/miss k v))
            (async/put! out v))))
      out)))

(defn create-async-cache
  [{:keys [cache-size-bytes] :as _config}]
  (let [memory  (or cache-size-bytes 1000000) ;; default 1MB memory
        memory-object-size (quot memory 100000)
        default-cache-atom (atom (default-object-cache-factory memory-object-size))]
    (default-async-cache-fn default-cache-atom)))

(defn resolve-node
  [store async-cache {:keys [id tempid] :as node}]
  (if (= :empty id)
    (resolve-empty-leaf node)
    (async-cache
      [::resolve id tempid]
      (fn [_]
        (resolve-index-node store node
                            (fn []
                              (async-cache [::resolve id tempid] nil)))))))
