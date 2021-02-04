(ns fluree.db.index
  (:require [clojure.data.avl :as avl]
            [fluree.db.dbproto :as dbproto]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

(def types
  "The five possible index orderings based on the subject, predicate, object, and
  transaction flake attributes"
  #{:spot :psot :post :opst :tspo})

(defrecord IndexConfig [index-type comparator historyComparator])

#?(:clj
   (defmethod print-method IndexConfig [^IndexConfig config, ^java.io.Writer w]
     (.write w (str "#FdbIndexConfig "))
     (binding [*out* w]
       (pr {:idx-type (:index-type config)}))))


(defrecord IndexNode [block t rhs children config leftmost?]
  dbproto/IResolve
  (-resolve [this] (async/go this))
  (-first-flake [_]
    (key (first children)))
  (-rhs [_] rhs)
  dbproto/INode
  (-lookup [this flake]
    (val
      (or (avl/nearest children <= flake)
          (first children))))
  (-lookup-after [_ flake]
    (val
      (or (avl/nearest children > flake)
          (last children))))
  (-lookup-leaf [this flake]
    (go-try
      (let [child (dbproto/-lookup this flake)]
        (if (:leaf child)
          child
          (-> (<? (dbproto/-resolve child))
              (dbproto/-lookup-leaf flake)
              (<?))))))
  (-lookup-leaf-after [this flake]
    (go-try
      (let [child (dbproto/-lookup-after this flake)]
        (if (:leaf child)
          child
          (-> (<? (dbproto/-resolve child))
              (dbproto/-lookup-leaf-after flake)
              (<?)))))))


(defn index-node?
  [node]
  (instance? IndexNode node))


;; node should stay between b and b*2-1, else merge/split
(defrecord DataNode [block t flakes rhs config]
  dbproto/IResolve
  (-resolve [this] (async/go this))
  (-resolve-history [_]
    (throw (ex-info "-resolve-history called on DATA NODE!!!" {})))
  (-first-flake [_]
    (first flakes))
  (-rhs [_] rhs)
  dbproto/INode
  (-lookup [root flake]
    (throw (ex-info "-lookup was called on a data node, which shouldn't happen!"
                    {:status 500 :error :db/unexpected-error})))
  (-lookup-leaf [root flake]
    (log/error "-lookup-leaf was called on a data node, which shouldn't happen!")
    (async/go (ex-info "-lookup-leaf was called on a data node, which shouldn't happen!"
                       {:status 500 :error :db/unexpected-error}))))


(defn data-node
  "Creates a new data node"
  [block-id t flakes rhs config]
  (->DataNode block-id t flakes rhs config))


(defn data-node?
  [node]
  (instance? DataNode node))
