(ns fluree.db.util.async
  (:require [clojure.core.async :as async :refer [<! >! chan go]]
            [fluree.db.util :as util]
            [fluree.db.util.compare :refer [max-key-by]])
  #?(:cljs (:require-macros [fluree.db.util.async :refer [<? go-try]])))

#?(:clj (set! *warn-on-reflection* true))

#?(:clj
   (defn cljs-env?
     "Take the &env from a macro, and tell whether we are expanding into cljs."
     [env]
     (boolean (:ns env))))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
      https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

(defn throw-err [e]
  (when (util/exception? e)
    (throw e))
  e)

#?(:clj
   (defmacro <?
     "Like <! but throws errors."
     [ch]
     `(if-cljs
       (throw-err (cljs.core.async/<! ~ch))
       (throw-err (clojure.core.async/<! ~ch)))))

#?(:clj
   (defmacro <??
     "Like <!! but throws errors. Only works for Java platform - no JavaScript."
     [ch]
     `(throw-err (clojure.core.async/<!! ~ch))))

#?(:clj
   (defmacro go-try
     "Like go but catches the first thrown error and puts it on the returned channel."
     [& body]
     `(if-cljs
       (cljs.core.async/go
         (try
           ~@body
           (catch js/Error e# e#)))
       (clojure.core.async/go
         (try
           ~@body
           (catch Throwable t# t#))))))

(defn repartition-by
  [f ch]
  (let [xf     (comp cat (partition-by f))
        out-ch (chan 1 xf)]
    (async/pipe ch out-ch)))

(defn repartition-each-by
  [f chs]
  (map (partial repartition-by f)
       chs))

(defn void?
  [x]
  (= ::void x))

(defn void-vec
  [n]
  (vec (repeat n ::void)))

(defn fill-voids
  [items chs]
  (go
    (let [item-count (count items)]
      (loop [current-items items
             i             0]
        (if (< i item-count)
          (let [next-i (inc i)]
            (if (void? (nth current-items i))
              (when-some [new-item (<! (nth chs i))] ; return empty channel on
                                                     ; any closed input channel.
                (recur (assoc current-items i new-item) next-i))
              (recur current-items next-i)))
          current-items)))))

(defn void-unlike-keys
  [key-cmp key-fn k items]
  (mapv (fn [item]
          (if (zero? (key-cmp (key-fn item) k))
            item
            ::void))
        items))

(defn full?
  [xs]
  (every? (complement void?) xs))

(defn inner-join-by
  "Merges the items from multiple pre-sorted input channels into an output channel
  containing chunks where all items have the same key.

  Takes a collection of input channels `chs`, each containing items pre-sorted
  by `key-fn` using the `key-cmp` comparator. Returns a channel that outputs
  chunks (vectors) where each chunk contains exactly one item from each input
  channel, and all items in the chunk have the same value when `key-fn` is
  applied.

  The function advances through all channels simultaneously, always processing
  items with the current maximum key value and ignoring any items with a key
  value less than the current maximum. Items are only output when all channels
  have an item with the same key. This creates an 'inner join' behavior where
  only keys present in ALL channels appear in the output.

  Input channels must be pre-sorted in ascending order by the result of applying
  `key-fn` and comparing with `key-cmp`.

  `buf-or-n` is either a number corresponding to a buffer size or a buffer, and
  `xform` is a transducer. When those arguments are supplied, they will be
  applied to the output channel."
  ([key-cmp key-fn chs]
   (inner-join-by key-cmp key-fn nil chs))
  ([key-cmp key-fn buf-or-n chs]
   (inner-join-by key-cmp key-fn buf-or-n nil chs))
  ([key-cmp key-fn buf-or-n xform chs]
   (let [item-count (count chs)
         out-ch     (async/chan buf-or-n xform)]
     (go
       (loop [cur-items (void-vec item-count)]
         (if-some [next-items (<! (fill-voids cur-items chs))]
           (let [max-k        (apply max-key-by key-cmp key-fn next-items)
                 pruned-items (void-unlike-keys key-cmp key-fn max-k next-items)]
             (if (full? pruned-items)
               (do (>! out-ch pruned-items)
                   (recur (void-vec item-count)))
               (recur pruned-items)))
           (async/close! out-ch))))
     out-ch)))
