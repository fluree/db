(ns fluree.db.util.async
  (:require [clojure.core.async :as async :refer [<! >! chan go-loop]]
            [fluree.db.util :as util]
            [fluree.db.util.compare :refer [max-key-by min-key-by]])
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

(def empty-channel
  (doto (async/chan)
    async/close!))

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
  (let [item-count (count items)]
    (go-loop [current-items items
              i             0]
      (if (< i item-count)
        (let [next-i (inc i)]
          (if (void? (nth current-items i))
            (when-some [new-item (<! (nth chs i))] ; return nil any closed input
                                                   ; channel.
              (recur (assoc current-items i new-item) next-i))
            (recur current-items next-i)))
        current-items))))

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
     (go-loop [cur-items (void-vec item-count)]
       (if-some [next-items (<! (fill-voids cur-items chs))]
         (let [max-k        (apply max-key-by key-cmp key-fn next-items)
               pruned-items (void-unlike-keys key-cmp key-fn max-k next-items)]
           (if (full? pruned-items)
             (do (>! out-ch pruned-items)
                 (recur (void-vec item-count)))
             (recur pruned-items)))
         (async/close! out-ch)))
     out-ch)))

(defn populated-items
  "Filters out void items from `items`, returning `nil` if all items are void or
  `items` was empty ."
  [items]
  (->> items
       (filterv (complement void?))
       not-empty))

(defn min-populated-key
  "Returns the minimum key from populated items, or nil if all items are void."
  [key-cmp key-fn items]
  (some->> items
           populated-items
           (apply min-key-by key-cmp key-fn)))

(defn replace-unlike-keys
  "Replaces items that don't have the target key with empty vectors.

  Similar to void-unlike-keys but replaces with [] instead of ::void."
  [key-cmp key-fn k items]
  (mapv (fn [item]
          (if (and (not (void? item))
                   (zero? (key-cmp (key-fn item) k)))
            item
            []))
        items))

(defn void-output-items
  "Replaces output items with ::void, keeping next items for non-output positions."
  [output-items next-items]
  (mapv (fn [output-item next-item]
          (if (= output-item [])
            next-item
            ::void))
        output-items
        next-items))

(defn fill-voids-outer
  "Like fill-voids, but for outer joins - only returns nil when ALL channels are closed.

  Fills void slots from their corresponding channels, but if a channel is closed,
  leaves that slot as void and continues processing other channels."
  [items chs]
  (let [item-count (count items)]
    (go-loop [current-items items
              i             0]
      (if (< i item-count)
        (let [next-i (inc i)]
          (if (void? (nth current-items i))
            (if-some [new-item (<! (nth chs i))]
              (recur (assoc current-items i new-item) next-i)
              (recur current-items next-i))
            (recur current-items next-i)))
        (when (->> current-items populated-items seq)
          current-items)))))

(defn outer-join-by
  "Merges items from multiple pre-sorted input channels into an output channel
  containing chunks where items have the same key.

  Similar to inner-join-by, but performs an outer join - outputs chunks for
  ANY key present in at least one channel. Positions without matching items
  are filled with empty vectors [].

  Takes a collection of input channels `chs`, each containing items pre-sorted
  by `key-fn` using the `key-cmp` comparator. Returns a channel that outputs
  chunks (vectors) where each chunk may contain either an item from that channel
  or an empty vector [] if that channel doesn't have the key.

  The function advances through all channels simultaneously, always processing
  items with the current minimum key value and replacing items with keys greater
  than the minimum with []. Items are output whenever ANY channel has an item
  with the minimum key.

  Input channels must be pre-sorted in ascending order by the result of applying
  `key-fn` and comparing with `key-cmp`.

  `buf-or-n` is either a number corresponding to a buffer size or a buffer, and
  `xform` is a transducer. When those arguments are supplied, they will be
  applied to the output channel."
  ([key-cmp key-fn chs]
   (outer-join-by key-cmp key-fn nil chs))
  ([key-cmp key-fn buf-or-n chs]
   (outer-join-by key-cmp key-fn buf-or-n nil chs))
  ([key-cmp key-fn buf-or-n xform chs]
   (let [item-count (count chs)
         out-ch     (async/chan buf-or-n xform)]
     (go-loop [cur-items (void-vec item-count)]
       (if-some [next-items (<! (fill-voids-outer cur-items chs))]
         (if-some [min-k (min-populated-key key-cmp key-fn next-items)]
           (let [output-items (replace-unlike-keys key-cmp key-fn min-k next-items)
                 next-items   (void-output-items output-items next-items)]
             (>! out-ch output-items)
             (recur next-items))
           (recur next-items))
         (async/close! out-ch)))
     out-ch)))
