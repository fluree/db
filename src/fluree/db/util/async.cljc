(ns fluree.db.util.async
  (:require [clojure.core.async :as async :refer [<! >! chan go]]
            [fluree.db.util.core :as util]
            [fluree.db.util.compare :refer [max-key-by]]))

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

(defn nil-vec
  [n]
  (vec (repeat n nil)))

(defn void?
  [x]
  (= ::void x))

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
  [cmp key-fn k items]
  (mapv (fn [item]
          (if (zero? (cmp (key-fn item) k))
            item
            ::void))
        items))

(defn full?
  [xs]
  (every? some? xs))

(defn fuse-by
  ([cmp key-fn chs]
   (fuse-by cmp key-fn nil chs))
  ([cmp key-fn buf-or-n chs]
   (fuse-by cmp key-fn buf-or-n nil chs))
  ([cmp key-fn buf-or-n xform chs]
   (let [ch-count (count chs)
         out-ch   (async/chan buf-or-n xform)]
     (go
       (loop [cur-items (nil-vec ch-count)]
         (if-some [next-items (<! (fill-voids cur-items chs))]
           (let [max-k  (apply max-key-by cmp key-fn next-items)
                 pruned (void-unlike-keys cmp key-fn max-k next-items)]
             (if (full? pruned)
               (do (>! out-ch pruned)
                   (recur (nil-vec ch-count)))
               (recur pruned)))
           (async/close! out-ch))))
     out-ch)))
