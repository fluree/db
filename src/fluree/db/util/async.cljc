(ns fluree.db.util.async
  (:require
    [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
    [clojure.core.async :as async]
    [clojure.core.async.impl.protocols :as async-protocols])
  #?(:cljs (:require-macros [fluree.db.util.async :refer [<? go-try]])))

#?(:clj (set! *warn-on-reflection* true))

;; some macros for working with core async

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
   (defmacro alts??
     "Like alts!! but throws errors. Only works for Java platform - no JavaScript."
     [ports & opts]
     `(let [[result# ch#] (clojure.core.async/alts!! ~ports ~@opts)]
        [(throw-err result#) ch#])))

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

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (instance? #?(:clj Throwable :cljs js/Error) x)
    (throw (ex-info #?(:clj (or (.getMessage ^Throwable x) (str x))
                       :cljs (str x))
                    (or (ex-data x) {})
                    x))
    x))

(defn merge-into?
  "Takes a sequence of single-value chans and returns the conjoined into collection.
  Realizes entire channel sequence first, and if an error value exists returns just the exception."
  [coll chs]
  (async/go
    (try*
      (loop [[c & r] chs
             acc coll]
        (if-not c
          acc
          (recur r (conj acc (<? c)))))
      (catch* e
              e))))

(defn into?
  "Like async/into, but checks each item for an error response and returns exception
  onto the response channel instead of results if there is one."
  [coll chan]
  (async/go
    (try*
      (loop [acc coll]
        (if-some [v (<? chan)]
          (recur (conj acc v))
          acc))
      (catch* e
              e))))

(defn channel?
  "Returns true if core async channel."
  [x]
  (satisfies? async-protocols/Channel x))
