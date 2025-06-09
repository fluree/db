(ns fluree.db.util.async
  (:require
   #?(:clj [clojure.core.async.impl.dispatch :as dispatch])
   [clojure.core.async :as async]
   [clojure.core.async.impl.protocols :as async-protocols]
   [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]])
  #?(:cljs (:require-macros [fluree.db.util.async :refer [<? go-try]]))
  #?(:clj (:import [java.util.concurrent Executor]
                   [org.slf4j MDC])))

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

;;  "OpenTelemetry uses ThreadLocal storage to propagate trace state.
;;   This typically requires wrapping your tasks or executors so that this state
;;   is propagated to threads when desired.
;;   See https://github.com/open-telemetry/opentelemetry-java/blob/main/context/src/main/java/io/opentelemetry/context/Context.java
;;   core.async does not expose the thread pool direclty so we need to monkey patch it.

;;   core.async will propagate clojure var bindings, so alternatively it might be possible
;;   to implement a ContextStorageProvider that stores state in a var instead of thread locals.
;;   I spent a bit of time trying to figure this out but it seems quite a bit more involved than
;;   patching core.aysnc"
#?(:clj
   (do
     (defn wrap-mdc ^Runnable [^Runnable r]
       (let [context (MDC/getCopyOfContextMap)]
         (reify Runnable
           (run [_]
             (let [current (MDC/getCopyOfContextMap)]
               (try
                 (MDC/setContextMap context)
                 (.run r)
                 (finally
                   (MDC/setContextMap current))))))))

     (defonce original-run dispatch/run)

     ;; Try to use OpenTelemetry if available, otherwise fall back to just MDC wrapping
     (try
       (import [io.opentelemetry.context Context])
       (defn wrapped-runable ^Runnable [^Runnable r]
         (.wrap (Context/current) (wrap-mdc r)))
       (catch ClassNotFoundException _
         ;; OpenTelemetry not available, fall back to just MDC wrapping
         (defn wrapped-runable ^Runnable [^Runnable r]
           (wrap-mdc r))))

     (defn patched-run
       "Runs Runnable r with OpenTelemetry context propagation."
       [^Runnable r]
       (original-run (wrapped-runable r)))

     (alter-var-root #'dispatch/run (constantly patched-run))

     (defonce ^Executor original-thread-macro-executor @#'async/thread-macro-executor)

     (defn context-wrapping-executor ^Executor [^Executor wrapped-executor]
       (reify Executor
         (execute [_ runnable]
           (.execute wrapped-executor (wrapped-runable runnable)))))

     (alter-var-root #'async/thread-macro-executor (constantly (context-wrapping-executor original-thread-macro-executor)))))
