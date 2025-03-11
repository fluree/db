(ns fluree.db.util.log
  (:require #?@(:clj  [[clojure.core.async :as async]
                       [clojure.tools.logging.readable :as log] ; readable variants use pr-str automatically
                       [fluree.db.util.core :refer [if-cljs]]]
                :cljs [[goog.log :as glog]
                       [fluree.db.util.core :refer-macros [if-cljs]]]))
  #?(:cljs (:require-macros [fluree.db.util.log :refer
                             [debug->val debug->>val debug-async->vals
                              debug-async->>vals]]))
  #?(:cljs (:import [goog.debug Console]
                    [goog.log Level])))

#?(:clj (set! *warn-on-reflection* true))


#?(:cljs
   (def levels {:severe  Level.SEVERE
                :warning Level.WARNING
                :info    Level.INFO
                :config  Level.CONFIG
                :fine    Level.FINE
                :finer   Level.FINER
                :finest  Level.FINEST}))


#?(:cljs
   (def logger
     (glog/getLogger "app" (:info levels))))


#?(:cljs
   (defn log-to-console! []
     (.setCapturing (Console.) true)))


#?(:cljs
   (defn set-level! [level]
     (glog/setLevel logger (get levels level (:info levels)))))


#?(:cljs
   (defn fmt [msgs]
     (apply str (interpose " " (map pr-str msgs)))))


#?(:cljs
   (defn log [logger level args]
     (if (instance? js/Error (first args))
       (glog/log logger (get levels level) (-> args rest fmt) (first args))
       (glog/log logger (get levels level) (fmt args) nil))))


#?(:clj
   (defmacro error
     {:arglists '([message & more] [throwable message & more])}
     [& args]
     `(if-cljs
          (log logger :severe ~(vec args))
          (log/logp :error ~@args))))

#?(:clj
   (defmacro warn
     {:arglists '([message & more] [throwable message & more])}
     [& args]
     `(if-cljs
          (log logger :warning ~(vec args))
          (log/logp :warn ~@args))))

#?(:clj
   (defmacro info
     {:arglists '([message & more] [throwable message & more])}
     [& args]
     `(if-cljs
          (log logger :info ~(vec args))
          (log/logp :info ~@args))))

#?(:clj
   (defmacro debug
     {:arglists '([message & more] [throwable message & more])}
     [& args]
     `(if-cljs
          (log logger :fine ~(vec args))
          (log/logp :debug ~@args))))

#?(:clj
   (defmacro trace
     {:arglists '([message & more] [throwable message & more])}
     [& args]
     `(if-cljs
          (log logger :finer ~(vec args))
          (log/logp :trace ~@args))))


#?(:cljs
   (set-level! :info))


#?(:cljs
   (log-to-console!))

#?(:clj
   (defmacro debug->>val
     "Logs a ->> threaded value w/ msg (at debug level) and then returns the
     value so it can continue being threaded."
     [msg v]
     `(do
        (debug ~msg ~v)
        ~v)))

#?(:clj
   (defmacro debug->val
     "Logs a -> threaded value w/ msg (at debug level) and then returns the
     value so it can continue being threaded."
     [v msg]
     `(do
        (debug ~msg ~v)
        ~v)))

#?(:clj
   (defmacro debug-async->vals
     "Logs value(s) taken from chan c w/ msg (at debug level) and then returns a
     new channel with the values on it so further async thread ops can consume
     them."
     [c msg]
     `(let [out-ch# (async/chan 100)]
        (async/pipeline-blocking 1 out-ch#
                                 (map (fn [v#] (debug->val v# ~msg))) ~c)
        out-ch#)))

#?(:clj
   (defmacro debug-async->>vals
     "Logs value(s) taken from chan c w/ msg (at debug level) and then returns a
     new channel with the values on it so further async thread ops can consume
     them."
     [msg c]
     `(debug-async->vals ~c ~msg)))

#?(:clj
   (defn with-mdc* [context f]
     (let [original (org.slf4j.MDC/getCopyOfContextMap)]
       (try
         (doseq [[k v] context]
           (org.slf4j.MDC/put (name k) (str v)))
         (f)
         (finally
           (org.slf4j.MDC/clear)
           (when original
             (doseq [[k v] original]
               (org.slf4j.MDC/put k v)))))))

   :cljs
   (defn with-mdc* [_context f]
     (f)))

#?(:clj
   (defmacro with-mdc [context & body]
     `(with-mdc* ~context (fn [] ~@body)))

   :cljs
   (defmacro with-mdc [_context & body]
     `(do ~@body)))