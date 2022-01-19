(ns fluree.db.util.log
  (:require #?@(:clj  [[clojure.tools.logging.readable :as log] ; readable variants use pr-str automatically
                       [fluree.db.util.core :refer [if-cljs]]]
                :cljs [[goog.log :as glog]
                       [fluree.db.util.core :refer-macros [if-cljs]]]))
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


(defmacro error
  {:arglists '([message & more] [throwable message & more])}
  [& args]
  `(if-cljs
     (log logger :error ~(vec args))
     (log/logp :error ~@args)))


(defmacro warn
  {:arglists '([message & more] [throwable message & more])}
  [& args]
  `(if-cljs
     (log logger :warning ~(vec args))
     (log/logp :warn ~@args)))


(defmacro info
  {:arglists '([message & more] [throwable message & more])}
  [& args]
  `(if-cljs
     (log logger :info ~(vec args))
     (log/logp :info ~@args)))


(defmacro debug
  {:arglists '([message & more] [throwable message & more])}
  [& args]
  `(if-cljs
     (log logger :fine ~(vec args))
     (log/logp :debug ~@args)))


(defmacro trace
  {:arglists '([message & more] [throwable message & more])}
  [& args]
  `(if-cljs
     (log logger :finer ~(vec args))
     (log/logp :trace ~@args)))


#?(:cljs
   (set-level! :info))


#?(:cljs
   (log-to-console!))
