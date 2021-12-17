(ns fluree.db.util.log
  (:require #?(:clj  [clojure.tools.logging :as log]
               :cljs [goog.log :as glog]))
  #?(:cljs (:import [goog.debug Console]
                    [goog.log Level])))

#?(:clj (set! *warn-on-reflection* true))


; Added deprecated parameter since compiling in strict mode
#?(:cljs
   (def logger
     (glog/getLogger "app" Level.INFO)))

#?(:cljs
   (def levels {:severe  Level.SEVERE
                :warning Level.WARNING
                :info    Level.INFO
                :config  Level.CONFIG
                :fine    Level.FINE
                :finer   Level.FINER
                :finest  Level.FINEST}))

#?(:cljs
   (defn log-to-console! []
     (.setCapturing (Console.) true)))

#?(:cljs
   (defn set-level! [level]
     (glog/setLevel logger (get levels level (:info levels)))))

(defn fmt [msgs]
  (apply str (interpose " " (map pr-str msgs))))

(defn error [& s]
  #?(:clj  (if (instance? Exception (first s))
             (log/error (first s) (fmt (rest s)))
             (log/error (fmt (rest s))))
     :cljs (glog/error logger (fmt s) nil)))

(defn warn [& s]
  #?(:clj  (log/warn (fmt s))
     :cljs (glog/warning logger (fmt s) nil)))

(defn info [& s]
  #?(:clj  (log/info (fmt s))
     :cljs (glog/info logger (fmt s) nil)))

(defn debug [& s]
  #?(:clj  (log/debug (fmt s))
     :cljs (glog/fine logger (fmt s) nil)))


(defn trace [& s]
  #?(:clj  (log/trace (fmt s))
     :cljs (glog/fine logger (fmt s) nil)))


#?(:cljs
   (set-level! :info))

#?(:cljs
   (log-to-console!))



