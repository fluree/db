(ns flureenjs
  (:require
    [cljs.compiler :as compiler]
    [cljs.core :as cljs]
    [cljs.env :as env]
    [clojure.edn :as edn]))

(defmacro analyzer-state [[_ ns-sym]]
  `'~(get-in @env/*compiler* [:cljs.analyzer/namespaces ns-sym]))

(defn- to-property [sym]
  (symbol (str "-" sym)))

(defmacro goog-extend [type base-type ctor & methods]
  `(do
     (defn ~type ~@ctor)

     (goog/inherits ~type ~base-type)

     ~@(map
         (fn [method]
           `(set! (.. ~type -prototype ~(to-property (first method)))
                  (fn ~@(rest method))))
         methods)))

(defmacro version []
  (let [deps (-> "deps.edn" slurp edn/read-string)]
    (get-in deps [:aliases :mvn/version])))
