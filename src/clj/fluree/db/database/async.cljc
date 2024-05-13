(ns fluree.db.database.async
  (:require [fluree.db.database :as database :refer [Database]]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defrecord AsyncDB [alias branch t db-chan]
  Database
  (query [_ q]
    (go-try
      (if-let [db (<? db-chan)]
        (<? (database/query db q))
        (throw (ex-info (str "Database for " alias "/" branch " at `t` = " t " not delivered.")
                        {:status 500 :error :db/not-delivered})))))

  (stage [_ tx]
    (go-try
      (if-let [db (<? db-chan)]
        (<? (database/stage db tx))
        (throw (ex-info (str "Database for " alias "/" branch " at `t` = " t " not delivered.")
                        {:status 500 :error :db/not-delivered}))))))

(def ^String label "#fluree/AsyncDB ")

(defn display
  [db]
  (select-keys db [:alias :branch :t]))

#?(:cljs
   (extend-type AsyncDB
     IPrintWithWriter
     (-pr-writer [db w _opts]
       (-write w label)
       (-write w (-> db display pr)))))

#?(:clj
   (defmethod print-method AsyncDB [^AsyncDB db, ^Writer w]
     (.write w label)
     (binding [*out* w]
       (-> db display pr))))

(defmethod pprint/simple-dispatch AsyncDB
  [db]
  (print label)
  (-> db display pprint))

(defn deliver!
  [^AsyncDB async-db db]
  (-> async-db
      :db-chan
      (async/put! db)))

(defn ->db
  [alias branch t]
  (->AsyncDB alias branch t (async/promise-chan)))
