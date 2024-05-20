(ns fluree.db.database.async
  (:require [fluree.db.database :as database :refer [Database]]
            [fluree.db.db.json-ld :as jld-db]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.db.query.exec.where :as where]
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
                        {:status 500 :error :db/not-delivered})))))

  where/Searchable
  (-match-id [_ fuel-tracker solution s-match error-ch]
    (let [match-ch (async/chan)]
      (go
        (let [db (<! db-chan)]
          (-> db
              (where/-match-id fuel-tracker solution s-match error-ch)
              (async/pipe match-ch))))
      match-ch))

  (-match-triple [_ fuel-tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (let [db (<! db-chan)]
          (-> db
              (where/-match-triple fuel-tracker solution triple error-ch)
              (async/pipe match-ch))))
      match-ch))

  (-match-class [_ fuel-tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (let [db (<! db-chan)]
          (-> db
              (where/-match-class fuel-tracker solution triple error-ch)
              (async/pipe match-ch))))
      match-ch)))

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

(defn load
  [conn ledger-alias branch t commit]
  (let [async-db (->AsyncDB ledger-alias branch t (async/promise-chan))]
    (go
      (let [db (<! (jld-db/load conn ledger-alias branch commit))]
        (deliver! async-db db)))
    async-db))
