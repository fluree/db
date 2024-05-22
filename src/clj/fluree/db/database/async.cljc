(ns fluree.db.database.async
  (:refer-clojure :exclude [load])
  (:require [fluree.db.db.json-ld :as jld-db]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.indexer :as indexer]
            [clojure.core.async :as async :refer [<! go]]
            [fluree.db.util.core :as util :refer [get-first get-first-value]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.json-ld.transact :as transact]
            [fluree.db.query.json-ld.response :as jld-response]
            [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defrecord AsyncDB [alias branch t db-chan]
  where/Matcher
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
      match-ch))


  jld-response/NodeFormatter
  (-forward-properties [_ iri select-spec context compact-fn cache fuel-tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (let [db (<! db-chan)]
          (-> db
              (jld-response/-forward-properties iri select-spec context compact-fn cache fuel-tracker error-ch)
              (async/pipe prop-ch))))
      prop-ch))

  (-reverse-property [_ iri reverse-spec compact-fn cache fuel-tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (let [db (<! db-chan)]
          (-> db
              (jld-response/-reverse-property iri reverse-spec compact-fn cache fuel-tracker error-ch)
              (async/pipe prop-ch))))
      prop-ch))


  transact/Transactable
  (-stage-txn [_ fuel-tracker context identity annotation raw-txn parsed-txn]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-stage-txn db fuel-tracker context identity annotation raw-txn parsed-txn)))))


  indexer/Indexed
  (collect [_ changes-ch]
    (go-try
      (let [db (<? db-chan)]
        (<? (indexer/collect db changes-ch))))))


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
  [conn ledger-alias branch commit-jsonld]
  (let [t (-> commit-jsonld
              (get-first const/iri-data)
              (get-first-value const/iri-t))
        async-db (->AsyncDB ledger-alias branch t (async/promise-chan))]
    (go
      (let [db (<! (jld-db/load conn ledger-alias branch commit-jsonld))]
        (deliver! async-db db)))
    async-db))
