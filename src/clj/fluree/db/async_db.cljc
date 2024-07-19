(ns fluree.db.async-db
  (:refer-clojure :exclude [load])
  (:require [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.transact :as transact]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.history :as history]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(defrecord AsyncDB [alias branch commit t db-chan]
  where/Matcher
  (-match-id [_ fuel-tracker solution s-match error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-id fuel-tracker solution s-match error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-match-triple [_ fuel-tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-triple fuel-tracker solution triple error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-match-class [_ fuel-tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-class fuel-tracker solution triple error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-activate-alias [db alias']
    (when (= alias alias')
      db))

  (-aliases [_]
    [alias])

  subject/SubjectFormatter
  (-forward-properties [_ iri select-spec context compact-fn cache fuel-tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (subject/-forward-properties iri select-spec context compact-fn cache fuel-tracker error-ch)
                (async/pipe prop-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      prop-ch))

  (-reverse-property [_ iri reverse-spec compact-fn cache fuel-tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (subject/-reverse-property iri reverse-spec compact-fn cache fuel-tracker error-ch)
                (async/pipe prop-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      prop-ch))

  (-iri-visible? [_ iri]
    (go-try
      (let [db (<? db-chan)]
        (<? (subject/-iri-visible? db iri)))))

  transact/Transactable
  (-stage-txn [_ fuel-tracker context identity annotation raw-txn parsed-txn]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-stage-txn db fuel-tracker context identity annotation raw-txn parsed-txn)))))
  (-merge-commit [_ new-commit]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-merge-commit db new-commit)))))
  (-merge-commit [_ new-commit proof]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-merge-commit db new-commit proof)))))

  indexer/Indexable
  (index [_ changes-ch]
    (go-try
      (let [db (<? db-chan)]
        (<? (indexer/index db changes-ch)))))

  time-travel/TimeTravel
  (datetime->t [_ datetime]
    (go-try
      (let [db (<? db-chan)]
        (<? (time-travel/datetime->t db datetime)))))

  (latest-t [_]
    t)

  (-as-of [_ t]
    (let [db-chan-at-t (async/promise-chan)
          db-at-t      (->AsyncDB alias branch commit t db-chan-at-t)]
      (go
        (try*
          (let [db (<? db-chan)]
            (async/put! db-chan-at-t
                        (time-travel/-as-of db t)))
          (catch* e
            (log/error e "Error in time-traveling database")
            (async/put! db-chan-at-t e))))
      db-at-t))

  history/AuditLog
  (-history [_ context from-t to-t commit-details? error-ch history-q]
    (go-try
      (let [db (<? db-chan)]
        (<? (history/-history db context from-t to-t commit-details? error-ch history-q)))))

  (-commits [_ context from-t to-t error-ch]
    (let [commit-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (history/-commits context from-t to-t error-ch)
                (async/pipe commit-ch)))
          (catch* e
            (log/error e "Error loading database for commit range")
            (>! error-ch e))))
      commit-ch))

  policy/Restrictable
  (wrap-policy [_ policy default-allow? values-map]
    (go-try
     (let [db (<? db-chan)]
       (<? (policy/wrap-policy db policy default-allow? values-map)))))
  (wrap-identity-policy [_ identity default-allow? values-map]
    (go-try
      (let [db (<? db-chan)]
        (<? (policy/wrap-identity-policy db identity default-allow? values-map)))))
  (root [_]
    (let [root-ch (async/promise-chan)
          root-db (->AsyncDB alias branch commit t root-ch)]
      (go
        (try*
          (let [db (<? db-chan)]
            (async/put! root-ch (policy/root db)))
          (catch* e
            (log/error e "Error loading db while setting root policy")
            (async/put! root-ch e))))
      root-db)))

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

(defn deref-async
  [^AsyncDB async-db]
  (:db-chan async-db))

(defn load
  [conn ledger-alias branch commit-jsonld]
  (let [commit-map (commit-data/jsonld->clj commit-jsonld)
        t          (-> commit-map :data :t)
        async-db   (->AsyncDB ledger-alias branch commit-map t (async/promise-chan))]
    (go
      (let [db (<! (flake-db/load conn ledger-alias branch [commit-jsonld commit-map]))]
        (deliver! async-db db)))
    async-db))
