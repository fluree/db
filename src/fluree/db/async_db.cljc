(ns fluree.db.async-db
  (:refer-clojure :exclude [load])
  (:require [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.history :as history]
            [fluree.db.query.optimize :as optimize]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.transact :as transact]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(declare ->async-db ->AsyncDB deliver!)

(defrecord AsyncDB [alias commit t db-chan]
  dbproto/IFlureeDb
  (-query [_ tracker query-map]
    (go-try
      (let [db (<? db-chan)]
        (<? (dbproto/-query db tracker query-map)))))
  (-class-ids [_ tracker subject]
    (go-try
      (let [db (<? db-chan)]
        (<? (dbproto/-class-ids db tracker subject)))))
  (-index-update [_ commit-index]
    (let [commit* (-> commit
                      (assoc :index commit-index)
                      (assoc :alias alias))  ; Ensure alias is on commit for nameservice publishing
          updated-db (->async-db alias commit* t)]
      (go
        (try*
          (let [db  (<? db-chan)
                db* (dbproto/-index-update db commit-index)]
            (deliver! updated-db db*))
          (catch* e
            (deliver! updated-db e))))
      updated-db))
  where/Matcher
  (-match-id [_ tracker solution s-match error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-id tracker solution s-match error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-match-triple [_ tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-triple tracker solution triple error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-match-class [_ tracker solution triple error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-class tracker solution triple error-ch)
                (async/pipe match-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      match-ch))

  (-activate-alias [_ alias']
    (go-try
      (let [db (<? db-chan)]
        (<? (where/-activate-alias db alias')))))

  (-aliases [_]
    [alias])

  (-finalize [_ _ _ solution-ch]
    solution-ch)

  subject/SubjectFormatter
  (-forward-properties [_ iri select-spec context compact-fn cache tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (subject/-forward-properties iri select-spec context compact-fn cache tracker error-ch)
                (async/pipe prop-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      prop-ch))

  (-reverse-property [_ iri reverse-spec context tracker error-ch]
    (let [prop-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (subject/-reverse-property iri reverse-spec context tracker error-ch)
                (async/pipe prop-ch)))
          (catch* e
            (log/error e "Error loading database")
            (>! error-ch e))))
      prop-ch))

  (-iri-visible? [_ tracker iri]
    (go-try
      (let [db (<? db-chan)]
        (<? (subject/-iri-visible? db tracker iri)))))

  transact/Transactable
  (-stage-txn [_ tracker context identity author annotation raw-txn parsed-txn]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-stage-txn db tracker context identity author annotation raw-txn parsed-txn)))))
  (-merge-commit [_ commit-jsonld commit-data-jsonld]
    (go-try
      (let [db (<? db-chan)]
        (<? (transact/-merge-commit db commit-jsonld commit-data-jsonld)))))

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

  (sha->t [_ sha]
    (go-try
      (let [db (<? db-chan)]
        (<? (time-travel/sha->t db sha)))))

  (-as-of [_ t]
    (let [db-chan-at-t (async/promise-chan)
          db-at-t      (->AsyncDB alias commit t db-chan-at-t)]
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
  (-history [_ tracker context from-t to-t commit-details? include error-ch history-q]
    (go-try
      (let [db (<? db-chan)]
        (<? (history/-history db tracker context from-t to-t commit-details? include error-ch history-q)))))

  (-commits [_ tracker context from-t to-t include error-ch]
    (let [commit-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (history/-commits context tracker from-t to-t include error-ch)
                (async/pipe commit-ch)))
          (catch* e
            (log/error e "Error loading database for commit range")
            (>! error-ch e))))
      commit-ch))

  policy/Restrictable
  (wrap-policy [_ policy policy-values]
    (go-try
      (let [db (<? db-chan)]
        (<? (policy/wrap-policy db policy policy-values)))))
  (wrap-policy [_ tracker policy policy-values]
    (go-try
      (let [db (<? db-chan)]
        (<? (policy/wrap-policy db tracker policy policy-values)))))
  (root [_]
    (let [root-ch (async/promise-chan)
          root-db (->AsyncDB alias commit t root-ch)]
      (go
        (try*
          (let [db (<? db-chan)]
            (async/put! root-ch (policy/root db)))
          (catch* e
            (log/error e "Error loading db while setting root policy")
            (async/put! root-ch e))))
      root-db))

  optimize/Optimizable
  (-plan [_ parsed-query]
    (go-try
      (let [db (<? db-chan)]
        (<? (optimize/-plan db parsed-query)))))

  (-reorder [_ planned-query]
    (go-try
      (let [db (<? db-chan)]
        (<? (optimize/-reorder db planned-query)))))

  (-explain [_ planned-query]
    (go-try
      (let [db (<? db-chan)]
        (<? (optimize/-explain db planned-query))))))

(defn db?
  [x]
  (instance? AsyncDB x))

(def ^String label "#fluree/AsyncDB ")

(defn display
  [db]
  (select-keys db [:alias :t]))

#?(:clj
   (defmethod print-method AsyncDB [^AsyncDB db, ^Writer w]
     (.write w label)
     (binding [*out* w]
       (-> db display pr)))

   :cljs
   (extend-type AsyncDB
     IPrintWithWriter
     (-pr-writer [db w _opts]
       (-write w label)
       (-write w (-> db display pr)))))

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

(defn ->async-db
  "Creates an async-db.
  This is to be used in conjunction with `deliver!` that will deliver the
  loaded db to the async-db."
  [ledger-alias commit-map t]
  (when-not (:alias commit-map)
    (log/error "Creating AsyncDB with commit missing :alias field!"
               {:ledger-alias ledger-alias
                :commit-id (:id commit-map)
                :commit-keys (keys commit-map)
                :stack-trace (try
                               (throw (ex-info "Stack trace capture" {}))
                               (catch #?(:clj Exception :cljs js/Error) e
                                 #?(:clj (.getStackTrace e)
                                    :cljs (.-stack e))))}))
  (->AsyncDB ledger-alias commit-map t (async/promise-chan)))

(defn load
  ([ledger-alias commit-catalog index-catalog commit-jsonld indexing-opts]
   (let [commit-map (commit-data/jsonld->clj commit-jsonld)]
     (load ledger-alias commit-catalog index-catalog commit-jsonld commit-map indexing-opts)))
  ([ledger-alias commit-catalog index-catalog commit-jsonld commit-map indexing-opts]
   (let [t        (-> commit-map :data :t)
         async-db (->async-db ledger-alias commit-map t)]
     (go
       (let [db (<! (flake-db/load ledger-alias commit-catalog index-catalog
                                   [commit-jsonld commit-map] indexing-opts))]
         (deliver! async-db db)))
     async-db)))
