(ns fluree.db.async-db
  (:refer-clojure :exclude [load])
  (:require [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]]
            [clojure.core.async :as async :refer [<! >! go]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.flake.flake-db :as flake-db]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.indexer :as indexer]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.history :as history]
            [fluree.db.time-travel :as time-travel]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log])
  #?(:clj (:import (java.io Writer))))

#?(:clj (set! *warn-on-reflection* true))

(declare deliver!)

(defrecord AsyncDB [alias branch commit t db-chan
                    reindex-min-bytes
                    reindex-max-bytes
                    max-old-indexes]
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
    (let [commit* (assoc commit :index commit-index)
          updated-db (map->AsyncDB {:alias alias
                                    :branch branch
                                    :commit commit*
                                    :t t
                                    :db-chan (async/promise-chan)
                                    :reindex-min-bytes reindex-min-bytes
                                    :reindex-max-bytes reindex-max-bytes
                                    :max-old-indexes max-old-indexes})]
      (go-try
        (let [db (<? db-chan)]
          (deliver! updated-db (dbproto/-index-update db commit-index))))
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

  (-match-properties [_ tracker solution triples error-ch]
    (let [match-ch (async/chan)]
      (go
        (try*
          (let [db (<? db-chan)]
            (-> db
                (where/-match-properties tracker solution triples error-ch)
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

  flake.transact/Transactable
  (-stage-txn [_ tracker context identity author annotation raw-txn parsed-txn]
    (go-try
      (let [db (<? db-chan)]
        (<? (flake.transact/-stage-txn db tracker context identity author annotation raw-txn parsed-txn)))))
  (-merge-commit [_ commit-jsonld commit-data-jsonld]
    (go-try
      (let [db (<? db-chan)]
        (<? (flake.transact/-merge-commit db commit-jsonld commit-data-jsonld)))))

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
          db-at-t      (map->AsyncDB {:alias alias
                                      :branch branch
                                      :commit commit
                                      :t t
                                      :db-chan db-chan-at-t
                                      :reindex-min-bytes reindex-min-bytes
                                      :reindex-max-bytes reindex-max-bytes
                                      :max-old-indexes max-old-indexes})]
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
          root-db (map->AsyncDB {:alias alias
                                 :branch branch
                                 :commit commit
                                 :t t
                                 :db-chan root-ch
                                 :reindex-min-bytes reindex-min-bytes
                                 :reindex-max-bytes reindex-max-bytes
                                 :max-old-indexes max-old-indexes})]
      (go
        (try*
          (let [db (<? db-chan)]
            (async/put! root-ch (policy/root db)))
          (catch* e
            (log/error e "Error loading db while setting root policy")
            (async/put! root-ch e))))
      root-db)))

(defn db?
  [x]
  (instance? AsyncDB x))

(def ^String label "#fluree/AsyncDB ")

(defn display
  [db]
  (select-keys db [:alias :branch :t]))

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
  "Creates an async-db from a flake-db when updating the index. The async db will receive
  the flake db with the updated index reference on the :db-chan promise-chan."
  [{:keys [alias branch commit t reindex-min-bytes reindex-max-bytes max-old-indexes] :as _flake-db}]
  (map->AsyncDB {:alias             alias
                 :branch            branch
                 :commit            commit
                 :t                 t
                 :db-chan           (async/promise-chan)
                 :reindex-min-bytes reindex-min-bytes
                 :reindex-max-bytes reindex-max-bytes
                 :max-old-indexes   max-old-indexes}))

(defn load
  ([ledger-alias branch commit-catalog index-catalog commit-jsonld indexing-opts]
   (let [commit-map (commit-data/jsonld->clj commit-jsonld)]
     (load ledger-alias branch commit-catalog index-catalog commit-jsonld commit-map indexing-opts)))
  ([ledger-alias branch commit-catalog index-catalog commit-jsonld commit-map
    {:keys [reindex-min-bytes reindex-max-bytes max-old-indexes] :as indexing-opts}]
   (let [t        (-> commit-map :data :t)
         ;; Ensure AsyncDB commit reflects index t when an index address exists but :t is missing
         commit-map* (if (and (get-in commit-map [:index :address])
                              (nil? (get-in commit-map [:index :data :t])))
                       (assoc-in commit-map [:index :data :t] t)
                       commit-map)
         async-db    (map->AsyncDB {:alias ledger-alias
                                    :branch branch
                                    :commit commit-map*
                                    :t t
                                    :db-chan (async/promise-chan)
                                    :reindex-min-bytes reindex-min-bytes
                                    :reindex-max-bytes reindex-max-bytes
                                    :max-old-indexes max-old-indexes})]
     (go
       (let [db (<! (flake-db/load ledger-alias commit-catalog index-catalog branch
                                   [commit-jsonld commit-map] indexing-opts))]
         (deliver! async-db db)))
     async-db)))
