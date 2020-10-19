(ns fluree.db.api.ledger
  (:require [fluree.db.session :as session]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.permissions :as permissions]
            [fluree.db.auth :as auth]
            [fluree.db.time-travel :as time-travel]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])))


(defn root-db
  "Returns a queryable database from the connection for the specified ledger."
  ([conn ledger]
   (session/db conn ledger nil))
  ([conn ledger opts]
   (if-let [block (:block opts)]
     (let [pc (async/promise-chan)]
       (async/go
         (async/put! pc (try*
                          (-> (<? (session/db conn ledger nil))
                              (time-travel/as-of-block block)
                              (<?))
                          (catch* e e)))
         (async/close! pc))
       pc)
     ;; session/db returns a promise channel
     (session/db conn ledger nil))))


(defn db
  "Returns a queryable database from the connection for the specified ledger."
  ([conn ledger]
   (root-db conn ledger nil))
  ([conn ledger opts]
   (let [pc (async/promise-chan)]
     (async/go
       (try
         (let [rootdb        (<? (session/db conn ledger nil))
               {:keys [roles auth block]} opts
               auth_id       (when (and auth (not= 0 auth))
                               (or
                                 (<? (dbproto/-subid rootdb auth))
                                 (throw (ex-info (str "Auth id: " auth " unknown.")
                                                 {:status 401
                                                  :error  :db/invalid-auth}))))
               roles         (or roles (if auth_id
                                         (<? (auth/roles rootdb auth_id)) nil))

               permissions-c (when roles (permissions/permission-map rootdb roles :query))
               dbt           (if block
                               (<? (time-travel/as-of-block rootdb block))
                               rootdb)
               dba           (if auth
                               (assoc dbt :auth auth)
                               dbt)
               permdb        (if roles
                               (assoc dba :permissions (<? permissions-c))
                               dba)]
           (async/put! pc permdb))
         (catch Exception e
           (async/put! pc e)
           (async/close! pc))))
     ;; return promise chan immediately
     pc)))
