(ns fluree.db.dbfunctions.ctx
  (:require [fluree.db.util.json :as json]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.dbfunctions.fns :refer [extract]]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.util.core :as util]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            #?(:cljs [fluree.db.flake :refer [Flake]])
            [fluree.db.dbproto :as dbproto])
  #?(:clj (:import (fluree.db.flake Flake))))

;; Handles context

(defn ctx-flakes->k+fn
  "Iterates over ctx-flakes to extract context key and context fn subject id as two-tuple"
  [ctx-flakes]
  (when (seq ctx-flakes)
    (loop [[^Flake f & r] ctx-flakes
           k nil
           v nil]
      (if (and k v)
        [k v]
        (when f
          (cond
            (= const/$_ctx:key (.-p f))
            (recur r (.-o f) v)

            (= const/$_ctx:fn (.-p f))
            (recur r k (.-o f))

            :else
            (recur r k v)))))))


(defn resolve-ctx
  "Resolves value for a single ctx for a given user."
  [db-root ?ctx ctx-sid]
  (go-try
    (let [ctx-flakes (<? (query-range/index-range db-root :spot = [ctx-sid]))
          [k fn-sid] (ctx-flakes->k+fn ctx-flakes)
          ctx-fn-str (some-> (<? (query-range/index-range db-root :spot = [fn-sid const/$_fn:code]))
                             first
                             (#(.-o ^Flake %)))
          f          (when ctx-fn-str
                       (<? (dbfunctions/parse-fn db-root ctx-fn-str "functionDec")))
          result     (when f (extract (f ?ctx)))
          result*    (if (sequential? result)
                       (set result)
                       result)]
      (if k
        [k result*]
        (do
          (log/warn (str "Context being executed but no corresponding key to set value at for: "
                         (:network db-root) "/" (:dbid db-root) " ctx-subject _id is: " ctx-sid
                         " and function being executed is: " ctx-fn-str "."))
          [])))))


(defn build
  [db-root auth-id roles]
  (go-try
    (let [?ctx {:auth_id auth-id
                :instant (util/current-time-millis)
                :db      db-root
                :state   (atom {:stack   []
                                :credits 10000000
                                :spent   0})}]
      (loop [[role & r] roles
             ctx {}]
        (if role
          (let [role-ctx-chs (some->> (<? (query-range/index-range db-root :spot = [role const/$_role:ctx]))
                                      not-empty
                                      (mapv #(resolve-ctx db-root ?ctx (.-o ^Flake %))))]
            (if role-ctx-chs
              (recur r (loop [[role-ctx-ch & r*] role-ctx-chs
                              ctx* ctx]
                         (if role-ctx-ch
                           (let [[k v] (<? role-ctx-ch)]
                             (recur r* (assoc ctx* k v)))
                           ctx*)))
              ctx))
          ctx)))))
