(ns fluree.db.permissions-validate
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.schema :as schema-util])
  #?(:clj (:import (fluree.db.flake Flake))))

;; goal is a quick check if we can not check every result. Many queries are small,
;; so an extended effort in here is not worth the time, we can just check each result
(defn no-filter?
  "Quick check if we can skip filtering."
  [permissions s1 s2 p1 p2]
  (or (true? (:root? permissions))
      (and s1 s2
           ;; always allow a tag query
           (= 3 (flake/sid->cid s1) (flake/sid->cid s2)))))

(defn process-functions
  [^Flake flake functions db permissions]
  (async/go
    (let [root-db (dbproto/-rootdb db)
          sid     (.-s flake)
          ctx     {:sid     sid
                   :auth_id (or (:auth db) (:auth permissions))
                   :db      root-db
                   :state   (atom {:stack   []
                                   :credits 10000000
                                   :spent   0})}]
      (loop [[f & r] functions]
        (if f
          (let [res (try*
                      (let [result (f ctx)]
                        (if (channel? result)
                          (<? result)
                          result))
                      (catch* e
                              (log/warn "Caught exception in database function: "
                                        (:fnstr (meta f)) ": " #?(:clj (.getMessage e) :cljs (str e)))
                              (log/error e)
                              (ex-info
                                (str "Caught exception in database function: "
                                     (:fnstr (meta f)) ": " #?(:clj (.getMessage e) :cljs (str e)))
                                {:status 400
                                 :error  :db/db-function-error})))]
            (if res
              true
              (recur r)))
          false)))))


(defn check-explicit-functions
  [^Flake flake db permissions fns-paths]
  (async/go
    (let [trace? (:trace? permissions)]
      (loop [[f & r] fns-paths
             ;; if any explicit predicate exists, we will not check defaults - else we check defaults
             check-defaults? true]
        (let [funs   (get-in permissions f)
              result (when (not (nil? funs))
                       (if (boolean? funs)
                         funs
                         (async/<! (process-functions flake funs db permissions))))
              ;; if we ever find a function explicitly assigned, don't check for
              ;; collection defaults
              check-defaults?* (if (nil? funs) check-defaults? false)]
          (cond
            ;; exception
            (util/exception? result) result

            ;; any truthy value means flake is allowed, don't check defaults
            result
            [true false]

            ;; nothing left to check, cannot see flake
            (empty? r)
            [false check-defaults?*]

            :else
            (recur r check-defaults?*)))))))


(defn root-permission?
  "Returns true for root db permissions."
  [permissions]
  (true? (:root? permissions)))


(defn allow-flake?
  "Returns either:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed"
  ([db flake] (allow-flake? db flake (:permissions db)))
  ([db flake permissions]
   (async/go
     (if (root-permission? permissions)
       true
       (let [cid       (flake/sid->cid (.-s flake))
             pid       (.-p flake)
             fns-paths [[:collection cid pid] [:collection cid :all] [:predicate pid]]
             check     (async/<! (check-explicit-functions flake db permissions fns-paths))]
         (if (util/exception? check)
           check
           (let [[result check-defaults?] check]
             (if check-defaults?
               (first                                       ;; returns two-tuple
                 (async/<!
                   (check-explicit-functions flake db permissions [[:collection cid :default]
                                                                   [:collection :default]])))
               result))))))))


(defn allow-flakes?
  "Like allow-flake, but filters a sequence of flakes to only allow those
  that are whose permissions do not return 'false'."
  [db flakes]
  ;; check for root access, in which case we can short-circuit and return true.
  (async/go
    (loop [[flake & r] flakes
           acc []]
      (if flake (if (schema-util/is-schema-flake? flake)    ;; always allow schema flakes
                  (recur r (conj acc flake))
                  (let [res (async/<! (allow-flake? db flake (:permissions db)))]
                    (if (util/exception? res)
                      res
                      (if res
                        (recur r (conj acc flake))
                        (recur r acc)))))
                acc))))

