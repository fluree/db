(ns fluree.db.permissions-validate
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [clojure.core.async :refer [go <!] :as async]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.schema :as schema-util]))

#?(:clj (set! *warn-on-reflection* true))


(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note this should only be called if the db is permissioned, don't call if the root user as the results will
  not come back correctly."
  [{:keys [permissions] :as db} flake]
  (go-try
    (let [s         (flake/s flake)
          p         (flake/p flake)
          class-ids (or (get @(:cache permissions) s)
                        (let [classes (<? (dbproto/-class-ids (dbproto/-rootdb db) (flake/s flake)))]
                          ;; note, classes will return empty list if none found ()
                          (swap! (:cache permissions) assoc s classes)
                          classes))
          fns       (keep #(or (get-in permissions [:f/view :class % p :function])
                               (get-in permissions [:f/view :class % :default :function])) class-ids)]
      (loop [[[async? f] & r] fns]
        ;; TODO - all fns are currently sync - but that will change. Can check for presence of ch? in response, or ideally pass as meta which fns are sync or async
        ;; return first truthy response, else false
        (if f
          (let [res (if async?
                      (<? (f db flake))
                      (f db flake))]
            (or res
                (recur r)))
          false)))))


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
                  (let [res (async/<! (allow-flake? db flake))]
                    (if (util/exception? res)
                      res
                      (if res
                        (recur r (conj acc flake))
                        (recur r acc)))))
                acc))))
