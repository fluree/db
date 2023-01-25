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

  Note this should only be called if the db is permissioned, don't call if the
  root user as the results will not come back correctly."
  [{:keys [policy] :as db} flake]
  (go-try
    (let [s         (flake/s flake)
          p         (flake/p flake)
          class-ids (or (get @(:cache policy) s)
                        (let [classes (<? (dbproto/-class-ids
                                            (dbproto/-rootdb db)
                                            (flake/s flake)))]
                          ;; note, classes will return empty list if none found ()
                          (swap! (:cache policy) assoc s classes)
                          classes))
          fns       (keep #(or (get-in policy [:f/view :class % p :function])
                               (get-in policy [:f/view :class % :default :function])) class-ids)]
      (loop [[[async? f] & r] fns]
        ;; return first truthy response, else false
        (if f
          (let [res (if async?
                      (<? (f db flake))
                      (f db flake))]
            (or res
                (recur r)))
          false)))))


(defn allow-flakes?
  "Like allow-flake, but filters a sequence of flakes to only
  allow those whose policy do not return 'false'."
  [db flakes]
  (async/go
    (loop [[flake & r] flakes
           acc []]
      (if flake
        (if (schema-util/is-schema-flake? flake) ;; always allow schema flakes
          (recur r (conj acc flake))
          (let [res (async/<! (allow-flake? db flake))]
            (if (util/exception? res)
              res
              (if res
                (recur r (conj acc flake))
                (recur r acc)))))
        acc))))
