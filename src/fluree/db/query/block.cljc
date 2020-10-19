(ns fluree.db.query.block
  (:require [fluree.db.storage.core :as storage]
            #?(:clj [fluree.db.permissions-validate :as perm-validate])
            #?(:clj  [clojure.core.async :refer [>! <! >!! <!! go chan buffer close! thread
                                                 alts! alts!! timeout] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.core :as util]
            [fluree.db.util.async :refer [<?]]))

;; TODO - for nodejs we need to re-enable permissions for javascript but in a way code only
;; TODO - exists for nodejs compilation
(defn block-range
  "Returns a async channel containing each of the blocks from start (inclusive) to end if provided (inclusive). Should received PERMISSIONED db."
  [db start end opts]
  (async/go
    (loop [db         db
           reverse?   (when end (< end start))
           next-block start
           acc        []]
      (let [{:keys [conn network dbid]} db
            last-block  (or end start)                      ;; allow for nil end-block for now
            res         (<? (storage/block conn network dbid next-block))
            root?       (-> db :permissions :root?)
            ;; Note this bypasses all permissions in CLJS for now!
            res #?(:cljs res                                ;; always allow for now
                   :clj (if root?
                          res
                          {:block  (:block res)
                           :t      (:t res)
                           :flakes (<? (perm-validate/allow-flakes? db (:flakes res)))}))
            acc'        (concat acc [res])]
        (if (or (= next-block last-block) (util/exception? res))
          acc'
          (if reverse?
            (recur db reverse? (dec next-block) acc')
            (recur db reverse? (inc next-block) acc')))))))


