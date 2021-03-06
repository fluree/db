(ns fluree.db.query.block
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.permissions-validate :as perm-validate]
            #?(:clj  [clojure.core.async :refer [>! <! >!! <!! go chan buffer close! thread
                                                 alts! alts!! timeout] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]))

(defn- filter-block-flakes
  "Applies filter(s) to flakes in a block"
  [db block]
  (let [root? (-> db :permissions :root?)]
    (async/go
      (try*
        (if root?
          block
          {:block  (:block block)
           :t      (:t block)
           :flakes (<? (perm-validate/allow-flakes? db (:flakes block)))})
        (catch* e
                (log/error (str "Unable to validate permissions on block " (:block block) " error: " e))
                {:block (:block block)
                 :t     (:t block)
                 :flakes []})))))

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
            res #?(:cljs (if (identical? "nodejs" cljs.core/*target*)
                           (<? (filter-block-flakes db res))
                           res)  ;; browser: always allow for now
                   :clj (<? (filter-block-flakes db res)))
            acc'        (concat acc [res])]
        (if (or (= next-block last-block) (util/exception? res))
          acc'
          (if reverse?
            (recur db reverse? (dec next-block) acc')
            (recur db reverse? (inc next-block) acc')))))))


