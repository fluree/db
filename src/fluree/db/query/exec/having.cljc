(ns fluree.db.query.exec.having
  (:require [clojure.core.async :as async :refer [>! go]]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true])
  (:refer-clojure :exclude [filter]))

(defn filter
  [q error-ch solution-ch]
  (if-let [filter-fn (:having q)]
    (let [filtered-ch (async/chan 2 (log/xf-debug! ::query-having {:having (:having q)}))]
      (async/pipeline-async 2
                            filtered-ch
                            (fn [solution ch]
                              (go (try* (when (:value (filter-fn solution))
                                          (>! ch solution))
                                        (async/close! ch)
                                        (catch* e
                                          (log/error! ::having-error e {:msg "Error applying having function"})
                                          (log/error e "Error applying having function")
                                          (>! error-ch e)))))
                            solution-ch)
      filtered-ch)
    solution-ch))
