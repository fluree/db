(ns fluree.db.query.having
  (:require [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.core.async :as async :refer [>! go]])
  (:refer-clojure :exclude [filter]))

(defn filter
  [q error-ch solution-ch]
  (if-let [filter-fn (:having q)]
    (let [filtered-ch (async/chan)]
      (async/pipeline-async 2
                            filtered-ch
                            (fn [solution ch]
                              (go (try* (when (:value (filter-fn solution))
                                          (>! ch solution))
                                        (async/close! ch)
                                        (catch* e
                                                (log/error e "Error applying having function")
                                                (>! error-ch e)))))
                            solution-ch)
      filtered-ch)
    solution-ch))
