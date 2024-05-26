(ns fluree.db.json-ld.policy
  (:require [fluree.json-ld :as json-ld]))

#?(:clj (set! *warn-on-reflection* true))

(defn parse-policy-identity
  ([opts]
   (parse-policy-identity opts nil))
  ([opts context]
   (when-let [{:keys [role] :as identity} (-> opts
                                              (select-keys [:did :role :credential])
                                              not-empty)]
     (if (and role context)
       (update identity :role json-ld/expand-iri context)
       identity))))


(defprotocol Restrictable
  (wrap-policy [db identity])
  (root [db]))
