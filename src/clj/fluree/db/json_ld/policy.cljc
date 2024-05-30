(ns fluree.db.json-ld.policy
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.constants :as const]))

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

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

(defn root
  [db]
  (assoc db :policy root-policy-map))

(defprotocol Restrictable
  (wrap-policy [db policy-rules default-allow? values-map])
  (wrap-identity-policy [db identity default-allow? values-map])
  (root [db]))
