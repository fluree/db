(ns fluree.db.json-ld.policy
  (:require [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))

(def root-policy-map
  "Base policy (permissions) map that will give access to all flakes."
  {const/iri-view   {:root? true}
   const/iri-modify {:root? true}})

(defn root-db
  [db]
  (assoc db :policy root-policy-map))

(defprotocol Restrictable
  (wrap-policy [db policy-rules default-allow? values-map])
  (wrap-identity-policy [db identity default-allow? values-map])
  (root [db]))
