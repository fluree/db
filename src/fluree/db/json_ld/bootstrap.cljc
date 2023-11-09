(ns fluree.db.json-ld.bootstrap
  (:require [clojure.core.async :refer [go]]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defn bootstrap
  "Bootstraps a permissioned JSON-LD db. Returns async channel."
  ([blank-db] (bootstrap blank-db nil))
  ([blank-db initial-tx]
   (if-let [tx (when initial-tx
                 {"@context" "https://ns.flur.ee/ledger/v1"
                  "@graph"   initial-tx})]
     (db-proto/-stage blank-db tx {:bootstrap? true})
     (go blank-db))))

(defn blank-db
  "When not bootstrapping with a transaction, bootstraps initial base set of flakes required for a db."
  [blank-db]
  (let [t           (dec (:t blank-db))
        base-flakes (jld-transact/base-flakes t)]
    (-> blank-db
        #_(update :t dec)
        #_(commit-data/update-novelty base-flakes))))
