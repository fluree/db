(ns fluree.indexer.db
  (:require [fluree.db.db.json-ld :as jld-db]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.store.api :as store]))

(defn create-db-address
  [db]
  ;; TODO: uses tt-id now, would be nice to get a content-addressed id
  (store/address (:conn db) "db" (str (:alias db) "/db/" (or (:tt-id db) "init"))))

(defn status
  "Returns current commit metadata for specified branch (or default branch if nil)"
  [{:keys [state address alias] :as _ledger} requested-branch]
  (let [{:keys [branch branches]} @state
        branch-data (if requested-branch
                      (get branches requested-branch)
                      (get branches branch))
        {:keys [latest-db commit]} branch-data
        {:keys [stats t]} latest-db
        {:keys [size flakes]} stats]
    {:address address
     :alias alias
     :branch branch
     :t (when t (- t))
     :size size
     :flakes flakes
     :commit commit}))

(defrecord DummyLedger []
    ledger-proto/iLedger
    (-status [ledger] (status ledger nil))
    (-status [ledger branch] (status ledger branch)))

(defn state-at-t
  [t stats]
  (atom {:branch :main
         :branches {:main {:name :main
                           :latest-db {:stats (or stats {:size 0 :flakes 0})
                                       :t 0}}}}))

(defn create
  [store {:keys [ledger/name]}]
  (jld-db/create (map->DummyLedger {:method nil
                                    :alias name
                                    :branch :main
                                    :state (state-at-t 0 nil)
                                    ;; resolve-flake-slices looks for a Resolver under :conn
                                    :conn store})))
;; create at t0, update before every tx to hardcode commit t,

(defn prepare
  "Hardcode the branch data so ->tx-state can figure out the next t and stats. This is a
  temporary hack until we can move the branch mechanics to the ledger."
  [{:keys [t stats] :as db}]
  (assoc db :state (state-at-t t stats)))
