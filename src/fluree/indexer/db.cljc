(ns fluree.indexer.db
  (:require [fluree.db.db.json-ld :as jld-db]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.store.api :as store]
            [fluree.db.indexer.default :as idx-default]
            [clojure.string :as str]
            [fluree.common.identity :as ident]
            [fluree.crypto :as crypto]
            [fluree.json-ld :as json-ld]))

(defn root-index-path
  "Returns the address of the index root, if it exists."
  [db]
  (let [addr (-> db :commit :index :address)
        [_ _ _ path] (str/split addr #": ")]
    path))

(defn create-db-address
  "Creates an address of the form `fluree:db:<store-type>:<ledger-name>/db/<db-block-id>`."
  [db db-block-id]
  (store/address (:conn db) "db" db-block-id))

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
                           :commit {:alias "COMMIT_ALIAS"
                                    :v 0
                                    :branch :main
                                    :data {:t (or t 0)}}
                           :latest-db {:stats (or stats {:size 0 :flakes 0})
                                       :t (or t 0)}}}}))

(defn prepare
  "Hardcode the branch data so ->tx-state can figure out the next t and stats. This is a
  temporary hack until we can move the branch mechanics to the ledger."
  [{:keys [t stats] :as db}]
  (assoc-in db [:ledger :state] (state-at-t (- t) stats)))

(defn update-index-writer-opts
  "Change `reindex-min-bytes` or `reindex-max-bytes` of an existing Db."
  [db opts]
  (let [index-writer-state-atom (-> db :indexer :state-atom)]
    (assoc db :indexer (idx-default/create (merge {:state-atom index-writer-state-atom} opts)))))

(defn create
  [store ledger-name opts]
  (-> (jld-db/create (map->DummyLedger {:branch :main
                                        ;; method is actually :network used in prefix index branch and leaf keys
                                        :method (str ledger-name "/index/")
                                        ;; used as the :ledger-id in index branch and leaf keys
                                        ;; leave it blank because we get the ledger name from network (:method)
                                        ;; (network is a prefix to index node keys, ledger-id is inside the key)
                                        :alias ""
                                        :state (state-at-t 0 nil)
                                        :indexer (idx-default/create opts)
                                        ;; resolve-flake-slices looks for a Resolver under :conn
                                        :conn store}))
      ;; :network is used for the prefix of garbage and root node keys
      (assoc :network (str ledger-name "/index/"))))
