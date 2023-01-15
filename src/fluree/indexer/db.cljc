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
  "Creates an address of the form `fluree:db:<store-type>:<ledger-name>/<tx-summary-address>`."
  ([db ledger-name]
   (create-db-address db ledger-name "init"))
  ([db ledger-name tx-summary-id]
   (store/address (:conn db) "db" (str ledger-name "/t/" tx-summary-id))))

(defn db-path-parts
  "Returns the ledger name from the db-address"
  [db-address]
  (let [path (:address/path (ident/address-parts db-address))
        [ledger-name tx-summary-id] (str/split path #"/")]
    {:ledger/name ledger-name
     :tx/summary-id tx-summary-id}))

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
                           :commit {:alias ""
                                    :v 0
                                    :branch :main
                                    :data {:t (or t 0)}}
                           :latest-db {:stats (or stats {:size 0 :flakes 0})
                                       :t (or t 0)}}}}))

(defn create
  [store ledger-name opts]
  (jld-db/create (map->DummyLedger {:method nil
                                    :network ledger-name
                                    :alias ledger-name
                                    :ledger-id "index"
                                    :branch :main
                                    :state (state-at-t 0 nil)
                                    :indexer (idx-default/create opts)
                                    ;; resolve-flake-slices looks for a Resolver under :conn
                                    :conn store})))
;; create at t0, update before every tx to hardcode commit t,

(defn prepare
  "Hardcode the branch data so ->tx-state can figure out the next t and stats. This is a
  temporary hack until we can move the branch mechanics to the ledger."
  [{:keys [t stats] :as db}]
  (assoc-in db [:ledger :state] (state-at-t (- t) stats)))

(comment
  (def db (create (store/start {:store/method :memory}) {}))
  (-> db
      :commit)
  {:alias "", :v 0, :branch :main, :data {:t 0}}
  :main
  :main
  (:method :alias :branch :state :indexer :conn)

  (:ledger :conn :method :alias :branch :commit :block :t :tt-id :stats :spot :psot :post :opst :tspo :schema :comparators :novelty :permissions :ecount)

  (create-db-address db "dan")
  (ident/address-parts "fluree:db:memory:dan/init/init")
  #:address{:ns "fluree", :type :db, :method :memory, :path "dan/init/init", :id "init"}
  "fluree:db:memory:dan/init/init"


  ,)
