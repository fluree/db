(ns fluree.db.conn.memory
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defrecord MemoryConnection [id transactor? parallelism memory close]

  storage/Store
  (read [_ k]
    (throw (ex-info (str "Memory connection does not support storage reads. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (write [_ k data]
    (throw (ex-info (str "Memory connection does not support storage writes. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (exists? [_ k]
    (throw (ex-info (str "Memory connection does not support storage exists?. Requested key: " k)
                    {:status 500 :error :db/unexpected-error})))
  (rename [_ old-key new-key]
    (throw (ex-info (str "Memory connection does not support storage rename. Old/new key: " old-key new-key)
                    {:status 500 :error :db/unexpected-error})))

  index/Resolver
  (resolve
    [conn node]
    ;; all root index nodes will be empty
    (storage/resolve-empty-leaf node))

  #?@(:clj
      [full-text/IndexConnection
       (open-storage [conn network dbid lang]
         (throw (ex-info "Memory connection does not support full text operations."
                         {:status 500 :error :db/unexpected-error})))]))


(defn connect
  "Creates a new memory connection."
  []
  (let [conn-id     (str (util/random-uuid))
        parallelism 4
        close-fn    (constantly (log/info (str "Memory Connection " conn-id " Closed")))]
    (map->MemoryConnection {:transactor? false
                            :id          conn-id
                            :parallelism parallelism
                            :close       close-fn
                            :memory      true})))