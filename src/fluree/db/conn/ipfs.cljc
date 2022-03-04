(ns fluree.db.conn.ipfs
  (:require [fluree.db.storage.core :as storage]
            [fluree.db.index :as index]
            [fluree.db.util.core :as util]
            #?(:clj [fluree.db.full-text :as full-text])
            [fluree.db.util.log :as log]
            [fluree.db.conn.json-ld-proto :as jld-proto]
            [fluree.db.method.ipfs.core :as ipfs]
            [fluree.db.util.async :refer [<? go-try channel?]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.conn.state-machine :as state-machine]))

#?(:clj (set! *warn-on-reflection* true))

;; IPFS Connection object

;; message format
(comment
  {:type   nil                                              ;; message type
   :ledger nil
   :res-ch nil                                              ;; optional response/result channel
   ;; body/contents of message
   :body   nil}

  )


(defrecord IPFSConnection [id transactor? memory state
                           context did
                           local-read local-write
                           read write push
                           parallelism close-fn
                           msg-in-ch msg-out-ch]

  jld-proto/Commit
  (c-read [_ commit-key] (read commit-key))
  (c-write [_ commit-data] (write commit-data))

  jld-proto/NameService
  (push [this commit-id] (push commit-id))
  (push [this commit-id ledger] (push commit-id ledger))
  (pull [this ledger] :TODO)
  (subscribe [this ledger] :TODO)

  jld-proto/ConnService
  (close [_]
    (when (fn? close-fn)
      (close-fn))
    (swap! state assoc :closed? true))
  (closed? [_] (boolean (:closed? @state)))
  (method [_] :ipfs)
  (parallelism [_] parallelism)
  (transactor? [_] transactor?)
  (id [_] id)
  (read-only? [_] (not (fn? write)))                        ;; if no commit fn, then read-only
  (context [_] context)
  (did [_] did)
  (msg-in [_ msg] (go-try
                    ;; TODO - push into state machine
                    true

                    ))
  (msg-out [_ msg] (go-try
                     ;; TODO - register/submit event
                     true

                     ))
  (state [_] @state)
  (state [_ ledger] (get @state ledger))

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

#_{:did     did
   :method  :ipfs
   :context context
   :write   nil                                             ;; when empty, don't write unless you commit
   :read    (ipfs/default-read-fn server)
   :commit  (ipfs/default-commit-fn server)
   :push    [(ipfs/default-push-fn server)]}

(defn connect
  "Creates a new memory connection."
  [{:keys [server context did local-read local-write parallelism] :as opts}]
  (let [conn-id  (str (util/random-uuid))
        read     (ipfs/default-read-fn server)
        write    (ipfs/default-commit-fn server)
        push     (ipfs/default-push-fn server)
        state    (state-machine/blank-state)
        close-fn (constantly (log/info (str "Memory Connection " conn-id " Closed")))]
    ;; TODO - need to set up monitor loops for async chans
    (map->IPFSConnection {:id          conn-id
                          :transactor? false
                          :context     context
                          :did         did
                          :local-read  local-read
                          :local-write local-write
                          :read        read
                          :write       write
                          :push        push
                          :parallelism parallelism
                          :msg-in-ch   (async/chan)
                          :msg-out-ch  (async/chan)
                          :close       close-fn
                          :memory      true
                          :state       state})))