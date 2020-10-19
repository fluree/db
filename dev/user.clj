(ns user
  (:require [clojure.tools.namespace.repl :as tn :refer [refresh refresh-all]]
            [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.api :as fdb]
            [fluree.db.flake :as flake]
            [fluree.db.permissions :as permissions]
            [fluree.db.dbfunctions.fns :as dbfunctions]
            [fluree.db.session :as session]
            [fluree.db.constants :as constants]
            [fluree.db.util.json :as json]
            [fluree.db.serde.json :as serdejson]
            [fluree.db.storage.core :as storage]
            [fluree.db.query.fql :as fql]
            [fluree.db.query.range :as query-range]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.query.schema :as schema]
            [clojure.string :as str]
    ;cljs
            [figwheel-sidecar.repl-api :as ra]))


;; async/query todo
;; fluree.db.query.fql/query
;; dbproto/-query
;; api/query
;; fluree.db.dbfunctions.internal/query


;; make sure all of following response are async
;; http-api/db-handler* (transator)


(set! *warn-on-reflection* true)


;; TODO - general
;; if an subject is a component, do we allow it to be explicitly assigned as a ref to another component?
;; If so, do we not retract it? If we do retract it, we need to also retract all other refs
;; For permissions, if the predicate or collection IDs don't exist, we can skip the rule

;; TODO - query
;; Tag queries are always allowed, but we still do a full permissions check. Currently have an exception in
;; graphdb no-filter? but should probably have a separate method that shortcuts range-query
;; Make graphql use standard query path
;; graphQL schema will create a 'collection' based on predicate namespace alone, even if the collection doesn't exist
;; Some sort of defined-size tag lookup cache could make sense. Would need to clear it if a transaction altered any tag


;cljs-stuff
(defn start [] (ra/start-figwheel!))
(defn start-cljs [] (ra/cljs-repl "dev"))
;(defn stop [] (ra/stop-figwheel!))  ;; errs



(comment
  @(fdb/transact (:conn user/system) "mytest/sample3"
                 [{:_id                  "company"
                   "company/name"        "Other6",
                   "company/phoneNumber" "8911232922",
                   "company/address"     "123 Other Rd, Other City, USA",
                   "company/tags"        ["other"],
                   "company/logo"        "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Disk_pack1.svg/1200px-Disk_pack1.svg.png",
                   }])

  @(fdb/transact conn "mytest/$sample3"
                 [{:_id           ["company/name" "Other6c"]
                   "company/name" "Other6d"
                   }])

  @(fdb/transact conn "mytest/$sample3"
                 [{:_id                  ["company/name" "Other6"]
                   "company/name"        "Other6b",
                   "company/phoneNumber" "8911232922",
                   "company/address"     "123 Other Rd, Other City, USA",
                   "company/tags"        ["other"],
                   "company/logo"        "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Disk_pack1.svg/1200px-Disk_pack1.svg.png",
                   }])
  )


; commands to validate cljs (against clj)
(comment
  (def my-conn (fdb/connect "http://localhost:8090"))
  (def my-db (fdb/db my-conn "test/chat"))
  (def my-query {:select ["*"] :from "_collection"})
  (async/<!! (fluree.db.api/query-async my-db my-query))
  ;(async/<!! (fluree.db.api/new-ledger-async my-conn "mytest/db7"))

  ;"[{\"_id\":\"_collection\",\"name\":\"chatty29\"}]"
  (let [my-tx   [{:_id "_collection", :name "chatty29"}]
        results (async/<!! (fluree.db.api/transact-async my-conn "test/chat" my-tx))]
    results
    )

  (async/<!! (fluree.db.api/query-async my-db my-query))
  (fluree.db.connection/listeners (:state my-conn))

  (async/<!! (fluree.db.api/ledger-info-async my-conn "test/chat"))
  (async/<!! (fluree.db.api/ledger-stats-async my-conn "test/chat"))

  (async/<!! (fdb/block-range-with-txn-async my-conn "test/chat" {:start 4 :end 4}))
  (async/<!! (fdb/block-range-with-txn-async my-conn "test/chat" {:start 1}))

  (fdb/close my-conn)
  )

; #FC-100
(comment
  (def my-conn (fluree.db.api/connect "http://localhost:8090"))
  (def my-db (fluree.db.api/db my-conn "test/test"))
  (def my-query {:select ["*"] :from "_collection"})

  (async/<!! (fluree.db.api/query-async my-db my-query))

  (let [my-tx   [{:_id "_collection", :name "chatty03"}]
        results (async/<!! (fluree.db.api/transact-async my-conn "test/test" my-tx))]
    results
    )
  )


(comment

  (def my-conn (fluree.db.api/connect "http://localhost:8090"))
  (def my-db (fluree.db.api/db my-conn "test/password"))

  (let [my-query {:select ["*"] :from "invoice"}]
    (async/<!! (fluree.db.api/query-async my-db my-query)))

  (let [my-query {:select ["*"] :from "invoice" :block 67}]
    (async/<!! (fluree.db.api/query-async my-db my-query)))

  (let [my-query {:select ["*"] :from "invoice" :block "2020-01-22T12:59:36.097Z"}]
    (async/<!! (fluree.db.api/query-async my-db my-query)))

  (let [my-query {:select ["*"] :from "invoice" :block "PT5M"}]
    (async/<!! (fluree.db.api/query-async my-db my-query)))

  (let [my-query {:block 67}]
    (async/<!! (fluree.db.api/block-query-async my-conn "test/password" my-query)))

  (let [my-query {:block "2020-01-22T12:59:36.097Z"}]
    (async/<!! (fluree.db.api/block-query-async  my-conn "test/password" my-query)))

  (let [my-query {:block "PT5M"}]
    (async/<!! (fluree.db.api/block-query-async  my-conn "test/password" my-query)))

  (fluree.db.api/close my-conn)
 )