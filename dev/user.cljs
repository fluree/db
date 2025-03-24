(ns user)

; commands to test cljs
(comment

  (fluree.db.util.log/set-level! :finest)
  (def my-conn (flureedb/connect "http://localhost:8090"))
  (def my-db (flureedb/db my-conn "test/chat"))
  (def my-query {:select ["*"] :from "_collection"})

  (-> (flureedb/ledger-stats my-conn "test/chat")
      (.then #(print %))
      (.catch #(print (str "Error: " %))))

  (let [block-map {:start 1 :end 5}]
    (-> (flureedb/block-range-with-txn my-conn "test/chat" block-map)
        (.then #(print %))
        (.catch #(print (str "Error: " %)))))

  ; returns transaction id to be used in a query
  (-> (flureedb/new-ledger my-conn "test/subcontract")
      (.then #(print %))
      (.catch #(print "Error" %)))

  ; Invalid database - no error
  ;Timeout of 6000ms reached without transaction being included in new block.
  (-> (flureedb/monitor-tx my-conn
                           "test/subcontract"
                           "e8437ff56c4fd0904f8ae618319afd71d30e983373e86d554b4ebb480d796739"
                           6000)
      (.then #(print "response is: " %))
      (.catch #(print %)))

  (-> (flureedb/delete-ledger my-conn "test/subcontract")
      (.then #(print %))
      (.catch #(print (str "Error: " %))))

  (-> (flureedb/q my-db (clj->js my-query))
      (.then #(print "response is" %))
      (.catch #(print "Error" %)))

  (flureedb/listeners my-conn)

  (-> (flureedb/db-schema my-db)
      (.then #(print %))
      (.catch #(print (str "Error: " %))))

  (let [my-tx [{:_id "_collection"
                :name "categories"}]]
    (-> (flureedb/transact my-conn "test/jinkies" (clj->js my-tx))
        (.then #(print %))
        (.catch #(print %))))

  (let [block-map {:start 5 :end 6}]
    (-> (flureedb/block-range-with-txn my-conn "test/chat" (clj->js block-map))
        (.then #(print %))
        (.catch #(print (str "Error: " %)))))

  (let [my-block-query {:block [5 6]}]
    (-> (flureedb/block-query my-conn "test/chat" (clj->js my-block-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-block-query {:block [1] :pretty-print true}]
    (-> (flureedb/block-query my-conn "test/chat" (clj->js my-block-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-history-query {:history ["person/handle", "zsmith"]}
        my-db            (flureedb/db my-conn "test/chat")]
    (-> (flureedb/history-query my-db (clj->js my-history-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-history-query {:history [351843720888322]  :pretty-print true}
        my-db            (flureedb/db my-conn "test/chat")]
    (-> (flureedb/history-query my-db (clj->js my-history-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-multi-query {"collections" {:select ["*"]
                                       :from "_collection"}
                        "persons" {:select ["*"]
                                   :from "person"}}
        my-db          (flureedb/db my-conn "test/chat")]
    (-> (flureedb/multi-query my-db (clj->js my-multi-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  ;(let [my-query "{\"query\":\"{ graph {\\n  chat {\\n    _id\\n    comments\\n    instant\\n    message\\n    person\\n  }\\n}\\n}\",\"variables\":null,\"operationName\":null}"]
  ;  (-> (flureedb/graphql my-conn "test/chat" my-query)
  ;      (.then #(print %))
  ;      (.catch #(print "Error: " %))))

  ;(let [my-query "SELECT ?chat ?message ?person ?instant ?comments
  ;WHERE {
  ;?chat fd:chat/message  ?message;
  ;      fd:chat/person   ?person;
  ;      fd:chat/comments ?comments;
  ;      fd:chat/instant  ?instant. }"
  ;      my-query-encoded (fluree.db.util.json/stringify my-query)
  ;      my-db   (flureedb/db my-conn "test/chat")
  ;      ]
  ;  (-> (flureedb/sparql my-db my-query-encoded)
  ;      (.then #(print %))
  ;      (.catch #(print "Error: " %))))

  ;(let [my-query "SELECT ?collection \nWHERE { \n  ?collectionID fdb:_collection/name ?collection. \n  }"
  ;      my-query-encoded (fluree.db.util.json/stringify my-query)
  ;      my-db   (flureedb/db my-conn "test/test")
  ;      ]
  ;  (-> (flureedb/sparql my-db my-query-encoded)
  ;      (.then #(print %))
  ;      (.catch #(print "Error: " %))))
  )
; transactions wo/signatures
(comment

  (def my-conn (flureedb/connect "http://localhost:8090"))

  (let [my-tx [{:_id "_collection"
                :name "categories"}]]
    (-> (flureedb/transact my-conn "test/jinkies" (clj->js my-tx))
        (.then #(print %))
        (.catch #(print %))))

  (flureedb/close my-conn))

; open-api = false
(comment

  ;;
  ;Public Key	0326173c52d698dfa6ee5ab71482eb4a6fa8ca15b107a79e1cd28b9abce522afc1
  ;Private Key	5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d
  ;Auth ID	TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY
  ;[
  ; {
  ;  :_id "_auth",
  ;  :id  "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY",
  ;  :roles [["_role/id","root"]]
  ;  }
  ; ]

  ; open-api = false
  ; 401 error returned because query is not signed
  (let [my-query (clj->js {:select ["*"] :from "_collection" :block 1})]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (let [my-db (flureedb/db my-conn "test/chat")]
                   (do
                     (print "the :flureedb-settings of my connection are " (:flureedb-settings my-conn))
                     (-> (flureedb/q my-db my-query)
                         (.then (fn [qry-results] (do
                                                    (print "the results are " qry-results)
                                                    qry-results)))
                         (.catch #(print "Error" %)))))))
        (.catch (fn [error] (print "the error is" error)))))

  ;; open-api = true/false; signed-query works
  (def the-conn (flureedb/connect "http://localhost:8090"))
  (let [my-query    (clj->js {:select ["*"] :from "_collection"})
        auth        "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY"
        private-key "5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d"
        my-opts     (clj->js {:private-key private-key :auth auth :timeout 600000})
        ledger      "test/chat"]
    (-> (flureedb/signed-query the-conn ledger my-query my-opts)
        (.then (fn [qry-results] (do
                                   (print "the results are " qry-results)
                                   qry-results)))
        (.catch #(print "Error" %))))
  (flureedb/close the-conn)

;; .net using pattern - example 1
  (let [my-query (clj->js {:select ["*"] :from "_collection" :block 1})]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (let [my-db (flureedb/db my-conn "test/chat")]
                   (-> (flureedb/q my-db my-query)
                       (.then (fn [qry-results]
                                (do
                                  (print "closing the connection..." (flureedb/close my-conn))
                                  (print "the results are " qry-results)
                                  qry-results)))
                       (.catch (fn [error]
                                 (do
                                   (print "Error" error)
                                   (print "closing the connection..." (flureedb/close my-conn)))))))))
        (.catch (fn [error] (print "the error is" error)))))

  ;; .net using pattern - example 2
  (let [my-query    (clj->js {:select ["*"] :from "_collection"})
        auth        "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY"
        private-key "5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d"
        my-opts     (clj->js {:private-key private-key :auth auth :timeout 600000})
        ledger      "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/signed-query my-conn ledger my-query my-opts)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error)))))

  ;; block query - unsigned
  (let [my-query (clj->js {:block 1})
        ledger   "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/block-query my-conn ledger my-query)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error)))))

  ;; block query - signed (invalid auth: 401)
  (let [my-query    (clj->js  {:block 1})
        auth        "Tf8FLEAryuQ6Y6wohBiGFRQLgWnr1QPTwvy"
        private-key "2403db98d5ecf115b0acc218621d292c8e3ca21a7c26b9f9e94c1ab2544fb2d7"
        my-opts     (clj->js  {:private-key private-key :auth auth :timeout 600000})
        ledger      "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/block-query my-conn ledger my-query my-opts)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error)))))

;; block-query - signed w/valid auth
  ;; [auth authority]= [Tf8FLEAryuQ6Y6wohBiGFRQLgWnr1QPTwvy Tf4Kj79GkbvQTgAB7yULwdc39vRZMba76u7]
  (let [my-query    (clj->js {:block 1})
        auth        "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY"
        private-key "5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d"
        my-opts     (clj->js {:private-key private-key :auth auth :timeout 600000})
        ledger      "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/block-query my-conn ledger my-query my-opts)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error)))))

;; multi-query
  (let [my-query    (clj->js {"collections" {:select ["*"]
                                             :from "_collection"}
                              "persons" {:select ["*"]
                                         :from "person"}})
        auth        "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY"
        private-key "5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d"
        my-opts     (clj->js {:private-key private-key
                              :action      :multi-query
                              :auth        auth
                              :timeout     600000})
        ledger      "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/signed-query my-conn ledger my-query my-opts)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error)))))

  ;; history
  (let [my-query    (clj->js {:history ["person/handle", "zsmith"]})
        auth        "TfDRfWTgmBqGev1Hc94UvZG6QnuxB6KAXtY"
        private-key "5d1a431f6eba9d5fad9ee7e8d05a3da2f8f3649d349c5566b1638946892d132d"
        my-opts     (clj->js {:private-key private-key
                              :action      :history
                              :auth        auth
                              :timeout     600000})
        ledger      "test/chat"]
    (-> (flureedb/connect-p "http://localhost:8090")
        (.then (fn [my-conn]
                 (-> (flureedb/signed-query my-conn ledger my-query my-opts)
                     (.then (fn [qry-results]
                              (do
                                (print "closing the connection..." (flureedb/close my-conn))
                                (print "the results are " qry-results)
                                qry-results)))
                     (.catch (fn [error]
                               (do
                                 (print "Error" error)
                                 (print "closing the connection..." (flureedb/close my-conn))))))))
        (.catch (fn [error] (print "the error is" error))))))

;; time-travel
(comment

  (def my-conn (flureedb/connect "http://localhost:8090"))

  (let [my-db (flureedb/db my-conn "test/password")
        my-query (clj->js {:select ["*"] :from "invoice"})]
    (-> (flureedb/q my-db my-query)
        (.then #(print "response is" %))
        (.catch #(print "Error" %))))

  (let [my-db (flureedb/db my-conn "test/password")
        my-query (clj->js {:select ["*"]
                           :from "invoice"
                           :block "2020-01-22T12:59:36.097Z"})]
    (-> (flureedb/q my-db my-query)
        (.then #(print "response is" %))
        (.catch #(print "Error" %))))

  (let [my-db (flureedb/db my-conn "test/password")
        my-query (clj->js {:select ["*"] :from "invoice" :block "PT5M"})]
    (-> (flureedb/q my-db my-query)
        (.then #(print "response is" %))
        (.catch #(print "Error" %))))

  (let [my-query (clj->js {:block 10})]
    (-> (flureedb/block-query my-conn "test/password" my-query)
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-query {:block "2020-01-22T12:59:36.097Z"}]
    (-> (flureedb/block-query my-conn "test/password" (clj->js my-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-query  {:block "PT5M"}]
    (-> (flureedb/block-query my-conn "test/password" (clj->js my-query))
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (flureedb/close my-conn))

(comment

  (def my-conn (flureedb/connect "http://localhost:8090"))

  (let [my-db (flureedb/db my-conn "test/password")
        my-query {:select ["*"] :from "invoice"}]
    (-> (flureedb/q my-db (clj->js my-query))
        (.then #(print "response is" %))
        (.catch #(print "Error" %))))

  ;; fails with #object[Error Error: cljs.core/*eval* not bound]
  (let [my-db (flureedb/db my-conn "test/password")
        my-query {:select ["?handle", "?num"]
                  :where [["?person", "person/handle", "?handle"],
                          ["?person", "person/favNums", "?num"]]
                  :filter ["(> 10 ?num)"]}]
    (-> (flureedb/q my-db (clj->js my-query))
        (.then #(print "response is" %))
        (.catch #(print "Error" %))))

  (flureedb/close my-conn))

;password-auth
(comment
  (def my-conn (flureedb/connect "http://localhost:8090"))

  (let [ledger   "test/pwd"
        user     "ldeakwilliams"
        password "fluree"]
    (-> (flureedb/password-login my-conn ledger password user)
        (.then #(print "token is" %))
        (.catch #(print "error! " %))))

  (let [token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ0ZXN0L3B3ZCIsInN1YiI6IlRmS0xvMzh2bmRucGJZOE51REVXcExDbXN2aVFFMlhQaWFlIiwiZXhwIjoxNjIyMDYxNzA3Njc2LCJpYXQiOjE1OTA1MjU3MDc2NzYsInByaSI6IjNjNjFhYzRhODU2ZmMxNjUwZGI4MmM4NWU4ZTJiZjcyNTgyMzdiYjBlZDZiN2FhZjdmZmNhMmEzYWZhNzRiYTdmNjhhOWQ3NTYzMjYwNjk0ZTQxOWVkODQxZmRiNTgzZWNhYTg4MzViNjE3YTE1MDYwNDhmODcxY2JjNjlhZTQxNTE1NDE5OTRmOGJlNzYwMGNkN2M0MmY3NWQ5YmY2NGYifQ.sUfc7EIzcbv39AFv5PZIWq4rOPlIntzBHTLAXCbLqLc"]
    (-> (flureedb/renew-token my-conn token)
        (.then #(print "token is" %))
        (.catch #(print "error! " %))))

  (let [ledger   "test/pwd"
        user     "telly"
        password "fluree"
        auth     "TfCeRPtS6XhY4iPFHJ5SttzDACCuzghwMMa"
        options  {:create-user true}]
    (-> (flureedb/generate-user my-conn ledger password user auth)
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [ledger   "test/pwd"
        user     "ldw1007"
        password "fluree"
        auth     "TfCeRPtS6XhY4iPFHJ5SttzDACCuzghwMMa"
        options  {:create-user false}]
    (-> (flureedb/generate-user my-conn ledger password user auth)
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [ledger   "test/pwd"
        user     "ldeakwilliams"
        password "fluree"]
    (-> (flureedb/password-login my-conn ledger password user nil nil)
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [my-tx   [{:_id "_collection", :name "iotDirectory"}]
        ledger  "test/pwd"
        token   "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ0ZXN0L3B3ZCIsInN1YiI6IlRmS0xvMzh2bmRucGJZOE51REVXcExDbXN2aVFFMlhQaWFlIiwiZXhwIjoxNjE0NzE2MzA2NTQ0LCJpYXQiOjE1ODMxODAzMDY1NDQsInByaSI6IjNjNjFhYzRhODU2ZmMxNjUwZGI4MmM4NWU4ZTJiZjcyNTgyMzdiYjBlZDZiN2FhZjdmZmNhMmEzYWZhNzRiYTdmNjhhOWQ3NTYzMjYwNjk0ZTQxOWVkODQxZmRiNTgzZWNhYTg4MzViNjE3YTE1MDYwNDhmODcxY2JjNjlhZTQxNTE1NDE5OTRmOGJlNzYwMGNkN2M0MmY3NWQ5YmY2NGYifQ.-i6sSA_frz9X_0a7N21T0LMlNwQUdKSiHqYKob-Ncp0"
        opts    {:jwt token}]
    (-> (flureedb/transact my-conn
                           ledger
                           (clj->js my-txn)
                           (clj->js opts))
        (.then #(print %))
        (.catch #(print %))))

  (let [ledger "test/pwd"
        jwt "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ0ZXN0L3B3ZCIsInN1YiI6IlRmS0xvMzh2bmRucGJZOE51REVXcExDbXN2aVFFMlhQaWFlIiwiZXhwIjoxNjE0NzgwNjExNTQ2LCJpYXQiOjE1ODMyNDQ2MTE1NDYsInByaSI6IjNjNjFhYzRhODU2ZmMxNjUwZGI4MmM4NWU4ZTJiZjcyNTgyMzdiYjBlZDZiN2FhZjdmZmNhMmEzYWZhNzRiYTdmNjhhOWQ3NTYzMjYwNjk0ZTQxOWVkODQxZmRiNTgzZWNhYTg4MzViNjE3YTE1MDYwNDhmODcxY2JjNjlhZTQxNTE1NDE5OTRmOGJlNzYwMGNkN2M0MmY3NWQ5YmY2NGYifQ.w8W-rNJdcwWjoOlXu7u8xakxyX02G6fl2OMFRhOSIRE"
        private-key "3a9cc8be7e77db18d18eb7c98c1f4a6a850b0489675e8161ae1552437756d608"
        my-query (clj->js {:select ["*"] :from "org"})
        my-opts (clj->js {:jwt jwt :private-key private-key})]
    (-> (flureedb/signed-query my-conn ledger my-query my-opts)
        (.then #(print %))
        (.catch #(print "error: " %))))

  (def my-conn (flureedb/connect "http://localhost:8090"))

  (let [ledger   "test/pwd"
        user     "ldw1007"
        password "fluree"
        expire-ms 5000]
    (-> (flureedb/password-login my-conn ledger password user nil expire-ms)
        (.then #(print %))
        (.catch #(print "Error: " %))))

  (let [jwt "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ0ZXN0L3B3ZCIsInN1YiI6IlRmMjVUd2d5SDU5c1JXWmlOaGE2YkdNNldFdnoxMk1CN3hLIiwiZXhwIjoxNjE3MTA5MDQ5NTQzLCJpYXQiOjE1ODU1NzMwNDk1NDMsInByaSI6ImMxNGNiY2EwODA0ZjMyMDZlZmYzM2I3YjNmZmI1NGVjN2QxNjUyMzg3MDU1ZjFjMDg5NDhmZTE0NWVkMWM4N2Q2MTRlYmUyM2NkY2ZlZWQ0OTAxZWNjY2E1Y2M4MTVjZWRhMDIxNDQ0NWNlZWI2ZmRjZGJlMzU0YTMzM2M5YzA3YmVkYTVmY2I4MWY3MjQxNDkzMTBmYzVlYzBlY2JkMjQifQ.HZQ9aXxxeoJqlMS39hltQPWbHD-h1pu_WeKKJ7n5HRI"
        secret (-> (:flureedb-settings my-conn) deref :jwt-secret
                   (alphabase.core/base-to-byte-array :hex))
        payload (fluree.db.token-auth/verify-jwt secret jwt)]
    payload)

  (flureedb/close my-conn))


