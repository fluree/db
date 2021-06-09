(ns fluree.db.util.xhttp-test
  (:require
    [fluree.db.util.xhttp :as fx]
    [fluree.db.util.json :as fj]
    [test-helpers :refer [test-async]]
    #?@(:clj  [[byte-streams :as bs]
               [clojure.test :refer :all]
               [clojure.core.async :refer [<! >! go]]
               [stub-http.core :refer :all]]
        :cljs [[cljs.test :refer-macros [deftest is testing use-fixtures]]
               [cljs.core.async :refer [<! >! go]]
               [cljs.js :as cjs]
               [goog.net.XhrIo :as xhr]
               [goog.object :as gobject]]))
  #?(:clj  (:import (aleph.utils RequestTimeoutException)
                    (clojure.lang ExceptionInfo))
     :cljs (:import [goog.net.ErrorCode])))

(def ledger-auth {:private "a603e772faec02056d4ec3318187487d62ec46647c0cba7320c7f2a79bed2615"
                  :auth    "TfCFawNeET5FFHAfES61vMf9aGc1vmehjT2"
                  :sid     105553116266496
                  :token   "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"})

;TODO - pull http/xhr mocks into separate class
(def healthcheck-uri "http://localhost/fdb/health")
(declare ^:dynamic *stub-server*)

#?(:cljs (set! *stub-server* {:uri "http://localhost"}))

;==========================
; xhr stub for cljs - start
#?(:cljs
   ; create instance of xhrio
   (def xhr (goog.net.XhrIo.)))
; xhr stub for cljs - end
;==========================

;==========================
; http stub for clj - start
#?(:clj (defn start-and-stop-stub-server [f]
          (binding [*stub-server* (start! {{:method "get" :path "/get-check"}
                                           {:status 200 :content-type "text/plain" :body (fj/stringify {:hello "mars"})}

                                           {:method "get" :path "/get-check-wikidata"}
                                           {:status 200
                                            :content-type "application/octet-stream"
                                            :headers ["Content-Type" "application/octet-stream"]
                                            :body (-> {:hello "mars"} fj/stringify bs/to-byte-array)}

                                           {:method "post" :path "/something"}
                                           {:status 200 :content-type "text/plain" :body (fj/stringify {:hello "world"})}})]
            (try
              (f)
              (finally
                (.close *stub-server*))))))

#?(:clj (use-fixtures :each start-and-stop-stub-server))
; http stub for clj - end
;==========================

; TODO - outstanding tests
;get output-format :wikidata - need to research passing binary content for response
;websocket unit tests
(deftest db-util-xhttp-test
  (testing "format-error-response"
    (testing "timeout"
      (let [e #?(:clj  (->> (RequestTimeoutException. "timeout")
                            (fx/format-error-response healthcheck-uri))
                 :cljs (do (-> xhr (.error_ goog.net.ErrorCode.TIMEOUT "timeout"))
                           (fx/format-error-response healthcheck-uri xhr)))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/timeout)))))

    (testing "http-error"
      (let [e #?(:clj  (->> (ex-info "http-error" {:status 403})
                            (fx/format-error-response healthcheck-uri))
                 :cljs (do (-> xhr (.error_ goog.net.ErrorCode.HTTP_ERROR "http-error"))
                           (fx/format-error-response healthcheck-uri xhr)))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/http-error)))))

    (testing "exception"
      (let [e #?(:clj  (->> (ex-info "server error" {:status 500})
                            (fx/format-error-response healthcheck-uri))
                 :cljs (do (-> xhr (.error_ goog.net.ErrorCode.EXCEPTION "exception"))
                           (fx/format-error-response healthcheck-uri xhr)))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/exception)))))

    (testing "unknown-error status=0"
      (let [e (->> (ex-info "unknown error" {:status 0})
                   (fx/format-error-response healthcheck-uri))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/unknown-error)))))

    (testing "cljs - other exceptions"
      (when-let [e #?(:clj nil
                      :cljs (do (-> xhr (.error_ goog.net.ErrorCode.NO_ERROR "no-error"))
                                (fx/format-error-response healthcheck-uri xhr)))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/no-error))))
      (when-let [e #?(:clj nil
                      :cljs (do (-> xhr (.error_ goog.net.ErrorCode.ABORT "abort"))
                            (fx/format-error-response healthcheck-uri xhr)))]
        (is (instance? ExceptionInfo e))
        (is (-> e
                ex-data
                :error
                (= :xhttp/abort))))))

  (testing "http-post - valid request"
    (test-async
      (go
        (when-let [res #?(:cljs nil                         ;skip test for now
                          :clj  (-> (str (:uri *stub-server*) "/something")
                                    (fx/post-json "message" {})
                                    <!))]
          (is (map? res))
          (is (= "world" (:hello res)))))))
  (testing "http-post - invalid request"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (-> (str (:uri *stub-server*) "/nothing")
                                    (fx/post-json "message" {})
                                    <!))]
          (is (instance? ExceptionInfo res))
          (is (-> res
                  ex-data
                  :error
                  (= :xhttp/unknown-error)))))))

  (testing "get - output-format :binary"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (->> {:headers {"Accept" "application/json"}
                                      :body     "message"
                                      :request-timeout 100
                                      :output-format :binary}
                                     (fx/get (str (:uri *stub-server*) "/get-check"))
                                     <!
                                     ))]
          (is (bytes? res))
          (is (= "mars" (-> res fj/parse :hello)))))))
  (testing "get - output-format :text"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (->> {:headers {"Accept" "application/json"}
                                      :body     "message"
                                      :request-timeout 100
                                      :output-format :text}
                                     (fx/get (str (:uri *stub-server*) "/get-check"))
                                     <!))]
          (is (string? res))
          (is (= "mars" (-> res fj/parse :hello)))))))
  (testing "get - output-format :json"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (->> {:headers {"Accept" "application/json"}
                                      :body     "message"
                                      :token (:token ledger-auth)
                                      :request-timeout 100
                                      :output-format :json}
                                     (fx/get (str (:uri *stub-server*) "/get-check"))
                                     <!))]
          (is (map? res))
          (is (= "mars" (:hello res)))))))
  (testing "get - error"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (->> {:headers {"Accept" "application/json"}
                                      :body     "message"
                                      :request-timeout 100
                                      :output-format :json}
                                     (fx/get (str (:uri *stub-server*) "/nothing"))
                                     <!))]
          (is (instance? ExceptionInfo res))
          (is (-> res
                  ex-data
                  :error
                  (= :xhttp/unknown-error)))))))

  (testing "get-json - valid request"
    (test-async
      (go
        (when-let [res #?(:cljs nil
                          :clj  (->> {:headers {"Cache-Control" "no-cache"}
                                      :body     "message"}
                                     (fx/get-json (str (:uri *stub-server*) "/get-check"))
                                     <!))]
          (is (map? res))
          (is (= "mars" (:hello res))))))))
