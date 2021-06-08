(ns fluree.db.util.xhttp-test
  (:require
    [fluree.db.util.xhttp :as fx]
    [fluree.db.util.json :as fj]
    [test-helpers :refer [test-async]]
    #?@(:clj  [[clojure.test :refer :all]
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
          (binding [*stub-server* (start! {{:method "post" :path "/something" }
                                           {:status 200 :content-type "application/json" :body (fj/stringify {:hello "world"})}})]
            (try
              (f)
              (finally
                (.close *stub-server*))))))

#?(:clj (use-fixtures :each start-and-stop-stub-server))
; http stub for clj - end
;==========================


(deftest http-post
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
                  (= :xhttp/unknown-error))))))))


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



  )