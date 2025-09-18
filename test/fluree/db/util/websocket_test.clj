(ns fluree.db.util.websocket-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.util.websocket :as ws])
  (:import [java.net.http WebSocket]
           [java.nio ByteBuffer]
           [java.util.concurrent CompletableFuture]))

(deftest test-socket-publish-loop-sends-text
  (testing "socket-publish-loop sends text messages via WebSocket"
    (let [sent (atom [])
          mock-ws (reify WebSocket
                    (^CompletableFuture sendText [this ^CharSequence data ^boolean _last]
                      (swap! sent conj (str data))
                      (CompletableFuture/completedFuture this))
                    (^CompletableFuture sendPing [this ^ByteBuffer _msg]
                      (CompletableFuture/completedFuture this))
                    (^CompletableFuture sendPong [this ^ByteBuffer _msg]
                      (CompletableFuture/completedFuture this))
                    (^CompletableFuture sendClose [this ^int _code ^String _reason]
                      (CompletableFuture/completedFuture this))
                    (^void request [_this ^long _n]
                      nil)
                    ;; Unused methods in this test can be no-ops or return defaults
                    (^String getSubprotocol [_] "")
                    (^boolean isOutputClosed [_] false)
                    (^boolean isInputClosed [_] false)
                    (^void abort [_]))
          msg-out (async/chan 1)]
      ;; start the publish loop
      (ws/socket-publish-loop mock-ws msg-out)
      ;; enqueue a message and wait for it to be processed
      (async/>!! msg-out ["hello" nil])
      ;; small yield to allow go-loop to run
      (Thread/sleep 50)
      (is (= ["hello"] @sent))
      (async/close! msg-out))))

(deftest test-listener-emits-events
  (testing "listener emits :on-message and :on-ping and auto-pongs"
    (let [events (async/chan 10)
          ponged? (atom false)
          mock-ws (reify WebSocket
                    (^CompletableFuture sendPong [this ^ByteBuffer _msg]
                      (reset! ponged? true)
                      (CompletableFuture/completedFuture this))
                    (^void request [_this ^long _n]
                      nil)
                    (^CompletableFuture sendText [this ^CharSequence _d ^boolean _l]
                      (CompletableFuture/completedFuture this))
                    (^CompletableFuture sendPing [this ^ByteBuffer _m]
                      (CompletableFuture/completedFuture this))
                    (^CompletableFuture sendClose [this ^int _c ^String _r]
                      (CompletableFuture/completedFuture this))
                    (^String getSubprotocol [_] "")
                    (^boolean isOutputClosed [_] false)
                    (^boolean isInputClosed [_] false)
                    (^void abort [_]))
          listener (@#'ws/create-listener {:msg-chan events})]
      ;; onText complete message
      (.onText ^java.net.http.WebSocket$Listener listener mock-ws "abc" true)
      (let [[etype msg last?] (async/<!! events)]
        (is (= :on-message etype))
        (is (= "abc" msg))
        (is (true? last?)))
      ;; onPing should emit and also sendPong
      (.onPing ^java.net.http.WebSocket$Listener listener mock-ws (ByteBuffer/allocate 0))
      (let [[etype _] (async/<!! events)]
        (is (= :on-ping etype)))
      (is (true? @ponged?))
      (async/close! events))))

(deftest test-close-status-codes
  (testing "WebSocket close status codes"
    (is (= 1000 (get-in ws/ws-close-status-codes [:normal-close :code])))
    (is (= 1006 (get-in ws/ws-close-status-codes [:abnormal :code])))
    (is (= "Going away" (get-in ws/ws-close-status-codes [:going-away :reason])))))

(deftest test-abnormal-close
  (testing "Abnormal close detection"
    (is (true? (ws/abnormal-close? 1006)))
    (is (false? (ws/abnormal-close? 1000)))
    (is (false? (ws/abnormal-close? 1001)))))

(deftest test-connect-options
  (testing "WebSocket connection options"
    ;; Test that options are properly passed
    (let [options {:connect-timeout 10000
                   :headers {"X-Custom-Header" "test"}
                   :subprotocols ["chat" "superchat"]}
          ;; Use an invalid URL to test error handling
          result (async/<!! (ws/connect-async "ws://invalid.example.com" options))]

      (is (instance? Throwable result)
          "Connection to invalid host should fail")
      ;; Print the actual error for debugging
      (println "Connection error:" (str result)))))
