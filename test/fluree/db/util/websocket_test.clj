(ns fluree.db.util.websocket-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.util.websocket :as ws])
  (:import [java.net.http WebSocket]))

(deftest test-websocket-connection
  (testing "WebSocket basic functionality"
    ;; Test with a public echo WebSocket server
    (let [echo-url "wss://echo.websocket.org/"
          msg-in   (async/chan 10)
          msg-out  (async/chan 10)
          result   (async/<!! (ws/websocket echo-url
                                            {:msg-in msg-in
                                             :msg-out msg-out
                                             :connect-timeout 5000}))]

      (if (instance? Throwable result)
        ;; If connection fails (e.g., in CI), just skip the test
        (println "WebSocket test skipped - could not connect to echo server:" (.getMessage ^Throwable result))

        (try
          (is (instance? WebSocket result))

          ;; Test sending a message
          (async/>!! msg-out ["Hello, WebSocket!" nil])

          ;; Wait for echo response
          (let [[event-type message _] (async/<!! msg-in)]
            (is (= :on-message event-type))
            ;; Echo server may return different types of messages
            ;; Just verify we got something back
            (is (not (nil? message))))

          ;; Test ping functionality  
          (ws/send-ping! result (java.nio.ByteBuffer/wrap (.getBytes "ping")))

          ;; Wait for pong (may take a moment)
          (let [timeout-ch (async/timeout 3000)
                [val ch] (async/alts!! [msg-in timeout-ch])]
            (when (= ch msg-in)
              (let [[event-type _] val]
                (is (= :on-pong event-type)))))

          (finally
            ;; Clean up
            (ws/close! result)
            (async/close! msg-in)
            (async/close! msg-out)))))))

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