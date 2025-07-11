(ns fluree.db.util.websocket
  "WebSocket client implementation using Java 11 HttpClient.
   Designed for GraalVM native image compatibility."
  (:require [clojure.core.async :as async]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.log :as log])
  (:import [java.net URI]
           [java.net.http HttpClient WebSocket WebSocket$Listener WebSocket$Builder]
           [java.time Duration]
           [java.util.concurrent CompletableFuture]))

(set! *warn-on-reflection* true)

(def ws-close-status-codes
  "WebSocket close status codes as defined in RFC 6455"
  {:normal-close {:code 1000 :reason "Normal closure"}
   :going-away   {:code 1001 :reason "Going away"}
   :protocol     {:code 1002 :reason "Protocol error"}
   :unsupported  {:code 1003 :reason "Unsupported data"}
   :no-status    {:code 1005 :reason "No status code"}
   :abnormal     {:code 1006 :reason "Abnormal closure"}
   :bad-data     {:code 1007 :reason "Invalid frame payload data"}
   :policy       {:code 1008 :reason "Policy violation"}
   :too-big      {:code 1009 :reason "Message too big"}
   :extension    {:code 1010 :reason "Missing extension"}
   :unexpected   {:code 1011 :reason "Internal error"}
   :internal     {:code 1012 :reason "Service restart"}
   :service      {:code 1013 :reason "Try again later"}
   :gateway      {:code 1014 :reason "Bad gateway"}
   :tls          {:code 1015 :reason "TLS handshake"}})

(defn- create-listener
  "Creates a WebSocket listener that handles incoming messages and events"
  [{:keys [on-open on-message on-error on-close on-ping on-pong msg-chan]}]
  (let [;; Track accumulated text messages for fragmented sends
        text-accumulator (atom nil)]
    (reify WebSocket$Listener
      (onOpen [_ websocket]
        (log/debug "WebSocket opened")
        (when on-open (on-open websocket))
        ;; Request one message at a time (backpressure control)
        (.request ^WebSocket websocket 1)
        nil)

      (onText [_ websocket data last]
        (try*
          (if last
            ;; Complete message received
            (let [complete-msg (if-let [accumulated @text-accumulator]
                                 (do
                                   (reset! text-accumulator nil)
                                   (str accumulated data))
                                 data)]
              (when msg-chan
                (async/put! msg-chan [:on-message complete-msg true]))
              (when on-message
                (on-message websocket complete-msg true)))
            ;; Partial message - accumulate
            (swap! text-accumulator #(str % data)))

          ;; Request next message
          (.request ^WebSocket websocket 1)
          nil
          (catch* e
            (log/error e "Error processing WebSocket text message")
            nil)))

      (onBinary [_ websocket _data _last]
        ;; For now, we don't handle binary messages
        ;; Could be extended if needed
        (.request ^WebSocket websocket 1)
        nil)

      (onPing [_ websocket message]
        (when msg-chan
          (async/put! msg-chan [:on-ping message]))
        (when on-ping
          (on-ping websocket message))
        ;; Automatically send pong response
        (.sendPong ^WebSocket websocket message)
        (.request ^WebSocket websocket 1)
        nil)

      (onPong [_ websocket message]
        (when msg-chan
          (async/put! msg-chan [:on-pong message]))
        (when on-pong
          (on-pong websocket message))
        (.request ^WebSocket websocket 1)
        nil)

      (onError [_ websocket error]
        (log/error error "WebSocket error")
        (when on-error
          (on-error websocket error))
        nil)

      (onClose [_ websocket status-code reason]
        (log/debug "WebSocket closed; status:" status-code "reason:" reason)
        (when msg-chan
          (async/put! msg-chan [:on-close status-code reason]))
        (when on-close
          (on-close websocket status-code reason))
        nil))))

(defn send-text!
  "Send a text message through the WebSocket.
   Returns a CompletableFuture that completes when the message is sent."
  [^WebSocket ws message]
  (.sendText ws message true))

(defn send-ping!
  "Send a ping message through the WebSocket."
  [^WebSocket ws message]
  (.sendPing ws message))

(defn send-pong!
  "Send a pong message through the WebSocket."
  [^WebSocket ws message]
  (.sendPong ws message))

(defn close!
  "Close the WebSocket connection"
  ([^WebSocket ws]
   (.sendClose ws WebSocket/NORMAL_CLOSURE ""))
  ([^WebSocket ws reason-kw]
   (let [{:keys [code reason]} (get ws-close-status-codes reason-kw
                                    {:code 1000 :reason "Normal closure"})]
     (.sendClose ws code reason))))

(defn abort!
  "Forcibly close the WebSocket connection"
  [^WebSocket ws]
  (.abort ws))

(defn- configure-builder
  "Configure WebSocket builder with options"
  [^WebSocket$Builder builder {:keys [connect-timeout headers subprotocols]}]
  (when connect-timeout
    (.connectTimeout builder (Duration/ofMillis connect-timeout)))

  (when headers
    (doseq [[k v] headers]
      (.header builder k v)))

  ;; Note: subprotocols support removed due to reflection issues
  ;; Can be re-added when needed with proper type resolution
  (when subprotocols
    (log/warn "WebSocket subprotocols not supported in this implementation"))

  builder)

(defn connect
  "Connect to a WebSocket endpoint.
   
   Options:
   - :on-open      - fn called when connection opens [ws]
   - :on-message   - fn called for each message [ws message last?]
   - :on-error     - fn called on error [ws error]
   - :on-close     - fn called when connection closes [ws status-code reason]
   - :on-ping      - fn called on ping [ws message]
   - :on-pong      - fn called on pong [ws message]
   - :msg-chan     - async channel to receive all events as vectors
   - :connect-timeout - connection timeout in ms (default 30000)
   - :headers      - map of headers to send
   - :subprotocols - collection of subprotocols to request
   
   Returns a CompletableFuture<WebSocket> or throws on connection failure."
  [url opts]
  (let [client   (HttpClient/newHttpClient)
        listener (create-listener opts)
        builder  (.newWebSocketBuilder ^HttpClient client)]
    (.buildAsync ^WebSocket$Builder (configure-builder builder opts) (URI/create url) listener)))

(defn connect-async
  "Async version of connect that returns a channel with the WebSocket or error"
  [url opts]
  (let [result-chan (async/promise-chan)]
    (try*
      (let [^CompletableFuture cf (connect url opts)]
        (.whenComplete cf
                       (reify java.util.function.BiConsumer
                         (accept [_ ws error]
                           (if error
                             (async/put! result-chan error)
                             (async/put! result-chan ws))))))
      (catch* e
        (async/put! result-chan e)))
    result-chan))

(defn socket-publish-loop
  "Sends messages out as they appear on pub-chan.
   If no message has sent out recently, sends a ping message."
  [^WebSocket ws pub-chan]
  (async/go-loop []
    (let [ping-timeout 20000  ; 20 seconds
          ping-chan    (async/timeout ping-timeout)
          [val ch]     (async/alts! [pub-chan ping-chan])]
      (cond
        ;; Channel closed
        (and (= ch pub-chan) (nil? val))
        (do
          (log/info "WebSocket pub/producer channel closed.")
          (close! ws :going-away))

        ;; Send ping
        (= ch ping-chan)
        (do
          (try*
            (send-ping! ws (java.nio.ByteBuffer/allocate 0))
            (catch* e
              (log/error e "Error sending ping")))
          (recur))

        ;; Send message
        :else
        (let [[msg resp-chan] val]
          (try*
            @(send-text! ws msg)
            (when resp-chan
              (async/put! resp-chan true)
              (async/close! resp-chan))
            (catch* e
              (log/error e "Error sending websocket message:" msg)
              (when resp-chan
                (async/put! resp-chan false)
                (async/close! resp-chan))))
          (recur))))))

(defn abnormal-close?
  "Check if the close status code indicates an abnormal closure"
  [status-code]
  (= status-code (get-in ws-close-status-codes [:abnormal :code])))

(defn websocket
  "High-level WebSocket connection that matches the API of existing xhttp implementation.
   
   Creates a WebSocket connection with automatic reconnection on abnormal closure.
   
   Options:
   - :msg-in       - async channel for incoming messages
   - :msg-out      - async channel for outgoing messages  
   - :connect-timeout - connection timeout in ms
   - :close-fn     - function to call on final close
   
   Returns a channel that will contain the WebSocket or an error."
  [url {:keys [msg-in msg-out connect-timeout close-fn]
        :or {connect-timeout 30000}}]
  (let [result-chan (async/promise-chan)]
    (letfn [(try-connect []
              (connect-async url
                             {:msg-chan msg-in
                              :connect-timeout connect-timeout
                              :on-close (fn [_ws status-code _reason]
                                          (if (abnormal-close? status-code)
                                            (do
                                              (log/info "Abnormal websocket closure, attempting to reconnect...")
                                              (async/go
                                                (async/<! (async/timeout 1000))
                                                (try-connect)))
                                            (when close-fn
                                              (close-fn))))}))]
      (async/go
        (let [ws (async/<! (try-connect))]
          (if (instance? Throwable ws)
            (async/put! result-chan ws)
            (do
              ;; Start publish loop
              (socket-publish-loop ws msg-out)
              (async/put! result-chan ws))))))
    result-chan))