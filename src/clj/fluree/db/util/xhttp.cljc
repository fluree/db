(ns fluree.db.util.xhttp
  (:refer-clojure :exclude [get])
  (:require #?@(:clj [[org.httpkit.sni-client :as sni-client]
                      [org.httpkit.client :as http]
                      [byte-streams :as bs]
                      [hato.websocket :as ws]])
            #?@(:cljs [["axios" :as axios]
                       ["ws" :as NodeWebSocket]])
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.platform :as platform]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true])
  (:import #?@(:clj  ((org.httpkit.client TimeoutException)
                      (java.nio HeapCharBuffer))
               :cljs ((goog.net.ErrorCode)))))

#?(:clj (set! *warn-on-reflection* true))

;; allow large websocket frames of ~10mb
#?(:clj (System/setProperty "org.asynchttpclient.webSocketMaxFrameSize" "10000000"))


(defn format-error-response
  [url e]
  (let [status #?(:cljs (when-let [resp (.-response e)]
                          (when-let [st (.-status resp)]
                            (when (> st 0)
                              st)))
                  :clj (:status (ex-data e)))
        error #?(:cljs (cond
                         (str/starts-with? (.-message e) "timeout")
                         :xhttp/timeout

                         (and status (<= 300 status 499))
                         :xhttp/http-error

                         (and status (>= status 500))
                         :xhttp/exception

                         :else
                         (do (log/error "XHTTP Request Error:" (.-request e))
                             :xhttp/unknown-error))

                 :clj  (cond
                         (instance? TimeoutException e)
                         :xhttp/timeout

                         (instance? Throwable e)
                         :xhttp/exception

                         (and status (<= 300 status 499))
                         :xhttp/http-error

                         (and status (>= status 500))
                         :xhttp/exception

                         :else
                         :xhttp/unknown-error))
        message        (str "xhttp error - " url
                            (if (and status (> status 0)) (str ": " status) "")
                            #?(:clj (str " - " (.getMessage ^Throwable e))))]
    (ex-info message
             (cond-> {:url   url
                      :error error}
                     status (assoc :status status)))))


#?(:clj
   (defn throw-if-timeout [response]
     (if (= TimeoutException (some-> response
                                     :error
                                     :error/via
                                     first
                                     :type))
       (throw (TimeoutException. (-> response :error :error/cause)))
       response)))

(defn post
  "Posts pre-formatted message (e.g. already stringified JSON)."
  [url message opts]
  (let [{:keys [request-timeout token headers keywordize-keys json?]
         :or   {request-timeout 5000
                keywordize-keys true}} opts
        response-chan (async/chan)
        multipart?    (and (map? message)
                           (contains? message :multipart))
        headers*      (cond-> headers
                        json? (assoc "Content-Type" "application/json")
                        token (assoc "Authorization" (str "Bearer " token)))
        base-req      (if multipart? ;; multipart requests need to be sent in special map structure
                        message
                        {:body message})]
    #?(:clj (http/post url (assoc base-req :headers headers*
                                           :timeout request-timeout)
                       (fn [{:keys [error status body] :as response}]
                         (try ;; TODO - throw-if-timeout will throw but uncaught as `post` fn returns response-chan - adding a 'try/catch' for now
                           ;; TODO - ideally throw-if-timeout should be part of the format-error-response fn to put ex on response-chan and can remove outer try/catch
                           (if (or error (< 299 status))
                             (do
                               (throw-if-timeout response)
                               (async/put!
                                 response-chan
                                 (format-error-response
                                   url
                                   (or error (ex-info "error response"
                                                      response)))))
                             (let [data (try (cond-> (bs/to-string body)
                                                     json? (json/parse keywordize-keys))
                                             (catch Exception e
                                               ;; don't throw, as `data` will get exception and put on response-chan
                                               (ex-info (str "JSON parsing error for xhttp post request to: " url
                                                             " with error message: " (ex-message e))
                                                        {:status 400 :error :db/invalid-json}
                                                        e)))]
                               (async/put! response-chan data)))
                           (catch Exception e
                             (async/put! response-chan e)))))
       :cljs
       (let [req {:url url
                  :method "post"
                  :timeout request-timeout
                  :headers (cond-> headers*
                             multipart? (assoc "Content-Type" "multipart/form-data"))
                  :data (if multipart?
                          (mapv :content (:multipart message))
                          message)}]
         (-> axios
             (.request (clj->js req))
             (.then (fn [resp]
                      (let [headers (js->clj (.-headers resp) :keywordize-keys true)]
                        (async/put! response-chan (condp = (:content-type headers)
                                                    "application/json" (:data (js->clj resp :keywordize-keys keywordize-keys))
                                                    resp)))))
             (.catch (fn [err]
                       (async/put! response-chan (format-error-response url err)))))))
    response-chan))


(defn post-json
  "Posts JSON content, returns parsed JSON response as core async channel.
  opts is a map with following optional keys:
  :request-timeout - how many milliseconds until we throw an exception without a response (default 5000)"
  [url message opts]
  (let [base-req (if (contains? message :multipart)
                   (->> (:multipart message) ;; stringify each :content key of multipart message
                        (mapv #(assoc % :content (json/stringify (:content %))))
                        (assoc message :multipart))
                   (json/stringify message))]
    (post url base-req (assoc opts :json? true))))


(defn get
  "Returns result body as a string, or an exception.

  If opts contains :body,
  It is assumed body is already in a format that can be sent directly in request (already encoded).

  Options
  - output-format - can be :text, :json, :edn or :binary (default :text), or special format (wikidata) to handle wikidata errors, which come back as html.

  "
  [url opts]
  (let [{:keys [request-timeout token headers body output-format]
         :or   {request-timeout 5000
                output-format   :text}} opts
        response-chan (async/chan)
        headers       (cond-> {}
                              headers (merge headers)
                              token (assoc "Authorization" (str "Bearer " token)))]
    #?(:clj  (http/get url (util/without-nils
                             {:headers headers
                              :timeout request-timeout
                              :body    body})
                       (fn [{:keys [error status body] :as response}]
                         (if (or error (< 299 status))
                           (if (= :wikidata output-format)
                             (let [err-body (-> error ex-data :body)
                                   res'     (cond
                                              (= (type err-body) java.io.ByteArrayInputStream)
                                              (slurp err-body)

                                              :else
                                              err-body)
                                   error    {:status  (or (-> error ex-data :status) 400)
                                             :message (str res')
                                             :error   :db/invalid-query}]
                               (async/put! response-chan error))
                             (async/put! response-chan
                                         (format-error-response
                                           url
                                           (or error (ex-info "error response"
                                                              response)))))
                           (do
                             (throw-if-timeout response)
                             (async/put! response-chan
                                         (case output-format
                                           (:text :json) (bs/to-string body)
                                           (:edn :wikidata) (-> body bs/to-string json/parse)
                                           ;; else
                                           (bs/to-byte-array body)))))))
       :cljs (-> axios
                 (.request (clj->js {:url     url
                                     :method  "get"
                                     :timeout request-timeout
                                     :headers headers}))
                 (.then (fn [resp]
                          (let [data (:data (js->clj resp :keywordize-keys true))]
                            (async/put! response-chan
                                        (case output-format
                                          :text data
                                          :json (json/stringify data)
                                          ;; else
                                          (throw (ex-info "http get only supports output formats of json and text." {})))))))
                 (.catch (fn [err]
                           (async/put! response-chan (format-error-response url err))))))
    response-chan))


(defn get-json
  "http get with JSON response.

  If opts contains :body key, it is encoded into JSON and sent as part
  of the body."
  [url opts]
  (let [opts* (cond-> (-> opts
                          (assoc-in [:headers "Accept"] "application/json")
                          (assoc :output-format :json))
                      (:body opts) (assoc :body (json/stringify (:body opts))))]
    (get url opts*)))

(def ws-close-status-codes
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



(defn close-websocket
  "Closes websocket with optional reason-keyword which
  will utilize the respective status code and reason string
  from ws-close-status-codes.
  Status code info:
  https://www.rfc-editor.org/rfc/rfc6455.html#section-7.1.5"
  ([ws]
   #?(:clj  (ws/close! ws)
      :cljs (.close ws)))
  ([ws reason-kw]
   (let [code   (get-in ws-close-status-codes [reason-kw :code] 1000)
         reason (get-in ws-close-status-codes [reason-kw :reason] "Normal closure")]
     #?(:clj  (ws/close! ws code reason)
        :cljs (.close ws code reason)))))

(defn socket-publish-loop
  "Sends messages out as they appear on pub-chan.
  If no message has sent out recently, sends a ping message.

  Does not use ws/ping! as in CLJS web browser support for a
  true ping is limited/non-existent, so just sends a ping event
  over the normal message channel which has the same keep-alive effect."
  [ws pub-chan]
  (async/go-loop []
    (let [ping-chan (async/timeout 20000)
          [val ch] (async/alts! [pub-chan ping-chan])]
      (if (and (= ch pub-chan)
               (nil? val))
        (do
          (log/info "Web socket pub/producer channel closed.")
          (close-websocket ws :going-away))
        (let [[msg resp-chan] (if (= ch ping-chan)
                                [(json/stringify {"action" "ping"}) nil]
                                val)]
          (try*
            #?(:clj  (ws/send! ws msg)
               :cljs (.send ws msg))
            (when resp-chan
              (async/put! resp-chan true)
              (async/close! resp-chan))
            (catch* e
                    (log/error e "Error sending websocket message:" msg)
                    (async/put! resp-chan false)))
          (recur))))))

(declare try-socket)

(defn retry-socket
  "Attempts repeated retried to re-establish connection."
  [url sub-chan pub-chan timeout close-fn]
  (async/go-loop [retries 1]
    (let [retry-timeout (min (* retries 1000) 20000)
          ws            (async/<! (try-socket url sub-chan pub-chan timeout close-fn))]
      (when (util/exception? ws)
        (do
          (log/info "Unable to establish websocket connection, retrying in " retry-timeout "ms. "
                    "Reported websocket exception: " (ex-message ws))
          (async/<! (async/timeout (min (* retries 500) 10000))) ;; timeout maxes at 10s
          (recur (inc retries)))))))


(defn abnormal-socket-close?
  [status-code]
  (= status-code (get-in ws-close-status-codes [:abnormal :code])))

(defn try-socket
  [url msg-in msg-out timeout close-fn]
  #?(:clj
     (let [resp-chan (async/promise-chan)
           ws-config {:connect-timeout timeout
                      :on-close        (fn [_ status reason]
                                         (log/debug "Websocket closed; status:" status
                                                    "reason:" reason)
                                         (if (abnormal-socket-close? status)
                                           (do
                                             (log/info "Abnormal websocket closure, attempting to re-establish connection.")
                                             (retry-socket url msg-in msg-out timeout close-fn))
                                           (do (log/debug "Closing websocket message channels")
                                               (async/close! msg-in)
                                               (async/close! msg-out)
                                               (close-fn))))
                      :headers         nil
                      :on-open         (fn [_]
                                         (log/debug "Websocket opened"))
                      :on-error        (fn [_ e]
                                         (log/error e "Websocket error")
                                         (close-fn)
                                         (when-not (nil? e) (async/put! resp-chan e)))
                      :on-message      (fn [_ msg last?]
                                         (async/put! msg-in [:on-message (.toString ^HeapCharBuffer msg) last?]))
                      :on-ping         (fn [ws msg]
                                         (async/put! msg-in [:on-ping msg])
                                         (ws/pong! ws msg))
                      :on-pong         (fn [_ msg]
                                         (async/put! msg-in [:on-pong msg]))}]

       ;; launch websocket connection in background
       (future
         (let [ws (try @(ws/websocket url ws-config)
                       (catch Exception e e))]
           (when-not (util/exception? ws)
             (socket-publish-loop ws msg-out))
           (async/put! resp-chan ws)
           (async/close! resp-chan)))

       ;; response chan will have websocket or exception
       resp-chan)

     :cljs
     (let [resp-chan    (async/promise-chan)
           ws           (if platform/BROWSER
                          (js/WebSocket. url)
                          (NodeWebSocket. url))
           open?        (async/promise-chan)
           timeout-chan (async/timeout timeout)]

       (set! (.-binaryType ws) "arraybuffer")
       (set! (.-onopen ws) (fn [] (async/put! open? true)))
       (set! (.-onmessage ws) (fn [e] (async/put! msg-in (.-data e))))
       (set! (.-onclose ws) (fn [e]
                              (log/info "Websocket closed: " (.-reason e) "Code: " (.-code e))
                              (if (abnormal-socket-close? (.-code e))
                                (do
                                  (log/info "Abnormal websocket closure, attempting to re-establish connection.")
                                  (retry-socket url msg-in msg-out timeout close-fn))
                                (close-fn))))
       ;; timeout connection attempt
       (async/go
         (let [[_ ch] (async/alts! [open? timeout-chan] :priority true)]
           (if (= ch timeout-chan)
             (do
               (async/put! resp-chan
                           (ex-info (str "Timeout reached, unable to establish communication to server, which responded healthy: " url)
                                    {:status 400 :error :db/connection-error}))
               (close-websocket ws))
             ;; socket is open, start loop for outgoing messages
             (socket-publish-loop ws msg-out))))
       resp-chan)))
