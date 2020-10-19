(ns fluree.db.util.xhttp
  (:refer-clojure :exclude [get])
  (:require #?(:clj [aleph.http :as http])
            #?(:clj
               [byte-streams :as bs])
            #?(:clj
               [manifold.deferred :as d])
            #?(:clj
               [manifold.stream :as s])
            #?(:cljs [goog.net.XhrIo :as xhr])
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]
            #?(:clj
                     [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.core :as util])
  (:import #?(:cljs [goog.net.ErrorCode]
              :clj  (aleph.utils RequestTimeoutException))))


(defn format-error-response
  [url e]
  (let [status #?(:cljs (when-let [st (.getStatus e)]
                          (when (> st 0)
                            st))
                  :clj (:status (ex-data e)))
        error #?(:cljs (condp = (.getLastErrorCode e)
                         goog.net.ErrorCode.NO_ERROR :xhttp/no-error
                         goog.net.ErrorCode.EXCEPTION :xhttp/exception
                         goog.net.ErrorCode.HTTP_ERROR :xhttp/http-error
                         goog.net.ErrorCode.ABORT :xhttp/abort
                         goog.net.ErrorCode.TIMEOUT :xhttp/timeout
                         ;; else
                         :xhttp/unknown-error)
                 :clj  (cond
                         (instance? RequestTimeoutException e)
                         :xhttp/timeout

                         (and status (<= 300 status 499))
                         :xhttp/http-error

                         (and status (>= status 500))
                         :xhttp/exception

                         :else
                         :xhttp/unknown-error))
        message        (str "xhttp error - " url
                            (if (and status (> status 0)) (str ": " status) "")
                            #?(:clj (str " - " (.getMessage e))))]
    (ex-info message
             (cond-> {:url   url
                      :error error}
                     status (assoc :status status)))))


;; TODO - determine if pooling XhrIo instances makes any significant advantage

(defn post-json
  "Posts JSON content.
  opts is a map with following optional keys:
  :request-timeout - how many milliseconds until we throw an exception without a response (default 5000)"
  [url message opts]
  (let [{:keys [request-timeout token headers] :or {request-timeout 5000}} opts
        response-chan (async/chan)
        headers       (cond-> {"Content-Type" "application/json"}
                              headers (merge headers)
                              token (assoc "Authorization" (str "Bearer " token)))]
    #?(:clj  (d/catch
               (d/chain
                 (http/post url {:headers         headers
                                 :request-timeout request-timeout
                                 :body            (json/stringify message)})
                 (fn [response]
                   (let [body (-> response :body bs/to-string json/parse)]
                     (async/put! response-chan body))))
               (fn [e] (async/put! response-chan (format-error-response url e))))
       :cljs (try
               (xhr/send url (fn [event]
                               (let [xhr      (-> event .-target)
                                     success? (.isSuccess xhr)]
                                 (if success?
                                   (async/put! response-chan (-> (.getResponseJson xhr)
                                                                 (js->clj :keywordize-keys true)))
                                   (async/put! response-chan (format-error-response url xhr)))))
                         "POST"
                         (json/stringify message)
                         (clj->js headers)
                         request-timeout)
               (catch :default e (log/warn "CAUGHT ERROR!") (async/put! response-chan e))))
    response-chan))


(defn get
  "Returns result body as a string, or an exception.

  If opts contains :body,
  It is assumed body is already in a format that can be sent directly in request (already encoded).

  Options
  - output-format - can be :text, :json or :binary (default text), or special format (wikidata) to handle wikidata errors, which come back as html.

  "
  [url opts]
  (let [{:keys [request-timeout token headers body output-format]
         :or   {request-timeout 5000
                output-format   :text}} opts
        response-chan (async/chan)
        headers       (cond-> {}
                              headers (merge headers)
                              token (assoc "Authorization" (str "Bearer " token)))]
    #?(:clj  (d/catch
               (d/chain
                 (http/get url (util/without-nils
                                 {:headers         headers
                                  :request-timeout request-timeout
                                  :body            body}))
                 (fn [response]
                   (async/put! response-chan
                               (case output-format
                                 :text (-> response :body bs/to-string)
                                 :json (-> response :body bs/to-string json/parse)
                                 :wikidata (if (= 200 (:status response))
                                             (-> response :body bs/to-string json/parse)
                                             (async/put! response-chan (ex-info (str "Error submitting query: ") {:status (:status response) :error :db/invalid-query})))
                                 ;; else
                                 (-> response :body bs/to-byte-array)))
                   (async/put! response-chan (-> response :body bs/to-string))))
               (fn [e]
                 (if (= :wikidata output-format)
                   (let [err-body (-> (ex-data e) :body)
                         res'     (cond
                                    (= (type err-body) java.io.ByteArrayInputStream)
                                    (slurp err-body)

                                    :else
                                    err-body)
                         error    {:status  (or (:status e) 400)
                                   :message (str res')
                                   :error   :db/invalid-query}]
                     (async/put! response-chan error))
                   (async/put! response-chan (format-error-response url e)))))
       :cljs (try
               (xhr/send url (fn [event]
                               (let [xhr      (-> event .-target)
                                     success? (.isSuccess xhr)]
                                 (if success?
                                   (async/put! response-chan
                                               (case output-format
                                                 :text (.getResponseText xhr)
                                                 :json (.getResponseJson xhr)
                                                 ;; else
                                                 (throw (ex-info "http get only supports output formats of json and text for now." {}))))
                                   (async/put! response-chan (format-error-response url xhr)))))
                         "GET"
                         body
                         (clj->js headers)
                         request-timeout)
               (catch :default e (log/warn "CAUUGHT ERROR!") (async/put! response-chan e))))
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


(defn socket-publish-loop
  "Sends messages across web socket. Sends never block and will immediately queue.
  Message to send should be a two-tuple placed on the pub-chan, of:
    [msg resp-chan]
  where a true/false response will eventually be placed on the resp-chan if the send
  is successful."
  [ws pub-chan]
  (async/go
    (loop []
      (let [x (async/<! pub-chan)]
        (if (nil? x)
          (log/info "Web socket pub/producer channel closed.")
          (let [[msg resp-chan] x]
            #?(:clj  (d/catch
                       (d/chain (s/put! ws msg)
                                #(async/put! resp-chan %))
                       (fn [e] (async/put! resp-chan e)))
               :cljs (try (.send ws msg)
                          (async/put! resp-chan true)
                          (catch js/Error e
                            (log/error e "Websocket send message error:" e)
                            (async/put! resp-chan false))))

            (recur)))))))


(defn close-websocket
  [ws]
  #?(:clj  (s/close! ws)
     :cljs (.close ws)))


#?(:clj
   (defn try-socket
     [url sub-chan pub-chan resp-chan timeout close-fn]
     (async/go
       (let [socket (d/timeout!
                      (http/websocket-client url {:max-frame-payload 1e8 :max-frame-size 2e8})
                      timeout ::timeout)]
         (d/catch
           (d/chain socket
                    (fn [ws]
                      (if (= ::timeout ws)
                        (do (log/warn "Web socket timed out after waiting: " timeout)
                            (close-websocket socket)
                            (close-fn)
                            (async/put! resp-chan
                                        (ex-info (str "Timeout reached, unable to establish communication to server, which responded healthy: " url)
                                                 {:status 400 :error :db/connection-error})))
                        (do
                          (socket-publish-loop ws pub-chan)
                          (async/put! resp-chan ws)

                          ;; if socket closes, clean up connection
                          (s/on-closed ws close-fn)
                          ;; start piping subscription events to our subscription core async channel
                          (d/loop []
                                  (d/chain (s/take! ws)
                                           #(when-not (nil? %)
                                              (async/put! sub-chan %)
                                              (d/recur))))))))
           (fn [e]
             (log/warn "Error establishing socket: " (.getMessage e))
             (close-fn)
             (async/put! resp-chan e)))
         ::no-return))))


#?(:cljs
   (defn try-socket
     [url sub-chan pub-chan resp-chan timeout close-fn]
     (let [ws           (js/WebSocket. url)
           open?        (async/promise-chan)
           timeout-chan (async/timeout timeout)]

       (set! (.-binaryType ws) "arraybuffer")
       (set! (.-onopen ws) (fn [] (async/put! open? true)))
       (set! (.-onmessage ws) (fn [e] (async/put! sub-chan (.-data e))))
       (set! (.-onclose ws) (fn [e]
                              (log/warn "Websocket closed: " (.-reason e) "Code: " (.-code e))
                              (close-fn)))
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
             (socket-publish-loop ws pub-chan))))
       ::no-return)))
