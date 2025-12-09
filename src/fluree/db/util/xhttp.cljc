(ns fluree.db.util.xhttp
  (:refer-clojure :exclude [get])
  (:require #?@(:clj [[fluree.db.util.websocket :as jws]])
            #?@(:cljs [["axios" :as axios]
                       ["ws" :as NodeWebSocket]
                       [fluree.db.platform :as platform]])
            [clojure.core.async :as async]
            [clojure.string :as str]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true])
  #?(:clj
     (:import (java.net.http HttpClient HttpRequest HttpRequest$Builder HttpRequest$BodyPublishers HttpResponse HttpResponse$BodyHandlers)
              (java.net URI)
              (java.time Duration)
              (java.util.concurrent TimeoutException))))

#?(:clj (set! *warn-on-reflection* true))

;; allow large websocket frames of ~10mb
#?(:clj (System/setProperty "org.asynchttpclient.webSocketMaxFrameSize" "10000000"))

#?(:clj
   (defn create-http-client
     "Creates a new HTTP client instance. For GraalVM compatibility, this is a
     function rather than a top-level def to avoid initialization at build time."
     []
     (-> (HttpClient/newBuilder)
         (.connectTimeout (Duration/ofSeconds 10))
         (.build))))

#?(:clj
   (def http-client
     "Delay that creates HTTP client on first use for GraalVM compatibility"
     (delay (create-http-client))))

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
                         (do (log/error! ::xhttp-request-error e {:msg "XHTTP Request Error"
                                                                  :request (.-request e)})
                             (log/error "XHTTP Request Error:" (.-request e))
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
               status (assoc :status status))
             e)))

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
                        token (assoc "Authorization" (str "Bearer " token)))]
    #?(:clj
       (async/thread
         (try
           (let [builder (-> (HttpRequest/newBuilder)
                             (.uri (URI/create url))
                             (.timeout (Duration/ofMillis request-timeout))
                             (.POST (if multipart?
                                      (throw (ex-info "Multipart not yet supported in native HTTP client" {:url url}))
                                      (if (bytes? message)
                                        (HttpRequest$BodyPublishers/ofByteArray message)
                                        (HttpRequest$BodyPublishers/ofString message)))))]
             ;; Add headers
             (doseq [[k v] headers*]
               (.header builder k v))

             (let [request (.build builder)
                   ^HttpClient client @http-client
                   ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))
                   status (.statusCode response)
                   body (.body response)]
               (if (< 299 status)
                 (async/put! response-chan
                             (ex-info (str "HTTP error response: " body)
                                      {:status status :body body :url url}))
                 (let [data (try (cond-> body
                                   json? (json/parse keywordize-keys))
                                 (catch Exception e
                                   (ex-info (str "JSON parsing error for xhttp post request to: " url
                                                 " with error message: " (ex-message e))
                                            {:status 400 :error :db/invalid-json}
                                            e)))]
                   (async/put! response-chan data)))))
           (catch TimeoutException e
             (async/put! response-chan
                         (format-error-response url e)))
           (catch Exception e
             (async/put! response-chan
                         (format-error-response url e)))))
       :cljs
       (let [req {:url     url
                  :method  "post"
                  :timeout request-timeout
                  :headers (cond-> headers*
                             multipart? (assoc "Content-Type" "multipart/form-data"))
                  :data    (if multipart?
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

(defn get-headers
  [^HttpResponse response]
  (reduce-kv (fn [m k v]
               (let [k* (str/lower-case k)
                     v* (vec v)]
                 (assoc m k* v*)))
             {} (-> response .headers .map)))

(defn get-response
  "Returns full HTTP response as a map with :status, :headers, and :body keys,
  or an exception.

  If opts contains :body, It is assumed body is already in a format that can be
  sent directly in request (already encoded).

  Options
  - output-format - can be :text, :json, :edn or :binary (default :text), or
                    special format (wikidata) to handle wikidata errors, which
                    come back as html."
  [url opts]
  (let [{:keys [request-timeout token headers output-format]
         :or   {request-timeout 5000
                output-format   :text}} opts

        response-chan (async/chan)
        headers       (cond-> {}
                        headers (merge headers)
                        token   (assoc "Authorization" (str "Bearer " token)))]
    #?(:clj
       (async/thread
         (try
           (let [builder (-> (HttpRequest/newBuilder)
                             (.uri (URI/create url))
                             (.timeout (Duration/ofMillis request-timeout))
                             (.GET))]
             ;; Add headers
             (doseq [[k v] headers]
               (.header builder k v))

             ;; Add body if provided (for GET with body)
             (when-let [body (:body opts)]
               (.method builder "GET" (HttpRequest$BodyPublishers/ofString body)))

             (let [request                (.build builder)
                   ^HttpClient client     @http-client
                   ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))

                   status  (.statusCode response)
                   body    (.body response)
                   headers (get-headers response)]
               (if (< 300 status)
                 (if (= :wikidata output-format)
                   (let [error {:status  status
                                :message body
                                :error   :db/invalid-query}]
                     (async/put! response-chan error))
                   (async/put! response-chan
                               (ex-info (str "HTTP error response: " body)
                                        {:status status :body body :url url})))
                 (async/put! response-chan
                             {:status  status
                              :headers headers
                              :body    (case output-format
                                         (:text :json)    body
                                         (:edn :wikidata) (json/parse body)
                                         body)}))))
           (catch TimeoutException e
             (async/put! response-chan
                         (format-error-response url e)))
           (catch Exception e
             (async/put! response-chan
                         (format-error-response url e)))))
       :cljs (-> axios
                 (.request (clj->js {:url     url
                                     :method  "get"
                                     :timeout request-timeout
                                     :headers headers}))
                 (.then (fn [resp]
                          (let [resp-data (js->clj resp :keywordize-keys true)
                                data      (:data resp-data)
                                status    (:status resp-data)
                                headers   (js->clj (.-headers resp) :keywordize-keys false)]
                            (async/put! response-chan
                                        {:status  status
                                         :headers headers
                                         :body    (case output-format
                                                    :text data
                                                    :json (json/stringify data)
                                                    (throw (ex-info "http get only supports output formats of json and text." {})))}))))
                 (.catch (fn [err]
                           (async/put! response-chan (format-error-response url err))))))
    response-chan))

(defn get
  "Returns result body as a string, or an exception.

  If opts contains :body,
  It is assumed body is already in a format that can be sent directly in
  request (already encoded).

  Options
  - output-format - can be :text, :json, :edn or :binary (default :text),
                    or special format (wikidata) to handle wikidata errors,
                    which come back as html."
  [url opts]
  (go-try
    (:body (<? (get-response url opts)))))

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

(defn put-response
  "http PUT request returning full response map"
  [url body opts]
  (let [{:keys [request-timeout headers]
         :or   {request-timeout 5000}}
        opts

        response-chan (async/chan)]
    #?(:clj
       (async/thread
         (try
           (let [builder (-> (HttpRequest/newBuilder)
                             (.uri (URI/create url))
                             (.timeout (Duration/ofMillis request-timeout))
                             (.PUT (if (bytes? body)
                                     (HttpRequest$BodyPublishers/ofByteArray body)
                                     (HttpRequest$BodyPublishers/ofString body))))
                 builder (reduce-kv (fn [^HttpRequest$Builder b k v]
                                      (.header b k v))
                                    builder
                                    headers)
                 request (.build ^HttpRequest$Builder builder)

                 ^HttpClient client     @http-client
                 ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))

                 status    (.statusCode response)
                 body-resp (.body response)
                 headers   (get-headers response)]
             (if (< 300 status)
               (async/put! response-chan
                           (ex-info (str "HTTP error response: " body-resp)
                                    {:status status :body body-resp :url url}))
               (async/put! response-chan {:status  status
                                          :headers headers
                                          :body    body-resp})))
           (catch TimeoutException e
             (async/put! response-chan
                         (format-error-response url e)))
           (catch Exception e
             (async/put! response-chan
                         (format-error-response url e)))))
       :cljs (-> axios
                 (.request (clj->js {:url     url
                                     :method  "put"
                                     :data    body
                                     :timeout request-timeout
                                     :headers headers}))
                 (.then (fn [resp]
                          (let [resp-data (js->clj resp :keywordize-keys true)
                                data      (:data resp-data)
                                status    (:status resp-data)
                                headers   (js->clj (.-headers resp) :keywordize-keys false)]
                            (async/put! response-chan {:status  status
                                                       :headers headers
                                                       :body    data}))))
                 (.catch (fn [err]
                           (async/put! response-chan (format-error-response url err))))))
    response-chan))

(defn put
  "http PUT request returning response body or an error."
  [url body opts]
  (go-try
    (:body (<? (put-response url body opts)))))

(defn delete-response
  "http DELETE request returning full response map"
  [url opts]
  (let [{:keys [request-timeout headers]
         :or   {request-timeout 5000}}
        opts
        response-chan (async/chan)]
    #?(:clj
       (async/thread
         (try
           (let [builder                (-> (HttpRequest/newBuilder)
                                            (.uri (URI/create url))
                                            (.timeout (Duration/ofMillis request-timeout))
                                            (.DELETE))
                 builder                (reduce-kv (fn [^HttpRequest$Builder b k v]
                                                     (.header b k v))
                                                   builder
                                                   headers)
                 request                (.build ^HttpRequest$Builder builder)
                 ^HttpClient client     @http-client
                 ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))
                 status                 (.statusCode response)
                 body                   (.body response)
                 headers                (get-headers response)]
             (if (< 299 status)
               (async/put! response-chan
                           (format-error-response url
                                                  (ex-info "HTTP error response"
                                                           {:status status :body body})))
               (async/put! response-chan {:status  status
                                          :headers headers
                                          :body    body})))
           (catch TimeoutException e
             (async/put! response-chan
                         (format-error-response url e)))
           (catch Exception e
             (async/put! response-chan
                         (format-error-response url e)))))
       :cljs (-> axios
                 (.request (clj->js {:url     url
                                     :method  "delete"
                                     :timeout request-timeout
                                     :headers headers}))
                 (.then (fn [resp]
                          (let [resp-data (js->clj resp :keywordize-keys true)
                                data      (:data resp-data)
                                status    (:status resp-data)
                                headers   (js->clj (.-headers resp) :keywordize-keys false)]
                            (async/put! response-chan {:status  status
                                                       :headers headers
                                                       :body    data}))))
                 (.catch (fn [err]
                           (async/put! response-chan (format-error-response url err))))))
    response-chan))

(defn delete
  "http DELETE request returning just the body.
  Uses delete-response internally for backward compatibility."
  [url opts]
  (go-try
    (:body (<? (delete-response url opts)))))

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
   #?(:clj  (jws/close! ws)
      :cljs (.close ws)))
  ([ws reason-kw]
   #?(:clj  (jws/close! ws reason-kw)
      :cljs (let [code   (get-in ws-close-status-codes [reason-kw :code] 1000)
                  reason (get-in ws-close-status-codes [reason-kw :reason] "Normal closure")]
              (.close ws code reason)))))

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
            #?(:clj  @(jws/send-text! ws msg)
               :cljs (.send ws msg))
            (when resp-chan
              (async/put! resp-chan true)
              (async/close! resp-chan))
            (catch* e
              (log/error! ::ws-send-error e {:msg "Error sending websocket message"
                                             :payload msg})
              (log/error e "Error sending websocket message:" msg)
              (when resp-chan
                (async/put! resp-chan false))))
          (recur))))))

(declare try-socket)

(defn retry-socket
  "Attempts repeated retried to re-establish connection."
  [url sub-chan pub-chan timeout close-fn]
  (async/go-loop [retries 1]
    (let [retry-timeout (min (* retries 1000) 20000)
          ws            (async/<! (try-socket url sub-chan pub-chan timeout close-fn))]
      (when (util/exception? ws)
        (log/info "Unable to establish websocket connection, retrying in " retry-timeout "ms. "
                  "Reported websocket exception: " (ex-message ws))
        (async/<! (async/timeout (min (* retries 500) 10000))) ;; timeout maxes at 10s
        (recur (inc retries))))))

(defn abnormal-socket-close?
  [status-code]
  (= status-code (get-in ws-close-status-codes [:abnormal :code])))

(defn try-socket
  [url msg-in msg-out timeout close-fn]
  #?(:clj
     ;; Use Java 11 HttpClient WebSocket implementation
     (jws/websocket url {:msg-in msg-in
                         :msg-out msg-out
                         :connect-timeout timeout
                         :close-fn close-fn})

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
