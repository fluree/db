(ns fluree.db.util.xhttp
  (:refer-clojure :exclude [get])
  (:require #?@(:clj [[org.httpkit.sni-client :as sni-client]
                      [org.httpkit.client :as http]
                      [http.async.client :as ha]
                      [byte-streams :as bs]
                      [http.async.client.websocket :as ws]])
            #?@(:cljs [["axios" :as axios]
                       ["ws" :as NodeWebSocket]])
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [clojure.string :as str]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log])
  (:import #?@(:clj  ((org.httpkit.client TimeoutException)
                      (org.asynchttpclient.ws WebSocket))
               :cljs ((goog.net.ErrorCode)))))

#?(:clj (set! *warn-on-reflection* true))


;; enable SNI in http-kit
;; See https://github.com/http-kit/http-kit#enabling-client-sni-support-disabled-by-default
;; for details.
#?(:clj (alter-var-root #'http/*default-client*
                        (fn [_] sni-client/default-client)))


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
    #?(:clj (http/post url {:headers headers
                            :timeout request-timeout
                            :body    (json/stringify message)}
                       (fn [{:keys [error status body] :as response}]
                         (if (or error (< 299 status))
                           (do
                             (throw-if-timeout response)
                             (async/put!
                               response-chan
                               (format-error-response
                                 url
                                 (or error (ex-info "error response"
                                                    response)))))
                           (let [body (-> body bs/to-string json/parse)]
                             (async/put! response-chan body)))))
       :cljs
       (-> axios
           (.request (clj->js {:url     url
                               :method  "post"
                               :timeout request-timeout
                               :headers headers
                               :data    message}))
           (.then (fn [resp]
                    (async/put! response-chan (:data (js->clj resp :keywordize-keys true)))))
           (.catch (fn [err]
                     (async/put! response-chan (format-error-response url err))))))
    response-chan))


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


(defn socket-publish-loop
  [ws pub-chan]
  (async/go-loop []
    (let [val (async/<! pub-chan)]
      (if (nil? val)
        (log/info "Web socket pub/producer channel closed.")
        (let [[msg resp-chan] val]
          (try*
            #?(:clj  (ws/-sendText ^WebSocket ws msg)
               :cljs (.send ws msg))
            (async/put! resp-chan true)
            (catch* e
              (log/error e "Error sending websocket message:" msg)
              (async/put! resp-chan false)))
          (recur))))))


(defn close-websocket
  [ws]
  #?(:clj  (ha/close-websocket ws)
     :cljs (.close ws)))


(defn try-socket
  [url sub-chan pub-chan resp-chan timeout close-fn]
  #?(:clj
     (let [client (ha/create-client)
           ws     (ha/websocket client url
                                :timeout timeout
                                :close (fn [_ code reason]
                                         (log/debug "Websocket closed; code" code
                                                    "reason:" reason)
                                         (close-fn))
                                :error (fn [^WebSocket ws e]
                                         (log/error e "websocket error")
                                         (.sendCloseFrame ws)
                                         (close-fn)
                                         (when-not (nil? e) (async/put! resp-chan e)))
                                :text (fn [_ msg]
                                        (when-not (nil? msg)
                                          (async/put! sub-chan msg))))]
       (socket-publish-loop ws pub-chan)
       (async/put! resp-chan ws))

     :cljs
     (let [ws           (if (identical? *target* "nodejs")
                          (NodeWebSocket. url)
                          (js/WebSocket. url))
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
