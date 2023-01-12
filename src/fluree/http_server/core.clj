(ns fluree.http-server.core
  (:require
   [fluree.common.model :as model]
   [fluree.http-server.model :as server-model]
   [muuntaja.core :as muun]
   [reitit.coercion.malli]
   [reitit.ring :as ring]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.adapter.jetty9 :as jetty9]))

(defn websocket-handler
  [upgrade-request]
  (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
        provided-extensions (:websocket-extensions upgrade-request)]
    {;; provide websocket callbacks
     :on-connect (fn on-connect [_]
                   (tap> [:ws :connect]))
     :on-text (fn on-text [ws text-message]
                (tap> [:ws :msg text-message])
                (jetty9/send! ws (str "echo: " text-message)))
     :on-bytes (fn on-bytes [_ _ _ _]
                 (tap> [:ws :bytes]))
     :on-close (fn on-close [_ status-code reason]
                 (tap> [:ws :close status-code reason]))
     :on-ping (fn on-ping [ws payload]
                (tap> [:ws :ping])
                (jetty9/send! ws payload))
     :on-pong (fn on-pong [_ _]
                (tap> [:ws :pong]))
     :on-error (fn on-error [_ e]
                 (tap> [:ws :error e]))
     :subprotocol (first provided-subprotocols)
     :extensions provided-extensions}))

(defn app
  [routes]
  (ring/ring-handler
    (ring/router
      routes
      {:data {:coercion reitit.coercion.malli/coercion
              :muuntaja (muun/create)
              :middleware [swagger/swagger-feature
                           muuntaja/format-negotiate-middleware
                           muuntaja/format-response-middleware
                           (exception/create-exception-middleware
                             {::exception/default
                              (partial exception/wrap-log-to-console
                                       exception/default-handler)})
                           muuntaja/format-request-middleware
                           coercion/coerce-response-middleware
                           coercion/coerce-request-middleware]}})
    (ring/routes
      (ring/ring-handler
        (ring/router
          ["/ws" {:get (fn [req]
                         (if (jetty9/ws-upgrade-request? req)
                           (jetty9/ws-upgrade-response websocket-handler)
                           {:status 400
                            :body "Invalid websocket upgrade request"}))}]))
      (swagger-ui/create-swagger-ui-handler
        {:path "/"
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler))))

(defn start
  [config]
  (if-let [validation-error (model/explain server-model/HttpServerConfig config)]
    (throw (ex-info "Invalid publisher config." {:errors (model/report validation-error)
                                                 :config config}))
    (jetty9/run-jetty
      (app (:http/routes config))
      {:port (:http/port config)
       :join? false})))

(defn stop
  [http-server]
  (jetty9/stop-server http-server))


(comment
  (def s (start {:http/port 8888
                 :http/routes
                 [["/swagger.json"
                   {:get {:no-doc true
                          :swagger {:info {:title "my api"}}
                          :handler (swagger/create-swagger-handler)}}]
                  ["/api"
                   ["/hey" {:post {:summary "Hey"
                                   :handler (fn [req] {:status 200 :body "Yo."})}}]]]}))

  (stop s)

  )
