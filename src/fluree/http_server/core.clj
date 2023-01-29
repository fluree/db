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
   [ring.adapter.jetty9 :as jetty9]
   [fluree.db.util.log :as log]))

(defn ws-send!
  [ws msg]
  (jetty9/send! ws msg))

(defn websocket-handler
  [ws-callbacks]
  (let [{:keys [on-connect on-text on-bytes on-close on-ping on-pong on-error]} ws-callbacks]
    (fn [req]
      (if (jetty9/ws-upgrade-request? req)
        (jetty9/ws-upgrade-response
          (fn [upgrade-request]
            (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
                  provided-extensions (:websocket-extensions upgrade-request)]
              {:on-connect (fn on-connect [ws]
                             (when on-connect (on-connect ws)))
               :on-text (fn on-text [ws text-message]
                          (when on-text (on-text ws text-message)))
               :on-bytes (fn on-bytes [ws bytes offset len]
                           (when on-bytes (on-bytes ws bytes offset len)))
               :on-error (fn on-error [ws error]
                           (when on-error (on-error ws error)))
               :on-close (fn on-close [ws status-code reason]
                           (when on-close (on-close ws status-code reason)))
               :on-ping (fn on-ping [ws bytebuffer]
                          (when on-ping (on-ping ws bytebuffer)))
               :on-pong (fn on-pong [ws bytebuffer]
                          (when on-pong (on-pong ws bytebuffer)))
               :subprotocol (first provided-subprotocols)
               :extensions provided-extensions})))
        {:status 400
         :body "Invalid websocket upgrade request"}))))

(defn app
  [routes]
  (ring/ring-handler
    (ring/router
      [routes
       ["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "API Docs"}}
               :handler (swagger/create-swagger-handler)}}]]
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
      (swagger-ui/create-swagger-ui-handler
        {:path "/api-docs"
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler))))

(defn start
  [config]
  (log/info "Starting http-server." config)
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
