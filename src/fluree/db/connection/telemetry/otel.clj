(ns fluree.db.connection.telemetry.otel
  (:import
   [io.opentelemetry.api GlobalOpenTelemetry]
   [io.opentelemetry.api.baggage.propagation W3CBaggagePropagator]
   [io.opentelemetry.api.common Attributes]
   [io.opentelemetry.api.trace.propagation W3CTraceContextPropagator]
   [io.opentelemetry.context.propagation ContextPropagators TextMapPropagator]
   [io.opentelemetry.exporter.otlp.http.logs OtlpHttpLogRecordExporter]
   [io.opentelemetry.exporter.otlp.http.metrics OtlpHttpMetricExporter]
   [io.opentelemetry.exporter.otlp.http.trace OtlpHttpSpanExporter]
   [io.opentelemetry.sdk OpenTelemetrySdk]
   [io.opentelemetry.sdk.logs SdkLoggerProvider]
   [io.opentelemetry.sdk.logs.export BatchLogRecordProcessor]
   [io.opentelemetry.sdk.metrics SdkMeterProvider]
   [io.opentelemetry.sdk.metrics.export PeriodicMetricReader]
   [io.opentelemetry.sdk.resources Resource]
   [io.opentelemetry.sdk.trace SdkTracerProvider]
   [io.opentelemetry.sdk.trace.export BatchSpanProcessor]))

(defn configure-otel
  [{:keys [endpoint]}]
  (let [resource  (-> (Resource/getDefault)
                      (.merge (Resource/create (-> (Attributes/builder)
                                                   (.put "service.name" "fluree-db")
                                                   (.build)))))

        trace-exporter (-> (OtlpHttpSpanExporter/builder)
                           (.setEndpoint (str endpoint "/v1/traces"))
                           (.build))
        trace-provider (-> (SdkTracerProvider/builder)
                           (.setResource resource)
                           (.addSpanProcessor (.build (BatchSpanProcessor/builder trace-exporter)))
                           (.build))

        metric-exporter (-> (OtlpHttpMetricExporter/builder)
                            (.setEndpoint (str endpoint "/v1/metrics"))
                            (.build))
        metric-provider (-> (SdkMeterProvider/builder)
                            (.setResource resource)
                            (.registerMetricReader (.build (PeriodicMetricReader/builder metric-exporter)))
                            (.build))

        log-exporter    (-> (OtlpHttpLogRecordExporter/builder)
                            (.setEndpoint (str endpoint "/v1/logs"))
                            (.build))
        log-provider    (-> (SdkLoggerProvider/builder)
                            (.setResource resource)
                            (.addLogRecordProcessor (.build (BatchLogRecordProcessor/builder log-exporter)))
                            (.build))

        propagators     (ContextPropagators/create
                         (TextMapPropagator/composite
                          (doto (java.util.ArrayList.)
                            (.add (W3CTraceContextPropagator/getInstance))
                            (.add (W3CBaggagePropagator/getInstance)))))

        sdk  (-> (OpenTelemetrySdk/builder)
                 (.setTracerProvider trace-provider)
                 (.setMeterProvider metric-provider)
                 (.setLoggerProvider log-provider)
                 (.setPropagators propagators)
                 (.build))]
    (GlobalOpenTelemetry/resetForTest)
    (GlobalOpenTelemetry/set sdk)
    sdk))

(comment

  (GlobalOpenTelemetry/resetForTest)
  (def sdk (configure-otel {:protocol "http/protobuf" :endpoint "http://localhost:4318" :service-name "fluree-db"})))
