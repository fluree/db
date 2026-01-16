# Fluree OpenTelemetry Tracing Guide

## Overview

Fluree can provide OpenTelemetry tracing data if a tracing collector
is configured. Open Telemetry traces are supported in a variety of
tools, and can provide insight into how the database is
performing.

Tracing is only available on the JVM build of Fluree.

### Basic Configuration

Specify the following Java system properties or environment
vars. System properties take precedence over environment vars.

| System Property             | Environment Var             | example value         |
|-----------------------------+-----------------------------+-----------------------|
| otel.exporter.otlp.endpoint | OTEL_EXPORTER_OTLP_ENDPOINT | http://localhost:4318 |
| otel.exporter.otlp.protocol | OTEL_EXPORTER_OTLP_PROTOCOL | http/protobuf         |
| otel.service.name           | OTEL_SERVICE_NAME           | my-service-name       |


Fluree does not support Open Telemetry logging or metrics at this time, so disable them:
| System Property       | Environment Var       | example value |
|-----------------------+-----------------------+---------------|
| otel.logs.exporter    | OTEL_LOGS_EXPORTER    | none          |
| otel.metrics.exporter | OTEL_METRICS_EXPORTER | none          |

### Testing
If you would like to see the tracing in action you can use the docker
image for the open-source tracing tool Jaeger.

This will start Jaeger in a docker container, collecting traces on
port 4318 and providing a user interface on port 16686.
```
docker run --rm -p 16686:16686 -p 4318:4318 jaegertracing/jaeger:2.11.0
```

Then start your Fluree application with the OpenTelemetry Java agent like this:
```
java -jar my-service.jar -javaagent:/path/to/opentelemetry-javaagent.jar
  \-Dotel.service.name=fluree-server
  \-Dotel.exporter.otlp.endpoint=http://localhost:4318
  \-Dotel.exporter.otlp.protocol=http/protobuf
  \-Dotel.logs.exporter=none
  \-Dotel.metrics.exporter=none
```

