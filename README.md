# pg-tracing-otel-forwarder

Simple application to forward pg_tracing's spans to an otel collector.

## Instructions

### Start Jaeger and otel collector
From the `docker` director, start the otel collector and jaeger with `docker-compose up`.

Jaeger interface should be available on http://localhost:16686/

### Build forwarder
To build the forwarder, use `go build`

### Run the forwarder
You can pass a connection string with the `DATABASE_URL` environment variable

```
DATABASE_URL="host=127.0.0.1 port=5432 user=postgres password=postgres dbname=my_db" ./pg-tracing-forwarder-otel
```

Running the forwarder will fetch consume all spans with `pg_tracing_consume_spans` and send them to the otel collector on port 4317.
