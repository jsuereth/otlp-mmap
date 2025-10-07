
# The build image
FROM rust:1.88.0-alpine3.22 AS build
RUN apk add musl-dev
WORKDIR /build

# list out directories to avoid pulling local cargo `target/`
COPY Cargo.toml /build/Cargo.toml
COPY Cargo.lock /build/Cargo.lock
COPY src /build/src

# Build weaver
RUN cargo build --release

# The runtime image
FROM alpine:3.22
LABEL maintainer="The OpenTelemetry Authors"
# TODO - Don't run as root...
# RUN addgroup otel \
#   && adduser \
#   --ingroup otel \
#   --disabled-password \
#   otel
WORKDIR /home/otel
COPY --from=build /build/target/release/otlp-mmap-collector /otel/otlp-mmap-collector
# USER otel
ENTRYPOINT ["/otel/otlp-mmap-collector"]