# TODO - figure out how to build all language protobufs here.

FROM rust:1-alpine3.22@sha256:3c06253e433c1b2ac2c279a98226d385d25c5f324138ab2861a5414bfa6855f9
LABEL maintainer="The OpenTelemetry Authors"
# Install protoc from Alpine package manager for better security
# This leverages Alpine's package signing and verification mechanisms
RUN apk add --no-cache protoc musl-dev

# Make directories we need.
RUN mkdir -p /mmap/rust


COPY docker_builder/rust /mmap/rust
RUN cd /mmap/rust && cargo build

# Move our script to build the proto into the image
COPY docker_builder/run_protoc.sh /mmap/run_protoc.sh
RUN chmod a+x /mmap/run_protoc.sh

# Move proto files into the image.
COPY mmap.proto /mmap

# Create non-root user for security
RUN addgroup -g 10001 appuser && \
    adduser -D -u 10001 -G appuser appuser && \
    chown -R appuser:appuser /mmap

WORKDIR /mmap
USER appuser
ENTRYPOINT ["/mmap/run_protoc.sh"]