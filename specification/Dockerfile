# TODO - figure out how to build all language protobufs here.

FROM rust:1-alpine3.22@sha256:064dfc925d68d1a63f4fd2871bd7dc6e6ea56692989a487185855d62885d90aa
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