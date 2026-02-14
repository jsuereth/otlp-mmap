# TODO - figure out how to build all language protobufs here.

ARG PROTOBUF_VERSION=33.0
# SHA256 checksum for protoc-33.0-linux-x86_64.zip
ARG PROTOBUF_SHA256=5cc0cf75a9c7d0ab6340d59fda3ca05d2efb2dcd7a323a5ba2b7211c2aba9d0f

FROM rust:1-alpine3.22@sha256:3c06253e433c1b2ac2c279a98226d385d25c5f324138ab2861a5414bfa6855f9
LABEL maintainer="The OpenTelemetry Authors"
# Install dependencies: curl and unzip for protoc download, musl-dev for Rust compilation
RUN apk add --no-cache curl unzip musl-dev

ARG PROTOBUF_VERSION
ARG PROTOBUF_SHA256

# Download protoc with SHA256 checksum verification
RUN mkdir -p /protoc && \
    cd /protoc && \
    curl -sSL -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    echo "${PROTOBUF_SHA256}  protoc.zip" | sha256sum -c - || { echo "ERROR: protoc checksum verification failed"; exit 1; } && \
    unzip protoc.zip && \
    rm protoc.zip && \
    chmod a+x /protoc/bin/protoc

ENV PROTOC=/protoc/bin/protoc

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
    chown -R appuser:appuser /mmap /protoc

WORKDIR /mmap
USER appuser
ENTRYPOINT ["/mmap/run_protoc.sh"]