# TODO - figure out how to build all language protobufs here.

ARG PROTOBUF_VERSION=33.0
FROM rust:1-alpine3.22
LABEL maintainer="The OpenTelemetry Authors"
RUN apk add --no-cache curl
RUN apk add --no-cache musl-dev
ARG PROTOBUF_VERSION
RUN mkdir -p /protoc && \
    cd /protoc && \
    curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip | unzip -
RUN chmod a+x /protoc/bin/protoc
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

WORKDIR /mmap
ENTRYPOINT ["/mmap/run_protoc.sh"]