#!/bin/ash

mkdir -p /mmap/generated_java
# Builds the JAVA code
/protoc/bin/protoc --java_out=/mmap/generated_java --proto_path=. mmap.proto 
# Builds the RUST code
/mmap/rust/target/debug/otlp-mmap-proto-build