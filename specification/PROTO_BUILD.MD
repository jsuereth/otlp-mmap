# Building Protocol buffers

We use a Docker-based workflow to generate the protocol definitions for all supported languages. This ensures consistency and avoids requiring local installations of `protoc` or specific language plugins.

## 1. Build the Generator Image

Navigate to the `specification` directory and build the image:

```bash
cd specification
docker build . -t mmap_proto_build
```

## 2. Generate the Files

Create temporary directories for the output and run the generator from the project root:

```bash
# From the project root
mkdir -p specification/proto_rust specification/proto_java

docker run --rm \
  -v "$(pwd)/specification/proto_rust:/mmap/generated_rust" \
  -v "$(pwd)/specification/proto_java:/mmap/generated_java" \
  mmap_proto_build
```

## 3. Update the Source Code

Copy the generated files into the respective modules:

### Rust
```bash
cp specification/proto_rust/opentelemetry.proto.mmap.v1.rs rust/crates/otlp-mmap-protocol/src/lib.rs
```
*Note: Prepend `//! OTLP MMap definitions.` to the top of the file if needed.*

### Java
```bash
cp specification/proto_java/opentelemetry/proto/mmap/v1/Mmap.java java/otlp-mmap/mmapsdk/src/main/java/opentelemetry/proto/mmap/v1/Mmap.java
```

## 4. Cleanup

```bash
rm -rf specification/proto_rust specification/proto_java
```
