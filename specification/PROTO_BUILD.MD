# Building Protocol buffers

We created a simple docker image that can eek out the proto
definitions we need without forcing you to install anything
more than docker.


## Build the image

```
cd ${OTLP_MMAP_DIR}/specification
docker build . -t mmap_proto_build
```

## Output the files

```
docker run mmap_proto_build \
  --mount 'type=bind,source=$(OTLP_MMAP_DIR)/proto_rust,target=/mmap/generated_rust' \
  --mount 'type=bind,source=$(OTLP_MMAP_DIR)/proto_java,target=/mmap/generated_java'
```