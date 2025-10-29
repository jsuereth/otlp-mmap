OTEL_PROTO_CONTAINER=otel/build-protobuf:latest

scalaproto:
	docker run --rm --mount \
	"type=bind,source=$(PWD)/java/otlp-mmap/src/main/java,target=/src_java" \
	--mount "type=bind,source=$(PWD)/specification,target=/src_proto,readonly" \
	otel/build-protobuf:latest \
	--java_out=/src_java \
	--proto_path=/src_proto \
	/src_proto/mmap.proto