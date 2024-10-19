FROM sbtscala/scala-sbt:eclipse-temurin-alpine-21.0.2_13_1.10.2_3.5.1 AS build
WORKDIR /build

# COPY IMPORTANT FILES
COPY build.sbt /build/build.sbt
COPY src /build/src
COPY project/build.properties /build/project/build.properties
COPY project/plugins.sbt /build/project/plugins.sbt

# Build
RUN sbt assembly

# The runtime Image
FROM eclipse-temurin:21
LABEL maintainer="The OpenTelemetry Authors"
WORKDIR /hom/otel
# TODO - assembly jar
COPY --from=build /build/target/scala-3.5.0/demo.jar /otel/demo.jar
CMD ["java", "-jar", "/otel/demo.jar"]

