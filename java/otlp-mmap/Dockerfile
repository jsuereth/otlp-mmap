FROM sbtscala/scala-sbt:eclipse-temurin-25_36_1.11.7_3.7.3 AS build
WORKDIR /build

# COPY IMPORTANT FILES
COPY build.sbt /build/build.sbt
COPY src /build/src
COPY mmapsdk/src /build/mmapsdk/src
COPY project/build.properties /build/project/build.properties
COPY project/plugins.sbt /build/project/plugins.sbt

# Build
RUN sbt assembly

# The runtime Image
FROM eclipse-temurin:25
LABEL maintainer="The OpenTelemetry Authors"
WORKDIR /hom/otel
# TODO - assembly jar
COPY --from=build /build/target/scala-3.7.3/demo.jar /otel/demo.jar
CMD ["java", "-jar", "/otel/demo.jar"]

