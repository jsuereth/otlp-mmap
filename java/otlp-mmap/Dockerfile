FROM sbtscala/scala-sbt:eclipse-temurin-25_36_1.11.7_3.7.3@sha256:4b3c50ee0f31825fae62bdd590c7363affe18be4b902d450b773a7fe5461e8b3 AS build
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
FROM eclipse-temurin:25@sha256:ddd55eda5ad0ef851a6c6b5169a83d6f9c9481449de77ae511a3118a3cf8fe91
LABEL maintainer="The OpenTelemetry Authors"

# Create non-root user for security
RUN groupadd -r -g 10001 appuser && \
    useradd -r -u 10001 -g appuser appuser

WORKDIR /home/otel
# TODO - assembly jar
COPY --from=build --chown=appuser:appuser /build/target/scala-3.7.3/demo.jar /otel/demo.jar

USER appuser
CMD ["java", "-jar", "/otel/demo.jar"]

