
val scala3Version = "3.9.0"
val otelVersion = "1.66.0"

lazy val mmapsdk =
  project
  .in(file("mmapsdk"))
  .settings(
    name := "mmap-sdk",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "4.36.2",
    libraryDependencies += "io.opentelemetry" % "opentelemetry-api" % otelVersion,
    libraryDependencies += "io.opentelemetry" % "opentelemetry-api-incubator" % s"${otelVersion}-alpha"
  ).disablePlugins(AssemblyPlugin)

lazy val root = project
  .in(file("."))
  .settings(
    name := "otlp-mmap",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk" % otelVersion,
    // libraryDependencies += "io.opentelemetry" % "opentelemetry-exporter-otlp-common" % "1.42.1",
    libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % otelVersion,
    libraryDependencies += "io.opentelemetry" % "opentelemetry-exporter-otlp" % "1.66.0",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.22.2",
    libraryDependencies += "org.scalameta" %% "munit" % "1.3.6" % Test,
    libraryDependencies += "io.opentelemetry.instrumentation" % "opentelemetry-java-http-server" % "2.21.0-alpha",
    libraryDependencies += "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java17" % "2.21.0-alpha",
    assembly / assemblyJarName := "demo.jar",
    assembly / assemblyMergeStrategy := {
       case PathList("META-INF", "versions", _*)           => MergeStrategy.preferProject
       case PathList("META-INF", "okio.kotlin_module", _*) => MergeStrategy.preferProject
       case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / mainClass := Some("demo")
  ).dependsOn(mmapsdk).aggregate(mmapsdk)
