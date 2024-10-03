val scala3Version = "3.5.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "otlp-mmap",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk-trace" % "1.42.1",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
    
  )
