val scala3Version = "3.5.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "otlp-mmap",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk-trace" % "1.42.1",
    // libraryDependencies += "io.opentelemetry" % "opentelemetry-exporter-otlp-common" % "1.42.1",
    libraryDependencies += "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % "1.42.1",
    libraryDependencies += "io.opentelemetry" % "opentelemetry-exporter-otlp" % "1.42.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.18.0",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    assembly / assemblyJarName := "demo.jar",
    assembly / assemblyMergeStrategy := {
       case PathList("META-INF", "versions", _*)           => MergeStrategy.preferProject
       case PathList("META-INF", "okio.kotlin_module", _*) => MergeStrategy.preferProject
       case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / mainClass := Some("demo")
  )
