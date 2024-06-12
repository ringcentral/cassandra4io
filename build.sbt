name := "cassandra4io"

inThisBuild(
  List(
    organization := "com.ringcentral",
    organizationName := "ringcentral",
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := crossScalaVersions.value.head,
    crossScalaVersions := Seq("2.13.4", "2.12.12")
  )
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "org.typelevel"       %% "cats-effect"                    % "2.3.1",
      "co.fs2"              %% "fs2-core"                       % "2.5.0",
      "com.datastax.oss"     % "java-driver-core"               % "4.9.0",
      "com.chuusai"         %% "shapeless"                      % "2.3.3"
    ) ++ Seq(
      "com.disneystreaming" %% "weaver-framework"               % "0.5.1"  % "it,test",
      "org.testcontainers"   % "testcontainers"                 % "1.15.1" % "it",
      "com.dimafeng"        %% "testcontainers-scala-cassandra" % "0.38.9" % "it"
    )
  )

Compile / compile / scalacOptions ++= Seq(
  "-encoding",
  "utf-8",
  "-feature",
  "-unchecked",
  "-deprecation"
) ++
  (scalaBinaryVersion.value match {
    case v if v.startsWith("2.13") =>
      List(
        "-Xlint:strict-unsealed-patmat",
        "-Xlint:-serial",
        // "-Ywarn-unused",
        "-Ymacro-annotations",
        "-Yrangepos",
        "-Werror",
        "-explaintypes",
        "-language:higherKinds",
        "-language:implicitConversions",
        "-Xfatal-warnings",
        "-Wconf:any:error"
      )
    case v if v.startsWith("2.12") =>
      Nil
    case v if v.startsWith("2.11") =>
      List("-target:jvm-1.8")
    case v if v.startsWith("0.")   =>
      Nil
    case other                     => sys.error(s"Unsupported scala version: $other")
  })

testFrameworks := Seq(new TestFramework("weaver.framework.TestFramework"))
