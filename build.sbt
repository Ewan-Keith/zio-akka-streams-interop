lazy val scala211  = "2.11.12"
lazy val scala212  = "2.12.10"
lazy val scala213  = "2.13.1"
lazy val mainScala = scala213
lazy val allScala  = Seq(scala211, scala212, mainScala)

lazy val zioVersion = "1.0.0-RC18-2"

organization := "org.ekeith"
homepage := Some(url("https://github.com/Ewan-Keith/zio-akka-streams-interop"))
name := "zio-akka-streams-interop"
licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
scalaVersion := mainScala
parallelExecution in Test := false
fork in Test := true
pgpPublicRing := file("/tmp/public.asc")
pgpSecretRing := file("/tmp/secret.asc")
releaseEarlyWith := SonatypePublisher

developers := List(
  Developer(
    "ekeith",
    "Ewan Keith",
    "keith.ewan@yahoo.co.uk",
    url("https://github.com/ewan-keith")
  )
)

libraryDependencies ++= Seq(
  "dev.zio"           %% "zio"                   % zioVersion,
  "dev.zio"           %% "zio-streams"           % zioVersion,
  "com.typesafe.akka" %% "akka-stream"           % "2.6.1",
  "dev.zio"           %% "zio-test"              % zioVersion % "test",
  "dev.zio"           %% "zio-test-sbt"          % zioVersion % "test",
  compilerPlugin("org.typelevel" %% "kind-projector"     % "0.10.3"),
  compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-Yrangepos",
  "-feature",
  "-language:higherKinds",
  "-language:existentials",
  "-unchecked",
  "-Xlint:_,-type-parameter-shadow",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard"
) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, 11)) =>
    Seq(
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Xfuture"
    )
  case Some((2, 12)) =>
    Seq(
      "-Xsource:2.13",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-opt-inline-from:<source>",
      "-opt-warnings",
      "-opt:l:inline",
      "-Xfuture"
    )
  case _ => Nil
})

fork in run := true

crossScalaVersions := allScala

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
