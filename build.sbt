// *****************************************************************************
// Projects
// *****************************************************************************

lazy val sbe =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin, GitVersioning)
    .settings(settings)
    .settings(
      libraryDependencies ++= Seq(
        "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.6",
        library.scalaCheck % Test,
        library.scalaTest  % Test
      )
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val scalaCheck = "1.13.4"
      val scalaTest  = "3.0.1"
    }
    val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck
    val scalaTest  = "org.scalatest"  %% "scalatest"  % Version.scalaTest
}

// *****************************************************************************
// Settings
// *****************************************************************************        |

lazy val settings =
  commonSettings ++
  scalafmtSettings ++
  gitSettings ++
  headerSettings

lazy val commonSettings =
  Seq(
    scalaVersion := "2.11.8",
    //crossScalaVersions := Seq(scalaVersion.value, "2.11.8"),
    organization := "net.karana",
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-target:jvm-1.8",
      "-encoding", "UTF-8"
    ),
    javacOptions ++= Seq(
      "-source", "1.8",
      "-target", "1.8"
    ),
    unmanagedSourceDirectories.in(Compile) :=
      Seq(scalaSource.in(Compile).value),
    unmanagedSourceDirectories.in(Test) :=
      Seq(scalaSource.in(Test).value)
)

lazy val scalafmtSettings =
  reformatOnCompileSettings ++
  Seq(
    formatSbtFiles := false,
    scalafmtConfig :=
      Some(baseDirectory.in(ThisBuild).value / ".scalafmt.conf"),
    ivyScala :=
      ivyScala.value.map(_.copy(overrideScalaVersion = sbtPlugin.value)) // TODO Remove once this workaround no longer needed (https://github.com/sbt/sbt/issues/2786)!
  )

lazy val gitSettings =
  Seq(
    git.useGitDescribe := true
  )

import de.heikoseeberger.sbtheader.HeaderPattern
import de.heikoseeberger.sbtheader.license.Apache2_0
lazy val headerSettings =
  Seq(
    headers := Map(
      "scala" -> (HeaderPattern.cStyleBlockComment,
                  """|/*
                     | * Copyright year author
                     | */""".stripMargin)
    )
  )
