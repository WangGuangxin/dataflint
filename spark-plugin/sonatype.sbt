
import xerial.sbt.Sonatype._

ThisBuild / sonatypeCredentialHost := "central.sonatype.com"

ThisBuild / sonatypeTimeoutMillis := 600000 // 10 minutes

sonatypeProfileName := "io.github.wangguangxin"

ThisBuild / sonatypeProfileName := "io.github.wangguangxin"

ThisBuild / publishMavenStyle := true

ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))


ThisBuild / description := "Open Source Data-Application Performance Monitoring for Apache Spark, with RAPIDS GPU profiling and diagnosis"

ThisBuild / homepage := Some(url("https://github.com/WangGuangxin/dataflint"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/WangGuangxin/dataflint"),
    "scm:git@github.com:WangGuangxin/dataflint.git"
  )
)
ThisBuild / developers := List(
    Developer(
    id = "menishmueli",
    name = "Meni Shmueli",
    email = "menishmueli@gmail.com",
    url = url("http://dataflint.io")
  ),
  Developer(
    id = "WangGuangxin",
    name = "Wang Guangxin",
    email = "",
    url = url("https://github.com/WangGuangxin")
  )
)
