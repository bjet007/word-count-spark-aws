scalaVersion in ThisBuild := "2.11.7"


val jacksonVersion = "2.4.4"//!!!!Need to Match Spark Version

lazy val commonTestDependencies = Seq(
  "org.scalatest" %% "scalatest" % "2.1.7" % Test
)

def noPublish = Seq(
     publish := {},
     publishLocal := {}
   )
val moduleName = Def.setting {
     Artifact(name.value)
}

def publishFatJar() = Seq(
         addArtifact(moduleName, assembly)
)

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
)

val processing = Project("processing", file("processing"))
  .settings(
    libraryDependencies ++= sparkDependencies ++ commonTestDependencies ++ Seq(
    ),
    mainClass in Compile := Some("org.bjean.sample.wordcount.processing.WordCount")
  )
  .settings(publishFatJar : _*)

val inputGenerator = Project("input-generator", file("input-generator"))
    .settings(
      libraryDependencies ++= commonTestDependencies ++ Seq(
        "org.apache.commons" % "commons-lang3" % "3.4"
      ),
      mainClass in Compile := Some("org.bjean.sample.wordcount.input.DocumentGenerator")
    )
    .settings(publishFatJar : _*)

val root = Project("word-count-spark-aws", file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(noPublish: _*)
  .aggregate(inputGenerator,processing)
