scalaVersion in ThisBuild := "2.11.7"


val jacksonVersion = "2.4.4"//!!!!Need to Match Spark Version
resolvers += Resolver.sonatypeRepo("public")

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
  ("org.apache.spark" %% "spark-core" % "1.5.2" % "provided")
)

val processing = Project("processing", file("processing"))
  .settings(
    libraryDependencies ++= sparkDependencies ++ commonTestDependencies ++ Seq(
      "com.github.scopt" %% "scopt" % "3.4.0"
    ),
    mainClass in Compile := Some("org.bjean.sample.wordcount.processor.WordCountApp")
  )
  .settings(publishFatJar : _*)

val inputGenerator = Project("input-generator", file("input-generator"))
    .settings(
      libraryDependencies ++= commonTestDependencies ++ Seq(
        "org.apache.commons" % "commons-lang3" % "3.3.2",
        "com.vtence.cli"%"cli"%"1.1"
      ),
      mainClass in Compile := Some("org.bjean.sample.wordcount.input.DocumentGenerator")
    )
    .settings(publishFatJar : _*)

val root = Project("word-count-spark-aws", file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(noPublish: _*)
  .aggregate(inputGenerator,processing)
