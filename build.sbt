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

val commonTest = Project("common-test", file("common-test"))
  .settings(
    libraryDependencies ++= commonTestDependencies ++ Seq(  )
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
    .dependsOn(commonTest % "test->test")
    .settings(
      libraryDependencies ++= commonTestDependencies ++ Seq(
        "org.apache.commons" % "commons-lang3" % "3.3.2",
        "com.vtence.cli"%"cli"%"1.1"
      ),
      mainClass in Compile := Some("org.bjean.sample.wordcount.input.DocumentGenerator")
    )
    .settings(publishFatJar : _*)


val awsLauncher = Project("aws-launcher", file("aws-launcher"))
  .dependsOn(commonTest % "test->test")
  .settings(
    libraryDependencies ++= sparkDependencies ++ commonTestDependencies ++ Seq(
      "com.amazonaws"       % "aws-java-sdk-core" % "1.9.+",
      "com.amazonaws"       % "aws-java-sdk-ec2" % "1.9.+",
      "com.amazonaws"       % "aws-java-sdk-emr" % "1.9.+",
      "com.amazonaws"       % "aws-java-sdk-s3" % "1.9.+",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      "com.github.scopt" %% "scopt" % "3.4.0",
      "com.google.guava" % "guava-testlib" % "16.0.1" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test"
    ),
    mainClass in Compile := Some("org.bjean.sample.wordcount.aws.AwsLauncher")
  )
  .settings(publishFatJar : _*)

val root = Project("word-count-spark-aws", file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(noPublish: _*)
  .aggregate(inputGenerator,processing,awsLauncher)
