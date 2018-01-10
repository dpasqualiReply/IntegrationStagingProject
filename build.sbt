name := "IntegrationStagingProject"

version := "0.1"

scalaVersion := "2.11.11"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test",
  "com.storm-enroute" %% "scalameter" % "0.8.2"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
)