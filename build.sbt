name := "IntegrationStagingProject"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "com.typesafe"        % "config"                          % "1.3.2",
  "org.scalatest"       % "scalatest_2.11"                  % "2.2.2"     % "test",
  "com.storm-enroute"   %% "scalameter"                     % "0.8.2",
  "org.apache.spark"    % "spark-core_2.11"                 % "2.2.0",
  "org.apache.spark"    % "spark-sql_2.11"                  % "2.2.0",
  "org.apache.hadoop"   % "hadoop-common"                   % "2.7.0",
  "org.apache.spark"    % "spark-sql_2.11"                  % "2.2.0",
  "org.apache.spark"    % "spark-hive_2.11"                 % "2.2.0",
  "org.apache.spark"    % "spark-yarn_2.11"                 % "2.2.0",
  "org.apache.spark"    % "spark-streaming_2.11"            % "2.2.0",
  "org.apache.spark"    % "spark-streaming-kafka-0-8_2.11"  % "2.2.0",
  "org.apache.kudu"     % "kudu-spark2_2.11"                % "1.5.0",
  "com.yammer.metrics"  % "metrics-core"                    % "2.2.0",
  "org.scalatra"        %% "scalatra-scalatest"             % "2.6.2"    % "test"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
parallelExecution in Test := false