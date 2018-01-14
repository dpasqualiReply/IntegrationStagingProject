import java.io.File

import com.typesafe.config._
import it.reply.data.pasquali.Storage
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.util.Properties
import sys.process._
import org.apache.log4j.Logger

class DevOpsSystemSpec extends FlatSpec with BeforeAndAfterAll{

  var CONF_DIR = ""
  var PSQL_PASS_FILE = ""

  var SPARK_MASTER = "local[*]"
  var SPARK_APPNAME = "Integration Tests"

  var configuration : Config = null

  var storage : Storage = Storage()
  var connectionUrl = ""

  var PSQL_PASSWORD = ""

  var hiveMovies : DataFrame = null
  var hiveLinks : DataFrame = null
  var hiveGTags : DataFrame = null

  var kuduMovies : DataFrame = null
  var kuduGTags : DataFrame = null

  var HIVE_MOVIES_TABLE = ""
  var HIVE_LINKS_TABLE = ""
  var HIVE_GTAGS_TABLE = ""
  var HIVE_TAGS_TABLE = ""
  var HIVE_RATINGS_TABLE = ""
  var HIVE_DATABASE = ""

  var KUDU_MOVIES_TABLE = ""
  var KUDU_LINKS_TABLE = ""
  var KUDU_GTAGS_TABLE = ""
  var KUDU_TAGS_TABLE = ""
  var KUDU_RATINGS_TABLE = ""

  var KUDU_ADDR = ""
  var KUDU_PORT = ""
  var KUDU_TABLE_BASE = ""
  var KUDU_DATABASE = ""

  var MODEL_ARCHIVE_PATH = ""

  var log : Logger = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    CONF_DIR = Properties.envOrElse("DEVOPS_CONF_DIR", "conf")

    // Setup logger
    log = Logger.getLogger(getClass.getName)


    log.info("----- Assuming that we are running " +
      "on staging devops-worker machine -----")
    log.info("----- Assuming that in the cloudera-vm " +
      "production machine exists some data in PSQL database -----")
    log.info("----- Assuming that sqoop bulk already runs and" +
      " there are some movies, links and gtags data in Hive datalake -----")
    log.info("----- Assuming there are test version of datalake and datamart databases -----")
    log.info("-----  -----")
    log.info("-----  -----")
    log.info("-----  -----")

    //**************************************************************************************

    log.info("----- Load Environment Config variables from file -----")

    println("\n")
    log.info(s"----- $CONF_DIR -----")
    println("\n")

    configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/RealTimeETL.conf"))

    KUDU_ADDR = configuration.getString("rtetl.kudu.address")
    KUDU_PORT = configuration.getString("rtetl.kudu.port")
    KUDU_TABLE_BASE = configuration.getString("rtetl.kudu.table_base")
    KUDU_DATABASE = configuration.getString("rtetl.kudu.database")

    log.info(s"----- Kudu Conf loaded -----")

    configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/BatchETL.conf"))

    HIVE_MOVIES_TABLE = configuration.getString("betl.hive.input.movies")
    HIVE_LINKS_TABLE = configuration.getString("betl.hive.input.links")
    HIVE_GTAGS_TABLE = configuration.getString("betl.hive.input.gtags")
    HIVE_DATABASE = configuration.getString("betl.hive.database")


    log.info(s"----- Hive Conf loaded -----")

    //**************************************************************************************

    log.info("----- Start confluent schema registry -----")

    // Run Confluent
    "confluent start schema-registry" !!

    log.info("----- Start Kafka JDBC Connector and populate topics -----")

    // Run My JDBC Connector
    "/opt/kafka-JDBC-connector/run.sh" !

    log.info("----- Stage environment initialized -----")

    log.info("----- Initialize Spark throught the storage class -----")
    log.info("----- Enable both Hive and Kudu support -----")

    // Initialize Spark
    storage.init(SPARK_MASTER, SPARK_APPNAME, true)
      .initKudu(KUDU_ADDR, KUDU_PORT, KUDU_TABLE_BASE)

    log.info("----- Init Done -----")
  }

  // -----------------------------------------------------------------------------
  // Sqoop Bulk tests --------------------------------------------------

  "Sqoop" must "load movies, links and genometags in hive datalake" in {

    log.info("----- Select movies, links and genometags tables from hive -----")

    var hiveMoviesExists = storage.runHiveQuery(s"show tables in $HIVE_DATABASE like '$HIVE_MOVIES_TABLE'")
    var hiveLinksExists = storage.runHiveQuery(s"show tables in $HIVE_DATABASE like '$HIVE_LINKS_TABLE'")
    var hiveGtagsExists = storage.runHiveQuery(s"show tables in $HIVE_DATABASE like '$HIVE_GTAGS_TABLE'")

    log.info("----- Tables must exists -----")

    assert(hiveMoviesExists.collect().length == 1)
    assert(hiveLinksExists.collect().length == 1)
    assert(hiveGtagsExists.collect().length == 1)

    hiveMovies = storage.runHiveQuery(s"select * from $HIVE_DATABASE.$HIVE_MOVIES_TABLE limit 10")
    hiveLinks = storage.runHiveQuery(s"select * from $HIVE_DATABASE.$HIVE_LINKS_TABLE limit 10")
    hiveGTags = storage.runHiveQuery(s"select * $HIVE_DATABASE.$HIVE_GTAGS_TABLE limit 10")

    log.info("----- And contains at least 10 elements -----")

    assert(hiveMovies.count() == 10)
    assert(hiveLinks.count() == 10)
    assert(hiveGTags.count() == 10)
  }

  // -----------------------------------------------------------------------------
  // Kafka JDBC Connector Tests --------------------------------------------------

  "Start a Kakfa console consumer" must
    "retrieve a lot of data, including rating and tag sample" in {

    log.info("----- Start Kafka Console Stream to check that Kafka JDBC connector is running properly -----")

    var kafkaConsoleStream = Process("/opt/kafka-JDBC-connector/debugConsoleConsumer.sh psql-m20-tags").lineStream

    var tagSample = """{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"string","optional":true,"field":"tag"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"tags"},"payload":{"id":5120,"userid":1741,"movieid":246,"tag":"setting:Chicago","timestamp":"1186434000"}}
    """.stripMargin

    var ratingSample = """
      {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"double","optional":true,"field":"rating"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"ratings"},"payload":{"id":39478,"userid":153,"movieid":508,"rating":4.5,"timestamp":"1101142930"}}
    """.stripMargin

    log.info("----- Check that the stream continas sample values for both tags and ratingss -----")

    assert(kafkaConsoleStream.contains(tagSample))
    assert(kafkaConsoleStream.contains(ratingSample))
  }

  // -----------------------------------------------------------------------------
  // Configurations Tests --------------------------------------------------------

  "Config folder" must "contains conf for Batch ETL" in {

    log.info(s"----- Check that $CONF_DIR conf dir contains configurations for Batch ETL -----")

    assert(new File(s"$CONF_DIR/BatchETL.conf").exists())
    assert(new File(s"$CONF_DIR/BatchETL_staging.conf").exists())

  }

  it must "contains conf for Real Time ETL" in {

    log.info(s"----- Check that $CONF_DIR conf dir contains configurations for Real Time ETL -----")

    assert(new File(s"$CONF_DIR/RealTimeETL.conf").exists())
    assert(new File(s"$CONF_DIR/RealTimeETL_staging.conf").exists())
  }

  it must "contains conf for Batch ML" in {

    log.info(s"----- Check that $CONF_DIR conf dir contains configurations for Batch ML -----")

    assert(new File(s"$CONF_DIR/BatchML.conf").exists())
    assert(new File(s"$CONF_DIR/BatchML_staging.conf").exists())
  }

  it must "contains conf for Real Time ML" in {

    log.info(s"----- Check that $CONF_DIR conf dir contains configurations for Real Time ML -----")

    assert(new File(s"$CONF_DIR/RealTimeML.conf").exists())
    assert(new File(s"$CONF_DIR/RealTimeML_staging.conf").exists())
  }
  // -----------------------------------------------------------------------------
  // Spark Initialization --------------------------------------------------------

  "Spark Session" must "be initialized" in {

    log.info("----- Check that spark session is running -----")

    assert(storage.spark != null)
  }

  // -----------------------------------------------------------------------------

  // -----------------------------------------------------------------------------
  // Batch ETL Tests -------------------------------------------------------------

  "The Batch ETL run" should "take some times" in {

    log.info("----- Run Batch ETL process from fat jar in lib folder -----")

    var betlStream = Process("spark-submit --master local --class BatchETL lib/BatchETL-assembly-0.1.jar").lineStream

    var done = false

    for{
      line <- betlStream
      !done
    }{
      println(line)
      done = line.contains("BATCH ETL PROCESS DONE")
    }
  }

  "The Batch ETL process" must "store data to KUDU datalake" in {

    log.info("----- kudu tables must contains values -----")

    kuduMovies = storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_MOVIES_TABLE").cache()
    kuduGTags = storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_GTAGS_TABLE").cache()

    assert(kuduMovies.count() >= 1)
    assert(kuduMovies.count() >= 1)
  }

  it must "store gtags as they are" in {

    log.info("----- Tag values in right format -----")

    var df = kuduMovies.where("tagid == 1")
    assert(df.count() == 1)
    assert(df.columns.contains("tagid"))
    assert(df.columns.contains("tag"))
  }

  it must "merge tmdb links and movies" in {

    log.info("----- Movie values in right format -----")

    var df = kuduMovies.where("movieid ==1")
    assert(df.count() == 1)
    assert(df.columns.contains("movieid"))
    assert(df.columns.contains("title"))
    assert(df.columns.contains("genres"))
    assert(df.columns.contains("link"))
  }

  // -----------------------------------------------------------------------------
  // Real Time ETL ---------------------------------------------------------------

  "The RealTime ETL run" should "take some times to process tags and ratings streams" in {

    log.info("----- Run Real Time ETL process for ratings stream -----")

    var rtetlStreamRatings = Process("spark-submit --master local --class it.reply.data.pasquali.StreamMono lib/RealTimeETL-assembly-0.1.jar psql-m20-ratings smallest").lineStream

    var done = false

    for{
      line <- rtetlStreamRatings
      !done
    }{
      println(line)
      done = line.contains("Empty RDD")
    }

    log.info("----- Run Real Time ETL process for tags stream -----")

    var rtetlStreamTags = Process("spark-submit --master local --class it.reply.data.pasquali.StreamMono lib/RealTimeETL-assembly-0.1.jar psql-m20-tags smallest").lineStream
    done = false

    for{
      line <- rtetlStreamTags
      !done
    }{
      println(line)
      done = line.contains("Empty RDD")
    }
  }

  // RATINGS --------------------------------

  "The ratings table" must "exists in Hive datalake" in {
    assert(storage.runHiveQuery(s"show tables in $HIVE_DATABASE like $HIVE_RATINGS_TABLE").count() == 1)
  }

  it must "contains transformed ratings" in {

    var rats = storage.runHiveQuery(s"select * from $HIVE_DATABASE.$HIVE_RATINGS_TABLE limit 10").cache()

    assert(rats.count() == 10)

    assert(rats.columns.contains("id"))
    assert(rats.columns.contains("userid"))
    assert(rats.columns.contains("movieid"))
    assert(rats.columns.contains("rating"))
    assert(rats.columns.contains("timestammp"))

    "\t1\t2\t3.5\t1112486027"

    val r = rats.where("id == 1").collect()(0)

    assert(r(0) == 1)
    assert(r(1) == 1)
    assert(r(2) == 2)
    assert(r(3) == 3.5)
    assert(r(4) == "1112486027")
  }


  "The ratings table" must "exists in Kudu datalake" in {
    assert(storage.runHiveQuery(s"show tables in $KUDU_DATABASE like $KUDU_RATINGS_TABLE").count() == 1)
  }

  it must "contains transformed ratings" in {

    var rats = storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_RATINGS_TABLE").limit(10).cache()

    assert(rats.count() == 10)

    assert(rats.columns.contains("userid"))
    assert(rats.columns.contains("movieid"))
    assert(rats.columns.contains("ratings"))
    assert(rats.columns.contains("timestammp"))

    val r = rats.where("id == 1").collect()(0)

    assert(r(0) == 1)
    assert(r(1) == 2)
    assert(r(2) == 3.5)
    assert(r(3) == "1112486027")
  }


  // TAGS -----------------------------------------


  "The tags table" must "exists in Hive datalake" in {
    assert(storage.runHiveQuery(s"show tables in $HIVE_DATABASE like $HIVE_TAGS_TABLE").count() == 1)
  }

  it must "contains transformed tags" in {

    var tags = storage.runHiveQuery(s"select * from $HIVE_DATABASE.$HIVE_TAGS_TABLE limit 10").cache()

    assert(tags.count() == 10)

    assert(tags.columns.contains("id"))
    assert(tags.columns.contains("userid"))
    assert(tags.columns.contains("movieid"))
    assert(tags.columns.contains("tags"))
    assert(tags.columns.contains("timestamp"))

    val t = tags.where("id == 1").collect()(0)

    assert(t(0) == 1)
    assert(t(1) == 18)
    assert(t(2) == 4141)
    assert(t(3) == "Mark Waters")
    assert(t(4) == "1240597180")
  }


  "The tags table" must "exists in Kudu datalake" in {
    assert(storage.runHiveQuery(s"show tables in $KUDU_DATABASE like $KUDU_RATINGS_TABLE").count() == 1)
  }

  it must "contains transformed tags" in {

    var tags = storage.readKuduTable(s"$KUDU_DATABASE.$KUDU_TAGS_TABLE").limit(10).cache()

    assert(tags.count() == 10)

    assert(tags.columns.contains("userid"))
    assert(tags.columns.contains("movieid"))
    assert(tags.columns.contains("tag"))
    assert(tags.columns.contains("time"))

    val t = tags.where("id == 1").collect()(0)

    assert(t(1) == 18)
    assert(t(2) == 4141)
    assert(t(3) == "Mark Waters")
    assert(t(4) == "1240597180")
  }


  // ---------------------------------------------------------------------------------------
  // Batch ML Tests --------------------------------------------------------------------

  var mseLine = ""

  "The Batch ML run" should "take some times" in {

    log.info("----- Run Batch ML process from fat jar in lib folder -----")

    var bmlStream = Process("spark-submit --master local --class Main lib/MRSpark2-assembly-0.1.jar").lineStream

    var done = false

    for{
      line <- bmlStream
      !done
    }{
      println(line)
      if(line.contains("Actual MSE is"))
        mseLine = line
      done = line.contains("BATCH ML PROCESS DONE")
    }
  }

  it must "store the model in the right directory" in {
    configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/BatchML.conf"))
    MODEL_ARCHIVE_PATH = configuration.getString("bml.recommender.model_archive_path")

    assert(new File(MODEL_ARCHIVE_PATH).exists())
  }


  it must "compute a valid model based on loaded ratings (ECM < 1)" in {

    var mse = mseLine.split(" ").last.asInstanceOf[Double]
    assert(mse < 1.0)
  }



  // ---------------------------------------------------------------------------------------
  // Real Time ML Tests --------------------------------------------------------------------

  "The Real Time ML process" must "load the model from right directory" in {
    configuration = ConfigFactory.parseFile(new File(s"${CONF_DIR}/RealTimeML.conf"))
    MODEL_ARCHIVE_PATH = configuration.getString("rtml.model.archive_path")

    assert(new File(MODEL_ARCHIVE_PATH).exists())
  }

  "The RealTime ML run" should "take some times" in {

    log.info("----- Run Batch ML process from fat jar in lib folder -----")

    var bmlStream = Process("spark-submit --master local --class JettyLauncher target/scala-2.11/RealTimeMovieRec-assembly-0.1.jar").lineStream

    var done = false

    for{
      line <- bmlStream
      !done
    }{
      println(line)
      if(line.contains("Actual MSE is"))
        mseLine = line
      done = line.contains("BATCH ML PROCESS DONE")
    }
  }


  it must "be online" in {
    pending
  }

  it must "compute a valid recommendation" in {
    pending
  }

  override def afterAll(): Unit = {
    super.afterAll()

    // Destroy Kudu and Hive tables

    // Drop Spark

    // stop JDBC connector
    // stop DBFeeder
    // destroy confluent
    // drop all tables
  }


}
