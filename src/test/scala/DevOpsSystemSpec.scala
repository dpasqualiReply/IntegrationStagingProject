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

  var storage : Storage = Storage()
  var connectionUrl = ""

  var PSQL_PASSWORD = ""

  var hiveMovies : DataFrame = null
  var hiveLinks : DataFrame = null
  var hiveGTags : DataFrame = null

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
    log.info("----- Start confluent schema registry -----")

    //**************************************************************************************

    log.info("----- Load Environment Config variables from file -----")






    //**************************************************************************************

    // Run Confluent
    "confluent start schema-registry" !!

    log.info("----- Start Kafka JDBC Connector and populate topics -----")

    // Run My JDBC Connector
    "./opt/kafka-JDBC-connector/run.sh" !

    log.info("----- Stage environment initialized -----")

    log.info("----- Initialize Spark throught the storage class -----")
    log.info("----- Enable both Hive and Kudu support -----")

    // Initialize Spark
    storage.init(SPARK_MASTER, SPARK_APPNAME, true)
  }

  // DB Feeder tests ---------------------------------------------------------------

  it must "contains Movies table and movie 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=movies" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 5, 'Father of the Bride Part II (1995)', 'Comedy')"""))
  }

  it must "contains Links table and link 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=links" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 5, '0113041', '11862')"""))
  }

  it must "contains Genometags table and gtag 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=genometags" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 5, '1930s')"""))
  }

  it must "contains Genomescores table and gscore 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=genomescores" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 1, 5, Decimal('0.14675'))"""))
  }

  it must "contains Tags table and tag 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=tags" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 65, 592, 'dark hero', '1368150078')"""))
  }

  it must "contains Ratings table and rating 5" in {
    val select = s"python dbConnector/PSQLInterface.py --database=postgres --psql_user=cloudera-scm --psql_password=${PSQL_PASSWORD}" +
      s" --host=cloudera-vm --port=7432 --selectAllFrom=ratings" !!

    assert(select.nonEmpty)
    assert(select.contains("""(5, 1, 50, 3.5, '1112484580')"""))
  }

  // -----------------------------------------------------------------------------
  // Kafka JDBC Connector Tests --------------------------------------------------

  "Start a Kakfa console consumer" must
    "retrieve a lot of data, including rating and tag 5" in {
    pending
  }
  // -----------------------------------------------------------------------------
  // Sqoop bulk tests ------------------------------------------------------------

  "The Hive Datalake" must "contains movies, links and gtags" in {
    val sqlContext = new org.apache.spark.sql.SQLContext(storage.spark.sparkContext)

    hiveMovies = sqlContext.sql(s"select * from datalake.movies")
    hiveLinks = sqlContext.sql(s"select * from datalake.links")
    hiveGTags = sqlContext.sql(s"select * from datalake.genometags")

    assert(hiveMovies.count() > 0)
    assert(hiveLinks.count() > 0)
    assert(hiveGTags.count() > 0)
  }
  // -----------------------------------------------------------------------------
  // Configurations Tests --------------------------------------------------------

  "Config folder" must "contains conf for Batch ETL" in {

  }

  it must "contains conf for Real Time ETL" in {
    pending
  }

  it must "contains conf for Batch ML" in {
    pending
  }

  it must "contains conf for Real Time ML" in {
    pending
  }
  // -----------------------------------------------------------------------------
  // Spark Initialization --------------------------------------------------------

  "Spark Context" must "be initialized" in {
    pending
  }

  "Spark Session" must "be initialized" in {
    pending
  }

  // -----------------------------------------------------------------------------

  // -----------------------------------------------------------------------------
  // Batch ETL Tests -------------------------------------------------------------

  "The Batch ETL process" must "load data from Hive datalake" in {
    pending
  }

  it must "store them to KUDU datalake" in {
    pending
  }

  it must "store gtags as they are" in {
    pending
  }

  it must "merge tmdb links and movies" in {
    pending
  }

  // -----------------------------------------------------------------------------
  // Real Time ETL ---------------------------------------------------------------

  "The Real Time ETL process" must
    "react to the publishing of a new rating on kafka topic" in {
    pending
  }

  it must "transform the rating and post it to Hive datalake and kudu datamart" in {
    pending
  }

  "The Real Time ETL process" must "react to the publishing of a new tag on kafka topic" in {
    pending
  }

  it must "transform the tag and post it to Hive datalake and kudu datamart" in {
    pending
  }

  // ---------------------------------------------------------------------------------------
  // Batch ML Tests --------------------------------------------------------------------

  "The Batch ML process" must "load ratings from Kudu datamart" in {
    pending
  }

  it must "compute a valid model based on loaded ratings (ECM < 0.04)" in {
    pending
  }

  it must "store the model in the right directory" in {
    pending
  }

  // ---------------------------------------------------------------------------------------
  // Real Time ML Tests --------------------------------------------------------------------

  "The Real Time ML process" must "load model from right directory" in {
    pending
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
