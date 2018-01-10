import it.reply.data.pasquali.Storage
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import scala.slick.driver.PostgresDriver.simple._

import scala.io.Source
import scala.util.Properties
import sys.process._


class DevOpsSystemSpec extends FlatSpec with BeforeAndAfterAll{

  var CONF_DIR = ""
  var PSQL_PASS_FILE = ""

  var SPARK_MASTER = "local[*]"
  var SPARK_APPNAME = "Integration Tests"

  var storage : Storage = Storage()
  var connectionUrl = ""

  override def beforeAll(): Unit = {
    super.beforeAll()

    CONF_DIR = Properties.envOrElse("DEVOPS_CONF_DIR", "conf")
    PSQL_PASS_FILE =
      Properties.envOrElse("PSQL_PASS_FILE", "/var/lib/cloudera-scm-server-db/data/generated_password.txt")

    // Load configs


    // read psql password
    val src = Source.fromFile(PSQL_PASS_FILE)
    val password = src.getLines.take(1).toList
    src.close

    connectionUrl = s"jdbc:postgresql://localhost:7432/postgres?user=cloudera-scm&password=${password}"

    // Initialize DB tables
    "sudo -i python $DBFEEDER_HOME/DBFeeder.py --init --datasets=$M20_PATH --psql_user=cloudera-scm --psql_password=" + password !!

    // run DB Feeder
    "python $DBFEEDER_HOME/DBFeeder.py --fraction=0.001 --datasets=$M20_PATH --psql_user=cloudera-scm --psql_password=" + password !!

    // Run Confluent
    "confluent start schema-registry" !!

    // Run My JDBC Connector
    "./opt/kafka-JDBC-connector/run.sh" !!

    // Run Sqoop bulk
    """sudo -u hdfs sqoop import --connect 'jdbc:postgresql://localhost:7432/postgres?ssl=false' --username 'cloudera-scm' -P --password '"""+
      password+
      """' --table 'genometags' --hive-table 'datalake.genometags' --hive-import  --check-column id --append""" !!

    """sudo -u hdfs sqoop import --connect 'jdbc:postgresql://localhost:7432/postgres?ssl=false' --username 'cloudera-scm' -P --password '"""+
      password+
      """' --table 'links' --hive-table 'datalake.links' --hive-import  --check-column id --append""" !!

    """sudo -u hdfs sqoop import --connect 'jdbc:postgresql://localhost:7432/postgres?ssl=false' --username 'cloudera-scm' -P --password '"""+
      password+
      """' --table 'movies' --hive-table 'datalake.movies' --hive-import  --check-column id --append""" !!

    println("------ ENVIRONMENT INITIALZED ------")

    // Initialize Spark
    storage.init(SPARK_MASTER, SPARK_APPNAME, true)
  }

  // DB Feeder tests ---------------------------------------------------------------

  it must "contains Movies table and movie 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[Movies]
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  it must "contains Links table and link 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[Links]
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  it must "contains Genometags table and gtag 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[GTags]
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  it must "contains Genomescores table and gscore 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[GScores]
        assert(table.list.nonEmpty)
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  it must "contains Tag table and tag 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[Tags]
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  it must "contains Rating table and rating 5" in {
    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val table = TableQuery[Ratings]
        assert(table.list.nonEmpty)
        assert(table.filter(_.id === 5).list.nonEmpty)
    }
  }

  // -----------------------------------------------------------------------------
  // Kafka JDBC Connector Tests --------------------------------------------------

  "Start a Kakfa console consumer" must
    "retrieve a lot of data, including rating and tag 5" in {
    pending
  }
  // -----------------------------------------------------------------------------
  // Sqoop bulk tests ------------------------------------------------------------

  "The Hive Datalake" must "contains movies links and gtags" in {
    pending
  }
  // -----------------------------------------------------------------------------
  // Configurations Tests --------------------------------------------------------

  "Config folder" must "contains conf for Batch ETL" in {
    pending
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

  class Movies(tag: Tag) extends Table[(Int, String)](tag, "movies") {
    def id = column[Int]("id")
    def movieid = column[String]("movieid")
    def * = (id, movieid)
  }
  class Tags(tag: Tag) extends Table[(Int)](tag, "tags") {
    def id = column[Int]("id")
    def * = (id)
  }
  class Links(tag: Tag) extends Table[(Int)](tag, "links") {
    def id = column[Int]("id")
    def * = (id)
  }
  class Ratings(tag: Tag) extends Table[(Int)](tag, "ratings") {
    def id = column[Int]("id")
    def * = (id)
  }
  class GTags(tag: Tag) extends Table[(Int)](tag, "genometags") {
    def id = column[Int]("id")
    def * = (id)
  }
  class GScores(tag: Tag) extends Table[(Int)](tag, "genomescores") {
    def id = column[Int]("id")
    def * = (id)
  }

}
