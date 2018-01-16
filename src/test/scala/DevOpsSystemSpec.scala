import java.io.File

import com.typesafe.config._
import it.reply.data.pasquali.Storage
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

import scala.util.Properties
import sys.process._
import org.apache.log4j.Logger
import org.scalatra.test.scalatest.ScalatraFlatSpec

class DevOpsSystemSpec extends ScalatraFlatSpec with BeforeAndAfterAll{

  val CONF_DIR = "CONF_DIR"

  val SPARK_MASTER = "local[*]"
  val SPARK_APPNAME = "Integration Tests"
  val THRIFT_SERVER = "cloudera-vm"
  val THRIFT_PORT = "9083"

  val TOGGLE_SPARK = "TOGGLE_SPARK"
  val TOGGLE_SQOOP = "TOGGLE_SQOOP"
  val TOGGLE_KAFKA = "TOGGLE_KAFKA"
  val TOGGLE_BETL = "TOGGLE_BETL"
  val TOGGLE_RTETL = "TOGGLE_RTETL"
  val TOGGLE_BML = "TOGGLE_BML"
  val TOGGLE_RTML = "TOGGLE_RTML"

  val KUDU_MOVIES_TABLE : String = "KUDU_MOVIES_TABLE"
  val KUDU_GTAGS_TABLE : String = "KUDU_GTAGS_TABLE"
  val KUDU_TAGS_TABLE : String = "KUDU_TAGS_TABLE"
  val KUDU_RATINGS_TABLE : String = "KUDU_RATINGS_TABLE"
  val KUDU_ADDR : String = "KUDU_ADDR"
  val KUDU_PORT : String = "KUDU_PORT"
  val KUDU_TABLE_BASE : String = "KUDU_TABLE_BASE"
  val KUDU_DATABASE : String = "KUDU_DATABASE"

  val MODEL_ARCHIVE_PATH_STORE = "MODEL_ARCHIVE_PATH_STORE"
  val MODEL_ARCHIVE_PATH_LOAD = "MODEL_ARCHIVE_PATH_LOAD"

  val HIVE_MOVIES_TABLE : String = "HIVE_MOVIES_TABLE"
  val HIVE_LINKS_TABLE : String = "HIVE_LINKS_TABLE"
  val HIVE_GTAGS_TABLE : String = "HIVE_GTAGS_TABLE"
  val HIVE_TAGS_TABLE : String = "HIVE_TAGS_TABLE"
  val HIVE_RATINGS_TABLE : String = "HIVE_RATINGS_TABLE"
  val HIVE_DATABASE : String = "HIVE_DATABASE"

  val KAFKA_PID = "KAFKA_PID"

  val log : Logger = Logger.getLogger(getClass.getName)
  val storage : Storage = Storage()
  var kuduMovies : DataFrame = null
  var kuduGTags : DataFrame = null

  def loadToggleSettings(confDir : String) : Unit = {

    "Config folder" must "contains conf Toggle integration tests" in {

      log.info(s"----- Check that $confDir conf dir contains toggle settings -----")
      assert(new File(s"$confDir/Toggle.conf").exists())
    }

    val toggleConf = ConfigFactory.parseFile(new File(s"${confDir}/Toggle.conf"))

    Configurator.putConfig(TOGGLE_SPARK,  toggleConf.getBoolean("toggle.integration.test.sparkSession"))
    Configurator.putConfig(TOGGLE_SQOOP,  toggleConf.getBoolean("toggle.integration.test.sqoop"))
    Configurator.putConfig(TOGGLE_KAFKA,  toggleConf.getBoolean("toggle.integration.test.kafka"))
    Configurator.putConfig(TOGGLE_BETL,   toggleConf.getBoolean("toggle.integration.test.betl"))
    Configurator.putConfig(TOGGLE_RTETL,  toggleConf.getBoolean("toggle.integration.test.rtetl"))
    Configurator.putConfig(TOGGLE_BML,    toggleConf.getBoolean("toggle.integration.test.bml"))
    Configurator.putConfig(TOGGLE_RTML,   toggleConf.getBoolean("toggle.integration.test.rtml"))

  }

  def loadBatchETLConf(confDir : String) : Unit = {

    "Config folder" must "contains conf for Batch ETL" in {

      log.info(s"----- Check that $confDir conf dir contains configurations for Batch ETL -----")

      assert(new File(s"$confDir/BatchETL.conf").exists())
      //assert(new File(s"$confDir/BatchETL_staging.conf").exists())
    }

    val betlConf = ConfigFactory.parseFile(new File(s"${confDir}/BatchETL.conf"))

    Configurator.putConfig(HIVE_MOVIES_TABLE, betlConf.getString("betl.hive.input.movies"))
    Configurator.putConfig(HIVE_LINKS_TABLE,  betlConf.getString("betl.hive.input.links"))
    Configurator.putConfig(HIVE_GTAGS_TABLE,  betlConf.getString("betl.hive.input.gtags"))
    Configurator.putConfig(HIVE_DATABASE,  betlConf.getString("betl.hive.database"))

    Configurator.putConfig(KUDU_GTAGS_TABLE, betlConf.getString("betl.kudu.gtags_table"))
    Configurator.putConfig(KUDU_MOVIES_TABLE, betlConf.getString("betl.kudu.movies_table"))



  }

  def loadRealTimeETLConf(confDir : String) : Unit = {

    "Config Folder" must "contains conf for Real Time ETL" in {

      log.info(s"----- Check that $confDir conf dir contains configurations for Real Time ETL -----")

      assert(new File(s"$confDir/RealTimeETL.conf").exists())
      //assert(new File(s"$confDir/RealTimeETL_staging.conf").exists())
    }

    val rtetlConf = ConfigFactory.parseFile(new File(s"${confDir}/RealTimeETL.conf"))

    Configurator.putConfig(KUDU_ADDR, rtetlConf.getString("rtetl.kudu.address"))
    Configurator.putConfig(KUDU_PORT, rtetlConf.getString("rtetl.kudu.port"))
    Configurator.putConfig(KUDU_TABLE_BASE, rtetlConf.getString("rtetl.kudu.table_base"))
    Configurator.putConfig(KUDU_DATABASE, rtetlConf.getString("rtetl.kudu.database"))

    Configurator.putConfig(KUDU_TAGS_TABLE, rtetlConf.getString("rtetl.default.table.tags"))
    Configurator.putConfig(KUDU_RATINGS_TABLE, rtetlConf.getString("rtetl.default.table.ratings"))

    Configurator.putConfig(HIVE_TAGS_TABLE, rtetlConf.getString("rtetl.default.table.tags"))
    Configurator.putConfig(HIVE_RATINGS_TABLE, rtetlConf.getString("rtetl.default.table.ratings"))

  }

  def loadBatchMLConf(confDir : String) : Unit = {

    "Confif Folder"  must "contains conf for Batch ML" in {

      log.info(s"----- Check that $confDir conf dir contains configurations for Batch ML -----")

      assert(new File(s"$confDir/BatchML.conf").exists())
      //assert(new File(s"$confDir/BatchML_staging.conf").exists())
    }

    val bml = ConfigFactory.parseFile(new File(s"${confDir}/BatchML.conf"))
    Configurator.putConfig(MODEL_ARCHIVE_PATH_STORE, bml.getString("bml.recommender.model_archive_path"))
  }

  def loadRealTimeMLConf(confDir : String) : Unit = {

    "Confif Folder" must "contains conf for Real Time ML" in {

      log.info(s"----- Check that $confDir conf dir contains configurations for Real Time ML -----")

      assert(new File(s"$confDir/RealTimeML.conf").exists())
      //assert(new File(s"$confDir/RealTimeML_staging.conf").exists())
    }

    val rtml = ConfigFactory.parseFile(new File(s"${confDir}/BatchML.conf"))
    Configurator.putConfig(MODEL_ARCHIVE_PATH_LOAD, rtml.getString("bml.recommender.model_archive_path"))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    //Configurator.putConfig(CONF_DIR, Properties.envOrElse("DEVOPS_CONF_DIR", "conf"))
    Configurator.putConfig(CONF_DIR, "conf")

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
    log.info("----- And Store them to Configurator Singleton -----")

    val confDir = Configurator.getStringConfig(CONF_DIR)

    println("\n")
    log.info(s"----- $confDir -----")
    println("\n")

    loadToggleSettings(confDir)
    log.info(s"----- Toggle Integration settings loaded -----")

    loadBatchETLConf(confDir)
    log.info(s"----- Batch ETL Conf Loaded Conf loaded -----")

    loadRealTimeETLConf(confDir)
    log.info(s"----- Real Time ETL Conf loaded -----")

    loadBatchMLConf(confDir)
    log.info(s"----- Batch ML Conf loaded -----")

    loadRealTimeMLConf(confDir)
    log.info(s"----- Real Time ML Conf loaded -----")

    log.info("----- CONFIGURATIONS -----")
    Configurator.printAll(log)
    log.info("\n----- -------------- -----")


    //**************************************************************************************

    log.info("----- Initialize Spark throught the storage class -----")
    log.info("----- Enable both Hive and Kudu support -----")

    val kuduAddr = Configurator.getStringConfig(KUDU_ADDR)
    val kuduPort = Configurator.getStringConfig(KUDU_PORT)
    val kuduTableBase = Configurator.getStringConfig(KUDU_TABLE_BASE)

    // Initialize Spark
    storage.init(SPARK_MASTER, SPARK_APPNAME, THRIFT_SERVER, THRIFT_PORT)
      .initKudu(kuduAddr, kuduPort, kuduTableBase)

    log.info("----- Init Done -----")

    // *************************************************************************************

    log.info("----- Start confluent schema registry -----")

    // Run Confluent
    "confluent start schema-registry" !

    log.info("----- Start Kafka JDBC Connector and populate topics -----")

    // Run My JDBC Connector
    var kafka = Process("/opt/kafka-JDBC-connector/run.sh &").lineStream
    //val pid = kafka.head

    var done = false

    for{
      line <- kafka
      if !done
    }{
      println(line)
      if(line.contains("finished initialization and start")){
        log.info("----- Stage environment initialized -----")
        done = true
        return
      }
    }
  }

  // -----------------------------------------------------------------------------
  // Spark Initialization --------------------------------------------------------

  "Spark Session" must "be initialized" in {

    val toggleSpark = Configurator.getBooleanConfig(TOGGLE_SPARK)

    if(toggleSpark){

      log.info("----- Check that spark session is running -----")
      assert(storage.spark != null)

    }
    else
      pending
  }

  // -----------------------------------------------------------------------------
  // Sqoop Bulk tests ------------------------------------------------------------

  "Sqoop" must "load movies, links and genometags in hive datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_SQOOP)

    if(toggle) {
      log.info("----- Select movies, links and genometags tables from hive -----")

      val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
      val movies = Configurator.getStringConfig(HIVE_MOVIES_TABLE)
      val links = Configurator.getStringConfig(HIVE_LINKS_TABLE)
      val gtags = Configurator.getStringConfig(HIVE_GTAGS_TABLE)

      var hiveMoviesExists = storage.runHiveQuery(s"show tables in $hiveDB like '$movies'")
      var hiveLinksExists = storage.runHiveQuery(s"show tables in $hiveDB like '$links'")
      var hiveGtagsExists = storage.runHiveQuery(s"show tables in $hiveDB like '$gtags'")

      log.info("----- Tables must exists -----")

      assert(hiveMoviesExists.collect().length == 1)
      assert(hiveLinksExists.collect().length == 1)
      assert(hiveGtagsExists.collect().length == 1)

      val hiveMovies = storage.runHiveQuery(s"select * from $hiveDB.$movies limit 10")
      val hiveLinks = storage.runHiveQuery(s"select * from $hiveDB.$links limit 10")
      val hiveGTags = storage.runHiveQuery(s"select * from $hiveDB.$gtags limit 10")

      log.info("----- And contains at least 10 elements -----")

      assert(hiveMovies.count() == 10)
      assert(hiveLinks.count() == 10)
      assert(hiveGTags.count() == 10)
    }
    else
      pending
  }

  // -----------------------------------------------------------------------------
  // Kafka JDBC Connector Tests --------------------------------------------------

  "Start a Kakfa console consumer" must
    "retrieve a lot of data, including rating and tag sample" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_KAFKA)

    if(toggle) {

      log.info("----- Start Kafka Console Stream to check that Kafka JDBC connector is running properly -----")

      var kafkaConsoleStream = Process("/opt/kafka-JDBC-connector/debugConsoleConsumer.sh psql-m20-tags &").lineStream

      var tagSample = """{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"string","optional":true,"field":"tag"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"tags"},"payload":{"id":1,"userid":18,"movieid":4141,"tag":"Mark Waters","timestamp":"1240597180"}}""".stripMargin
      var ratingSample = """{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"int32","optional":true,"field":"userid"},{"type":"int32","optional":true,"field":"movieid"},{"type":"double","optional":true,"field":"rating"},{"type":"string","optional":true,"field":"timestamp"}],"optional":false,"name":"ratings"},"payload":{"id":1,"userid":1,"movieid":2,"rating":3.5,"timestamp":"1112486027"}}""".stripMargin

      log.info("----- Check that the stream continas sample values for both tags and ratingss -----")

      assert(kafkaConsoleStream.contains(tagSample))

      s"pkill -f .*/opt/kafka-JDBC-connector/debugConsoleConsumer.sh.*" !

      log.info("----- Console consumer for tags killed -----")

      kafkaConsoleStream = Process("/opt/kafka-JDBC-connector/debugConsoleConsumer.sh psql-m20-ratings &").lineStream

      assert(kafkaConsoleStream.contains(ratingSample))

      s"pkill -f .*/opt/kafka-JDBC-connector/debugConsoleConsumer.sh.*" !

      log.info("----- Console consumer for ratings killed -----")
    }
    else
      pending
  }



  // -----------------------------------------------------------------------------
  // Batch ETL Tests -------------------------------------------------------------

  "The Batch ETL run" should "take some times" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BETL)

    if(toggle) {

      log.info("----- Run Batch ETL process from fat jar in lib folder -----")

      var betlStream = Process("spark-submit --master local --class BatchETL --driver-java-options -Dconfig.file=conf/BatchETL.conf  lib/BatchETL2-assembly-0.1.jar").lineStream

      assert(betlStream.contains("BATCH ETL PROCESS DONE"))

//      var done = false
//
//      for{
//        line <- betlStream
//        if !done
//      }{
//        println(line)
//        done = line.contains("BATCH ETL PROCESS DONE")
//      }
    }
    else
      pending
  }

  "The Batch ETL process" must "store data to KUDU datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BETL)

    if(toggle) {

      log.info("----- kudu tables must contains values -----")

      val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
      val movies = Configurator.getStringConfig(KUDU_MOVIES_TABLE)
      val gtags = Configurator.getStringConfig(KUDU_GTAGS_TABLE)

      kuduMovies = storage.readKuduTable(s"$kuduDB.$movies").cache()
      kuduGTags = storage.readKuduTable(s"$kuduDB.$gtags").cache()

      assert(kuduMovies.count() >= 1)
      assert(kuduMovies.count() >= 1)
    }
    else
      pending
  }

  it must "store gtags as they are" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BETL)

    if(toggle) {

      log.info("----- Tag values in right format -----")

      var df = kuduGTags.where("tagid == 1")
      assert(df.count() == 1)
      assert(df.columns.contains("tagid"))
      assert(df.columns.contains("tag"))
    }
    else
      pending

  }

  it must "merge tmdb links and movies" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BETL)

    if(toggle) {

      log.info("----- Movie values in right format -----")

      var df = kuduMovies.where("movieid ==1")
      assert(df.count() == 1)
      assert(df.columns.contains("movieid"))
      assert(df.columns.contains("title"))
      assert(df.columns.contains("genres"))
      assert(df.columns.contains("link"))
    }
    else
      pending
  }

  // -----------------------------------------------------------------------------
  // Real Time ETL ---------------------------------------------------------------

  "The RealTime ETL run" should "take some times to process tags and ratings streams" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      log.info("----- Run Real Time ETL process for ratings stream -----")

      try{
        var rtetlStreamRatings = Process("spark-submit --master local --class it.reply.data.pasquali.StreamMono --driver-java-options -Dconfig.file=conf/RealTimeETL.conf lib/RealTimeETL-assembly-0.1.jar psql-m20-ratings smallest").lineStream
        //assert(rtetlStreamRatings.contains("Empty RDD"))
        var done = false

        for{
          line <- rtetlStreamRatings
          if !done
        }{
          println(line)
          if(line.contains("Empty RDD")){
            done = true
            "pkill -f .*lib/RealTimeETL-assembly-0.1.jar.*" !
          }
        }
      }
      catch{
        case e: Exception => log.warn(s"Real Time ETL Stream Killed\n${e.getMessage}")
      }

      log.info("----- Run Real Time ETL process for tags stream -----")

      try{
        var rtetlStreamTags = Process("spark-submit --master local --class it.reply.data.pasquali.StreamMono --driver-java-options -Dconfig.file=conf/RealTimeETL.conf lib/RealTimeETL-assembly-0.1.jar psql-m20-tags smallest").lineStream
        //assert(rtetlStreamTags.contains("Empty RDD"))

        var done = false

        for{
          line <- rtetlStreamTags
          if !done
        }{
          println(line)

          if(line.contains("Empty RDD")){
            done = true
            "pkill -f .*lib/RealTimeETL-assembly-0.1.jar.*" !
          }
        }
      }
      catch{
        case e: Exception => log.info(s"Real Time ETL Stream Killed\n${e.getMessage}")
      }


    }
    else
      pending
  }

  // RATINGS --------------------------------

  "The ratings hive table" must "exists in Hive datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
      var ratings = Configurator.getStringConfig(HIVE_RATINGS_TABLE)

      assert(storage.runHiveQuery(s"show tables in $hiveDB like '$ratings'").count() == 1)
    }
    else
      pending

  }

  it must "contains transformed ratings" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {
      val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
      var ratings = Configurator.getStringConfig(HIVE_RATINGS_TABLE)

      var rats = storage.runHiveQuery(s"select * from $hiveDB.$ratings limit 10").cache()

      assert(rats.count() == 10)

      assert(rats.columns.contains("id"))
      assert(rats.columns.contains("userid"))
      assert(rats.columns.contains("movieid"))
      assert(rats.columns.contains("rating"))
      assert(rats.columns.contains("timestamp"))

      val r = rats.where("id == 1").collect()(0)

      assert(r(0) == 1)
      assert(r(1) == 1)
      assert(r(2) == 2)
      assert(r(3) == 3.5)
      assert(r(4) == "1112486027")
    }
    else
      pending
  }


  "The ratings kudu table" must "exists in Kudu datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
      var ratings = Configurator.getStringConfig(KUDU_RATINGS_TABLE)

      assert(storage.runHiveQuery(s"show tables in $kuduDB like '$ratings'").count() == 1)
    }
    else
      pending
  }

  it must "contains transformed ratings" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    val spark = storage.spark
    import spark.implicits._

    if(toggle) {

      val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
      var ratings = Configurator.getStringConfig(KUDU_RATINGS_TABLE)

      var rats = storage.readKuduTable(s"$kuduDB.$ratings").cache()

      assert(rats.count() > 0)

      assert(rats.columns.contains("userid"))
      assert(rats.columns.contains("movieid"))
      assert(rats.columns.contains("rating"))
      assert(rats.columns.contains("time"))

      val r =rats.where("userid=1 and movieid=2").collect()(0)

      assert(r(0) == 1)
      assert(r(1) == 2)
      assert(r(2) == 3.5)
      assert(r(3) == "1112486027")
    }
    else
      pending
  }


  // TAGS -----------------------------------------


  "The tags hive table" must "exists in Hive datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
      var tt = Configurator.getStringConfig(HIVE_TAGS_TABLE)

      assert(storage.runHiveQuery(s"show tables in $hiveDB like '$tt'").count() == 1)

    }
    else
      pending
  }

  it must "contains transformed tags" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
      var tt = Configurator.getStringConfig(HIVE_TAGS_TABLE)

      var tags = storage.runHiveQuery(s"select * from $hiveDB.$tt limit 10").cache()

      assert(tags.count() == 10)

      assert(tags.columns.contains("id"))
      assert(tags.columns.contains("userid"))
      assert(tags.columns.contains("movieid"))
      assert(tags.columns.contains("tag"))
      assert(tags.columns.contains("timestamp"))

      val t = tags.where("id == 1").collect()(0)

      assert(t(0) == 1)
      assert(t(1) == 18)
      assert(t(2) == 4141)
      assert(t(3) == "Mark Waters")
      assert(t(4) == "1240597180")
    }
    else
      pending

  }


  "The tags kudu table" must "exists in Kudu datalake" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
      var tt = Configurator.getStringConfig(KUDU_TAGS_TABLE)

      assert(storage.runHiveQuery(s"show tables in $kuduDB like '$tt'").count() == 1)
    }
    else
      pending
  }

  it must "contains transformed tags" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggle) {

      val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
      var tt = Configurator.getStringConfig(KUDU_TAGS_TABLE)

      var tags = storage.readKuduTable(s"$kuduDB.$tt").cache()

      assert(tags.count() > 0)

      assert(tags.columns.contains("userid"))
      assert(tags.columns.contains("movieid"))
      assert(tags.columns.contains("tag"))
      assert(tags.columns.contains("time"))

      val t = tags.where("userid=18 and movieid=4141").collect()(0)

      assert(t(0) == 18)
      assert(t(1) == 4141)
      assert(t(2) == "Mark Waters")
      assert(t(3) == "1240597180")
    }
    else
      pending
  }


  // ---------------------------------------------------------------------------------------
  // Batch ML Tests --------------------------------------------------------------------

  var mseLine = ""

  "The Batch ML run" should "take some times" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BML)

    if(toggle) {

      log.info("----- Run Batch ML process from fat jar in lib folder -----")

      var bmlStream = Process("spark-submit --master local --class Main --driver-java-options -Dconfig.file=conf/BatchML.conf lib/MRSpark2-assembly-0.1.jar").lineStream

      var done = false

      for{
        line <- bmlStream
        if !done
      }{
        println(line)
        if(line.contains("Actual MSE is"))
          mseLine = line
        done = line.contains("BATCH ML PROCESS DONE")
      }

    }
    else
      pending


  }

  it must "store the model in the right directory" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BML)

    if(toggle) {

      val path = Configurator.getStringConfig(MODEL_ARCHIVE_PATH_STORE)

      log.info("----- Load Batch ML configurations -----")
      log.info("----- Check if model exists -----")

      assert(new File(path).exists())
    }
    else
      pending


  }

  it must "compute a valid model based on loaded ratings (ECM < 1)" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_BML)

    if(toggle) {

      log.info("----- Evaluate moved -----")

      var mse = mseLine.split(" ").last.asInstanceOf[Double]
      assert(mse < 1.0)

    }
    else
      pending
  }



  // ---------------------------------------------------------------------------------------
  // Real Time ML Tests --------------------------------------------------------------------

  "The Real Time ML process" must "load the model from right directory" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTML)

    if(toggle) {

      val path = Configurator.getStringConfig(MODEL_ARCHIVE_PATH_LOAD)

      log.info("----- Load Real Time ML config -----")
      log.info("----- Check if model exists -----")

      assert(new File(path).exists())
    }
    else
      pending
  }

  "The RealTime ML run" should "take some times" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTML)

    if(toggle) {

      log.info("----- Run Real Time ML process from fat jar in lib folder -----")

      "spark-submit --master local --class JettyLauncher target/scala-2.11/RealTimeMovieRec-assembly-0.1.jar" !!
    }
    else
      pending


//    var bmlStream = Process("spark-submit --master local --class JettyLauncher target/scala-2.11/RealTimeMovieRec-assembly-0.1.jar").lineStream
//
//    var done = false
//
//    for{
//      line <- bmlStream
//      !done
//    }{
//      println(line)
//      if(line.contains("Actual MSE is"))
//        mseLine = line
//      done = line.contains("BATCH ML PROCESS DONE")
//    }
  }

  it must "be online" in {

    val toggle = Configurator.getBooleanConfig(TOGGLE_RTML)

    if(toggle) {

      get("/isOnline"){
        status should equal (200)
        body should include ("is Online")
      }
    }
    else pending
  }

  it must "compute a valid recommendation" in {
    val toggle = Configurator.getBooleanConfig(TOGGLE_RTML)

    if(toggle) {

      get("/raw/see/1/1"){
        assert(status equals 200)

        println("\n\n\n\n\n"+body+"\n\n\n\n\n\n\n")

        var pred : Double = body.toDouble

        assert(pred >= 0.0)
        assert(pred <= 1.0)
      }
    }
    else pending
  }

  override def afterAll(): Unit = {
    super.afterAll()

    val hiveDB = Configurator.getStringConfig(HIVE_DATABASE)
    val hRatings = Configurator.getStringConfig(HIVE_RATINGS_TABLE)
    val hTags = Configurator.getStringConfig(HIVE_TAGS_TABLE)

    val kuduDB = Configurator.getStringConfig(KUDU_DATABASE)
    val kTags = Configurator.getStringConfig(KUDU_TAGS_TABLE)
    val kRatings = Configurator.getStringConfig(KUDU_RATINGS_TABLE)
    val kGTags = Configurator.getStringConfig(KUDU_GTAGS_TABLE)
    val kMovies = Configurator.getStringConfig(KUDU_MOVIES_TABLE)

    //val kafkaPID = Configurator.getStringConfig(KAFKA_PID)

    log.info("----- Clean up Environment -----")


    val toggleRTETL = Configurator.getBooleanConfig(TOGGLE_RTETL)

    if(toggleRTETL) {
      log.info("----- Drop Hive Ratings and Tags tables generated by Real Time ETL -----")
      storage.runHiveQuery(s"drop table $hiveDB.$hRatings")
      storage.runHiveQuery(s"drop table $hiveDB.$hTags")

      log.info("----- Truncate Kudu tables generated by Real Time ETL -----")
      storage.deleteKuduRows(storage.readKuduTable(s"$kuduDB.$kTags").select("userid", "movieid"), s"$kuduDB.$kTags")
      storage.deleteKuduRows(storage.readKuduTable(s"$kuduDB.$kRatings").select("userid", "movieid"), s"$kuduDB.$kRatings")
    }

    val toggleBETL = Configurator.getBooleanConfig(TOGGLE_BETL)

    if(toggleBETL){

      log.info("----- Truncate Kudu tables generated by Batch ETL -----")

      storage.deleteKuduRows(storage.readKuduTable(s"$kuduDB.$kGTags").select("tagid"), s"$kuduDB.$kGTags")
      storage.deleteKuduRows(storage.readKuduTable(s"$kuduDB.$kMovies").select("movieid"), s"$kuduDB.$kMovies")
    }

    // Drop Spark
    log.info("----- Close spark Session -----")
    storage.closeSession()

    // stop JDBC connector
    log.info("----- Kill Kafka JDBC Connector Process -----")
    s"pkill -f .*/opt/kafka-JDBC-connector/.*" !

    // destroy confluent
    log.info("----- Stop and Destroy confluent topics -----")
    "confluent destroy" !

    "sudo rm -rf /opt/connectm20.offsets" !


  }




}
