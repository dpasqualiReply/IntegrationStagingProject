import org.apache.log4j.Logger

import scala.collection.mutable

object Configurator {

  var configs : mutable.Map[String, Any] = mutable.Map[String, Any]()

  def putConfig(name : String, value : Any) = {
    configs.put(name, value)
  }

  def getStringConfig(name : String) : String = {
    if(configs.contains(name))
      configs(name).asInstanceOf[String]
    else
      null
  }

  def getBooleanConfig(name : String) : Boolean = {
    if(configs.contains(name))
      configs(name).asInstanceOf[Boolean]
    else
      false
  }

  def printAll(log : Logger) : Unit = {
    configs.foreach(el => println(s"${el._1} -> ${el._2}"))
  }

  def logAll(log : Logger) : Unit = {
    configs.foreach(el => log.info(s"${el._1} -> ${el._2}"))
  }

}
