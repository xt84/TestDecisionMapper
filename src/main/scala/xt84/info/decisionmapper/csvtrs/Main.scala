package xt84.info.decisionmapper.csvtrs

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.{Failure, Success}

object Main extends App with LazyLogging {

  override def main(args: Array[String]): Unit = {
    if ((args.length == 0) || ((args.length % KEYS.size) > 0) || (args.length < 3)) {
      println(usage(KEYS))
      System.exit(1)
    } else {
      val parsed = parseArguments(args.toList)
      logger.info(
        s"""
          |Path to data   -> ${parsed("data")}
          |Path to rules  -> ${parsed("rules")}
          |Path to output -> ${parsed("output")}
          |Path to report -> ${parsed("report")}
        """.stripMargin
      )
      new JobRunner(CsvTransformConfiguration(
        dataPath = parsed("data"),
        reportPath = parsed("report"),
        outputPath = parsed("output"),
        rules = rulesLoader(parsed("rules"))
      )).run() match {
        case Success(_)   => logger.info("Processing finished")
        case Failure(ex)  => {
          logger.error("Error when processing data", ex)
          System.exit(1)
        }
      }
    }
  }

  def usage(keys: Map[String, String]): String = keys.map(
    kv => s"--${kv._1} ${kv._2}"
  ).toList.mkString("\n")

  def parseArguments(args: List[String], argsMap: Map[String, String] = Map()): Map[String, String] = {
    def isKey(arg: String) = arg.startsWith("--")
    args match {
      case Nil => argsMap
      case arg :: args if isKey(arg) => parseArguments(
        args.tail,
        argsMap ++ Map(arg.replaceAll("--", "") -> args.head)
      )
    }
  }
}
