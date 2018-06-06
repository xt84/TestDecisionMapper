package xt84.info.decisionmapper.csvtrs

import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.util.Try

class JobRunner(
                 val configuration: CsvTransformConfiguration,
                 val csvWriteOptions: Option[Map[String, String]] = None
               ) extends LazyLogging {

  def run() = Try({
    logger.info("Init Spark session")
    val spark = initSession(options = Some(Map("spark.driver.maxResultSize" -> "20g")))

    logger.info("Load main dataset")
    val df = Processing.load(spark, configuration.dataPath, configuration.rules)
    logger.info("Transform dataset and save it")
    val dft = Processing.transform(df, configuration.rules)
    dft.coalesce(1)
      .write
      .format(DEFAULT_FORMAT)
      .options(csvWriteOptions.getOrElse(CSV_WRITE_OPTIONS))
      .save(configuration.outputPath)

    logger.info("Prepare report")
    write(new Report(dft, configuration.rules).prepareReport())
  })

  def write(metrics: String) = Try({
    val reportFile = new File(configuration.reportPath)
    if (!reportFile.getParentFile.exists()) reportFile.getParentFile.mkdirs()
    val writer = new PrintWriter(reportFile)
    writer.write(metrics)
    writer.close()
  })
}
