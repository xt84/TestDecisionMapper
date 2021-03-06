package xt84.info.decisionmapper.csvtrs

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FeatureSpec, FlatSpec, GivenWhenThen}
import org.scalatest.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ProcessingSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfter {

  val APP_NAME: String = "TEST"
  val spark: SparkSession = initSession(appName=APP_NAME, master=Some("local"))

  val step1Header = Seq("name", "age", "birthday")
  val step3Header = Seq("first_name", "total_years", "d_o_b")

  val pathData: String = new File(getClass.getResource("/data/standard.csv").getPath).getAbsolutePath
  val pathRules: String = new File(getClass.getResource("/rules.json").getPath).getAbsolutePath

  feature("Process given CSV file and prepare report") {
    info("Task into divided on three steps")
    info("Each step in details described in task document")

    scenario("Step1 and Step2 - load data, and prepare for future processing.") {
      Given("Path to sample data, expected data")
        val pathDataExpected = new File(getClass.getResource("/data/step1expected.csv").getPath).getAbsolutePath
        val rules = rulesLoader(pathRules)
        val ve = transformToList(loadExpectedDataSet(pathDataExpected), step1Header)
      When("Data loaded and prepared")
        val va = transformToList(Processing.load(spark, pathData, rules), step1Header)
      Then("Datasets must be equal")
        assert(va == ve)
    }
    scenario("Step3 - rename columns and cast data types according rules described in file") {
      Given("Path to data, transformed (step1 dataset), transformation rules")
        val pathDataExpected = new File(getClass.getResource("/data/step3expected.csv").getPath).getAbsolutePath
        val rules = rulesLoader(pathRules)
        val ve = transformToList(loadExpectedDataSet(pathDataExpected), step3Header)
        val df = Processing.load(spark, pathData, rules)
      When("Try to transform loaded dataset to result set")
        val va = transformToList(Processing.transform(df, rules), step3Header)
      Then("Datasets must be equal")
        assert(va == ve)
    }
    scenario("Step4 - prepare report") {
      Given("Expected report, loaded processed dataset and rules set")
        val rules = rulesLoader(pathRules)
        val processedDf = Processing.transform(
          Processing.load(spark, pathData, rules),
          rules
        )
        val reportExpected = Source.fromFile(
          new File(getClass.getResource("/data/report_expected.json").getPath).getAbsolutePath
        ).mkString
      When("Try to prepare report")
        val reportActual = new Report(processedDf, rules).prepareReport()
      Then("Reports must be equal")
        assert(reportActual == reportExpected)
    }
  }

  def loadExpectedDataSet(path: String): DataFrame = spark.read
    .format(DEFAULT_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .load(path)

  def transformToList(df: DataFrame, columns: Seq[String]): List[Map[String, Nothing]] = df.collect().map(
    r => r.getValuesMap(columns)
  ).toList
}
