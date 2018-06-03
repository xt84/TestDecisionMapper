package xt84.info.decisionmapper

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

package object csvtrs {

  val INPUT_FORMAT = "com.databricks.spark.csv"

  def initSession(appName: String = "SparkApp", options: Option[Map[String, String]] = None): SparkSession =
    SparkSession.getActiveSession.getOrElse({
      val session = SparkSession.builder().appName(appName)
      if (options.isDefined) session.config(new SparkConf().setAll(options.get)).getOrCreate() else session.getOrCreate()
    })

  case class CsvTransformConfiguration(dataPath: String, reportPath: String, rules: List[ColumnTransformRule])
  case class ColumnTransformRule(
                                  existing_col_name: String,
                                  new_col_name: String,
                                  new_data_type: String,
                                  date_expression: Option[String] = None
                           ) {

    val typesMap = Map(
      "string"   -> StringType,
      "integer"  -> IntegerType,
      "date"     -> DateType,
      "boolean"  -> BooleanType
    )

    val columnCast: Column = col(existing_col_name) cast typesMap(new_data_type)

    val expression: Column = {
      if (date_expression.isDefined) date_format(columnCast, date_expression.get) else columnCast
    } as new_col_name
  }

  def rulesLoader(path: String): List[ColumnTransformRule] = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val fs = FileSystem.get(new URI(path), new Configuration())
    parse(fs.open(new Path(path))).extract[List[ColumnTransformRule]]
  }
}
