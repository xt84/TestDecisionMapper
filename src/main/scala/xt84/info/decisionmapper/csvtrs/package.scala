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

  def initSession(appName: String = "SparkApp", master: Option[String] = None, options: Option[Map[String, String]] = None): SparkSession =
    SparkSession.getActiveSession.getOrElse({
      val sessionBuilder = SparkSession.builder().appName(appName)
      if (master.isDefined) sessionBuilder.master(master.get)
      if (options.isDefined) sessionBuilder.config(new SparkConf().setAll(options.get)).getOrCreate() else sessionBuilder.getOrCreate()
    })

  case class CsvTransformConfiguration(dataPath: String, reportPath: String, rules: List[ColumnTransformRule])
  case class ColumnTransformRule(
                                  existing_col_name: String,
                                  new_col_name: String,
                                  new_data_type: String,
                                  date_expression: Option[String] = None
                           ) {

    val expressionTransformation: Column = new_data_type match {
      case "string"   => col(existing_col_name) cast StringType as new_col_name
      case "integer"  => col(existing_col_name) cast IntegerType as new_col_name
      case "date"     => to_date(unix_timestamp(col(existing_col_name), date_expression.get) cast TimestampType) as new_col_name
      case "boolean"  => col(existing_col_name) cast BooleanType as new_col_name
      case _          => throw new Exception("Type casting expression not implemented yet")
    }

    val expressionReport: Column = new_data_type match {
      case "date" => date_format(col(new_col_name), date_expression.get) as existing_col_name
      case _ => col(new_col_name) as existing_col_name
    }

    val reportFilterExpression: String = s"$new_col_name IS NOT NULL"
    val reportColumn: Column = col(new_col_name) as existing_col_name
  }

  def rulesLoader(path: String): List[ColumnTransformRule] = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val fs = FileSystem.get(new URI(path), new Configuration())
    parse(fs.open(new Path(path))).extract[List[ColumnTransformRule]]
  }
}
