package xt84.info.decisionmapper.csvtrs

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Processing(val spark: SparkSession) {

  def load(path: String): DataFrame = {
    val df = spark.read
      .format(INPUT_FORMAT)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .load(path)
    df.select(clearExpr(df.columns):_*).filter(filterExpr(df.columns))
  }

  def transform(df: DataFrame, rules: List[ColumnTransformRule]): DataFrame = df.select(rules.map(r => r.expression):_*)

  def clearExpr(columns: Seq[String]): Seq[Column] = columns.map(c => regexp_replace(col(c), " ", "") as c)
  def filterExpr(columns: Seq[String]): String = columns.map(c => s"($c IS NULL OR $c <> '')").mkString(" AND ")
}
