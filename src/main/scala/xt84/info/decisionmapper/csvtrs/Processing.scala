package xt84.info.decisionmapper.csvtrs

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Processing {

  def load(
            spark: SparkSession,
            path: String,
            rules: List[ColumnTransformRule],
            options: Option[Map[String, String]] = None
          ): DataFrame =
    spark.read
    .format(DEFAULT_FORMAT)
    .options(options.getOrElse(CSV_LOAD_OPTIONS))
    .load(path)
    .select(clearExpr(rules.map(r => r.existing_col_name)):_*)
    .filter(filterExpr(rules.map(r => r.existing_col_name)))

  def transform(df: DataFrame, rules: List[ColumnTransformRule]): DataFrame = df.select(rules.map(r => r.expressionTransformation):_*)

  def clearExpr(columns: Seq[String]): Seq[Column] = columns.map(c => regexp_replace(col(c), " ", "") as c)
  def filterExpr(columns: Seq[String]): String = columns.map(c => s"($c IS NULL OR $c <> '')").mkString(" AND ")
}
