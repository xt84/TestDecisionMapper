package xt84.info.decisionmapper.csvtrs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class Report(df: DataFrame, rules: List[ColumnTransformRule]) {

  val spark: SparkSession = SparkSession.getActiveSession.get

  import spark.implicits._

  val winBase: WindowSpec = Window.partitionBy(rules.map(r => col(r.new_col_name)): _*)
  val winFinal: WindowSpec = Window.partitionBy($"Column")

  def prepareReport(): String = build(
      prepareReportForColumn(rules.head),
      rules.tail
  ).toJSON.collect.mkString("[", ",", "]")

  def build(accDf: DataFrame, rules: List[ColumnTransformRule]): DataFrame = rules match {
    case Nil => accDf
    case rule :: rules => {
      build(
        prepareReportForColumn(rule),
        rules
      ).union(accDf)
    }
  }

  def prepareReportForColumn(rule: ColumnTransformRule): DataFrame = df.filter(
      rule.reportFilterExpression
    ).select(
      lit(rule.existing_col_name) as "Column",
      rule.expressionReport,
      count(rule.new_col_name) over winBase as "vals_count"
    ).distinct().select(
      $"Column",
      sum($"vals_count") over winFinal as "Unique_values",
      col(rule.existing_col_name),
      $"vals_count"
    ).groupBy($"Column", $"Unique_values").agg(
      collect_list(map(col(rule.existing_col_name) cast StringType, $"vals_count")) as "Values"
    )
}
