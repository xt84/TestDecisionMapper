package xt84.info.decisionmapper.csvtrs

import java.io.File

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, GivenWhenThen}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnTransformRulesSpec extends FlatSpec with GivenWhenThen {

  val rulesExpected = List(
    ColumnTransformRule(
      existing_col_name = "name",
      new_col_name = "first_name",
      new_data_type = "string"
    ),
    ColumnTransformRule(
      existing_col_name = "age",
      new_col_name = "total_years",
      new_data_type = "integer"
    ),
    ColumnTransformRule(
      existing_col_name = "birthday",
      new_col_name = "d_o_b",
      new_data_type = "date",
      date_expression = Some("dd-MM-yyyy")
    )
  )

  "Transformation rules " must " be loaded and parsed" in {
    Given("Path to rules configuration file")
      val path: String = new File(getClass.getResource("/rules.json").getPath).getAbsolutePath
    When("Rules loaded")
      val rulesActual = rulesLoader(path)
    Then("Rules must be parsed as expected")
      assert(rulesActual == rulesExpected)
  }

}
