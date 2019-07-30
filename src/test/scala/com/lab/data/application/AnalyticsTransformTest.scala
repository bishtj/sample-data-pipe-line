package com.lab.data.application

import com.lab.data.TestCommon
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

class AnalyticsTransformTest extends TestCommon {

  test("transform test - should successfully transform input data set") {
    implicit val spark1 = spark

    val inputDf = loadDataFrame("src/test/resources/input.csv", EmployeeSchema)
    val expectedDf = loadDataFrame("src/test/resources/expected.csv", RoleSchema)

    val result = AnalyticsTransform(inputDf)

    val actualDf = result match {
      case Left(e) => fail(s"Failed due to error ${e.msg}")
      case Right(v) => v
    }

    assertDataFrameEquals(columnNullable(expectedDf), columnNullable(actualDf))
  }

  val EmployeeSchema = StructType(
    StructField("id", StringType) ::
    StructField("name", StringType) ::
    StructField("employee_id", StringType) ::
    StructField("dept_name", StringType) ::
    StructField("salary", DecimalType(10,0)) ::
      Nil
  )

  val RoleSchema = StructType(
    StructField("id", StringType) ::
      StructField("role", StringType) ::
      Nil
  )

}
