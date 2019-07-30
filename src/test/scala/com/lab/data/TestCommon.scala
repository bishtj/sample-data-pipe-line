package com.lab.data

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class TestCommon extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  def loadDataFrame(file: String, schema: StructType, header: String = "true")(implicit spark: SparkSession) : DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", header)
      .schema(schema)
      .load(file)
      .toDF()
  }

  def columnNullable(df: DataFrame): DataFrame = {
    val altPropColumnNames = df.schema.fields.filter(field => !field.nullable).map(field => field.name)
    nullableFalseProperty(df, altPropColumnNames.toList, true)
  }

  private def nullableFalseProperty(df: DataFrame, columnNames: List[String], nullable: Boolean): DataFrame = {
    val schema = df.schema
    // modify [[StructField] with name in `columnNames`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) if columnNames.contains(c) => StructField(c, t, nullable = nullable, m)
      case y: StructField => y
    })
    df.sqlContext createDataFrame(df.rdd, newSchema)
  }

}
