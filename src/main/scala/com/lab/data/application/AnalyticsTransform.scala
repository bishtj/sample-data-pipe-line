package com.lab.data.application

import com.lab.data.{AnaError, EitherTryHandler}
import org.apache.spark.sql.DataFrame
import cats.implicits._
import org.apache.spark.sql.functions._

import scala.util.Try
object AnalyticsTransform extends EitherTryHandler {


  def apply(df : DataFrame) : Either[AnaError, DataFrame] = {

    for {

      transformDf <- transform(df)
      resultXfrDf <- selectColumns(transformDf)

    } yield resultXfrDf
  }

  private def transform(df: DataFrame) : Either[AnaError, DataFrame] = {
    eitherR(
      Try {
        df.withColumn("role", when(col("salary").>(lit(999999)), lit("Prime Minister")).otherwise(lit("Mayor")))
      }
    )
  }

  private def selectColumns(df: DataFrame) : Either[AnaError, DataFrame] = {
    eitherR(
      Try {
        df.select(
          "id",
          "role"
        )
      }
    )
  }


}
