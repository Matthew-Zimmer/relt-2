package ops

import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession, Column, DataFrame, SaveMode }
import org.apache.spark.sql.functions.{ collect_list, struct, sum, lit, udf, col }

object Table3Ops {
  def execute(spark: SparkSession, df0: DataFrame, df1: DataFrame): DataFrame = {
    import spark.implicits._

    val column = "__delta_state_kind";

    val ldc = s"l${column}"
    val rdc = s"r${column}"

    val df2 = df0.withColumnRenamed(column, ldc)
    val df3 = df1.withColumnRenamed(column, rdc)

    val df = df2
      .join(df3, col("id") === col("tid"), "inner")
      .withColumn(column, lit(0))

    return df.drop(ldc, rdc)
  }
}