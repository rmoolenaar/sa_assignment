package nl.javadb

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoadDataSpec extends AnyFlatSpec with Matchers {
  // Start default Spark session without INFO messages
  val spark = SparkSession.builder
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  // Load full data set at once (max 100 files) and do not wait too long
  LoadData.filesPerTrigger = 100
  LoadData.timeoutQuery = 15000

  // Load test data
  val testData = LoadData.GetUrlContentCSV(spark, "src/test/resources/routes.txt")

  // Test output of default grouping query
  "Stream aggregations" should "work" in {
    val df = LoadData.PlainGrouping(spark)

    // assert expected columns
    df.columns should contain theSameElementsAs Seq("SourceAirport", "count")

    // assert expected elements in the "SourceAirport" column
    val ports = df.select("SourceAirport").collect.map(_.getString(0))
    ports should contain theSameElementsAs Seq("DME", "CEK", "ASF", "KZN", "AER")
    // assert expected elements in the "count" column
    val counts = df.select("count").collect.map(_.getLong(0))
    counts should contain theSameElementsAs Seq(3, 2, 2, 1, 1)
  }

  // Test output of grouping query using windows
  "Stream aggregations with window" should "work" in {
    val df = LoadData.GroupingWithWindow(spark)

    // assert expected columns
    df.columns should contain theSameElementsAs Seq("window", "SourceAirport", "count", "count_rank")

    // assert expected elements in the "SourceAirport" column
    val ports = df.select("SourceAirport").collect.map(_.getString(0))
    ports should contain theSameElementsAs Seq("DME", "CEK", "ASF", "KZN", "AER", "DME", "CEK", "ASF", "KZN", "AER")
    // assert expected elements in the "count" column
    val counts = df.select("count").collect.map(_.getLong(0))
    counts should contain theSameElementsAs Seq(3, 2, 2, 1, 1, 3, 2, 2, 1, 1)
    val ranks = df.select("count_rank").collect.map(_.getInt(0))
    ranks should contain theSameElementsAs Seq(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
  }
}
