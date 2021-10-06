package nl.javadb

import org.apache.spark.SparkFiles
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_timestamp, row_number, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadData {
  val schema = "Airline string, AirlineID int, SourceAirport string, SourceAirportID int, DestinationAirport string, DestinationAirportID int, Codeshare string, Stops int, Equipment string"
  val csvDir = "/tmp/routesdir"
  // Load 10 files per batch to create a stream of CSV data
  var timeoutQuery = 100000
  var filesPerTrigger = 10

  // Load CSV data from URL into file and save this as a partitioned Spark data file
  def GetUrlContentCSV(spark: SparkSession, url: String): DataFrame = {
    spark.sparkContext.addFile(url)
    val fileName = url.replaceAll(".*/", "")

    val df = spark.read
      .option("inferSchema", true)
      .option("header", false)
      .option("nullValue", "\\N")
      .csv("file://" + SparkFiles.get(fileName))
      .toDF("Airline", "AirlineID", "SourceAirport", "SourceAirportID", "DestinationAirport", "DestinationAirportID", "Codeshare", "Stops", "Equipment")

    // Save into 100 partitions (see comment related to filesPerTrigger)
    df.repartition(100).write.mode("overwrite").option("header", true).csv(csvDir)
    df
  }

  // Group the data and save the output as a CSV file
  def SaveGrouping(df: DataFrame) = {
    val res = df.groupBy("SourceAirport").count().orderBy(col("count").desc).limit(10)
    res.coalesce(1).write.mode("overwrite").option("header", true).csv("/tmp/outputdir")
  }

  // Load the partitioned CSV data as a structured stream
  def LoadCSVFile(spark: SparkSession) = {
    spark.readStream
      .option("inferSchema", true)
      .option("header", true)
      .option("maxFilesPerTrigger", filesPerTrigger)
      .schema(schema)
      .csv(csvDir)
  }

  // Execute the default grouping on the streamed data
  def PlainGrouping(spark: SparkSession) = {
    var dfs = LoadCSVFile(spark)
    dfs = dfs.groupBy("SourceAirport").count().orderBy(col("count").desc).limit(10)
    // Run run the query and save to memory as a table
    val query = dfs.writeStream
      .outputMode("complete")
      .format("memory")
      .option("queryName", "groupedoutput")
      .start()

    query.awaitTermination(timeoutQuery)
    // Load the table and show the output
    val df = spark.sql("select * from groupedoutput")
    df.show(20, false)
    df
  }

  // Execute the windowed grouping query on the streamed data
  def GroupingWithWindow(spark: SparkSession) = {
    var dfs = LoadCSVFile(spark)
    // Add timestamp to perform the window function on
    dfs = dfs.withColumn("timestamp", current_timestamp())
    // Perform query for the window function (including watermark)
    dfs = dfs
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"), col("SourceAirport")).count()
    // Save the result to memory as a table
    val query = dfs.writeStream
      .outputMode("complete")
      .format("memory")
      .option("queryName", "groupedwindowoutput")
      .start()

    // Perform the top 10 per window on the saved output
    query.awaitTermination(timeoutQuery)
    val df = spark.sql("select * from groupedwindowoutput")
    val countWindowByFreq = Window.partitionBy(col("window")).orderBy(col("count").desc)
    // Rank the data per window based on the count
    var ranked_data = df.withColumn("count_rank", row_number over countWindowByFreq)
    // Only pass the top 10 per window as the output and order by (window, count_rank)
    ranked_data = ranked_data.filter(col("count_rank") <= 10).orderBy(col("window").asc, col("count_rank").asc)
    ranked_data.show(1000, false)
    ranked_data
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SchipholAirport")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Assignment 1: load data, group by source airport and save to filesystem
    val url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
    val df = GetUrlContentCSV(spark, url)
    SaveGrouping(df)

    // Assignment 2: load data as stream and perform the same grouping (see assignment 1)
    PlainGrouping(spark)

    // Assignment 3: load data as stream and show top 10 per window
    GroupingWithWindow(spark)
  }
}
