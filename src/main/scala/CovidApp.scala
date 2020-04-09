import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CovidApp extends App {

  val spark = SparkSession.builder()
    .appName("CovidApp")
    .master("local")
    .getOrCreate()

  def readAndCleanData1(spark: SparkSession, folder: String, updateColumnName: String): DataFrame = {

    val covidDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"./data/CovidData/$folder/*.csv")
      .cache()

    covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = covidDfRaw
      .withColumn("Date", to_date(col(updateColumnName)))

    // Convert Mainland China as China
    val covidDfFixedChina = covidDfWithDate
      .withColumn("Country", when(col("Country/Region") === "Mainland China", "China").otherwise(col("Country/region")))
      .drop("Country/Region")

    // Convert column name
    val covidDfFixed = covidDfFixedChina.withColumnRenamed("Province/State", "Province_State")

    // Keep only Country, Date, Deaths, Confirmed, Recovered columns
    val covidDf = covidDfFixed.select(col("Province_State"), col("Country"), col("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
    covidDf
  }

  def readAndCleanData2(spark: SparkSession, folder: String, updateColumnName: String): DataFrame = {

    val covidDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"./data/CovidData/$folder/*.csv")
      .cache()

    covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = covidDfRaw
      .withColumn("Date", to_date(col(updateColumnName)))

    // Convert column name
    val covidDfFixed = covidDfWithDate.withColumnRenamed("Country_Region", "Country")

    // Keep only Country, Date, Deaths, Confirmed, Recovered columns
    val covidDf = covidDfFixed.select(col("Province_State"), col("Country"), col("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
    covidDf
  }


  val df1 = readAndCleanData1(spark, "schema_1", "Last Update")
  val df2 = readAndCleanData1(spark, "schema_2", "Last Update")
  val df3 = readAndCleanData2(spark, "schema_3", "Last_Update")

  val combinedDf = df1.unionByName(df2).unionByName(df3).cache()
  combinedDf.show()
  print(combinedDf.count())
}

