import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CovidApp extends App {

  val spark = SparkSession.builder()
    .appName("CovidApp")
    .master("local")
    .getOrCreate()

  def getDateDf(df: DataFrame, flag: Boolean, updateColumnName: String): DataFrame = {

    // Get last time update as Date after converting to timestamp
    if(flag) {
      val finalDf = df.withColumn("Date", from_unixtime(unix_timestamp(col(updateColumnName), "M/dd/yyyy"), "yyyy-MM-dd"))
      finalDf
    } else {
      val finalDf = df.withColumn("Date", to_date(col(updateColumnName)))
      finalDf
    }
  }

  def readAndCleanData1(spark: SparkSession, folder: String, updateColumnName: String, needTimestampFlag: Boolean): DataFrame = {

    val covidDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"./data/CovidData/$folder/*.csv")
      .cache()

    covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = getDateDf(covidDfRaw, needTimestampFlag, updateColumnName)

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

  def readAndCleanData2(spark: SparkSession, folder: String, updateColumnName: String, needTimestampFlag: Boolean): DataFrame = {

    val covidDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"./data/CovidData/$folder/*.csv")
      .cache()

    covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = getDateDf(covidDfRaw, needTimestampFlag, updateColumnName)

    // Convert column name
    val covidDfFixed = covidDfWithDate.withColumnRenamed("Country_Region", "Country")

    // Keep only Country, Date, Deaths, Confirmed, Recovered columns
    val covidDf = covidDfFixed.select(col("Province_State"), col("Country"), col("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
    covidDf
  }


  val df1a = readAndCleanData1(spark, "schema_1/no_iso_date", "Last Update", true)
  val df1b = readAndCleanData1(spark, "schema_1/iso_date", "Last Update", false)
  val df2 = readAndCleanData1(spark, "schema_2", "Last Update", false)
  val df3a = readAndCleanData2(spark, "schema_3/no_iso_date", "Last_Update", true)
  val df3b = readAndCleanData2(spark, "schema_3/iso_date", "Last_Update", false)

  val combinedDf = df1a.unionByName(df1b).unionByName(df2).unionByName(df3a).unionByName(df3b).cache()

}

