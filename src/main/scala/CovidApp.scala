import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CovidApp extends App {

  val spark = SparkSession.builder()
    .appName("CovidApp")
    .master("local")
    .getOrCreate()

  def getDateDf(df: DataFrame, flag: Boolean, updateColumnName: String, dateFormat: String): DataFrame = {

    // Get last time update as Date after converting to timestamp
    if (flag) {
      val finalDf = df.withColumn("Date", from_unixtime(unix_timestamp(col(updateColumnName), dateFormat), "yyyy-MM-dd"))
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

    // covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = getDateDf(covidDfRaw, needTimestampFlag, updateColumnName, "M/dd/yyyy")

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

    //covidDfRaw.printSchema()

    // Get last time update as Date
    val covidDfWithDate = getDateDf(covidDfRaw, needTimestampFlag, updateColumnName, "M/dd/yy")

    // Convert column name
    val covidDfFixed = covidDfWithDate.withColumnRenamed("Country_Region", "Country")

    // Keep only Country, Date, Deaths, Confirmed, Recovered columns
    val covidDf = covidDfFixed.select(col("Province_State"), col("Country"), col("Date"), col("Confirmed"), col("Deaths"), col("Recovered"))
    covidDf
  }

  def readAndCleanPopulationData(spark: SparkSession): DataFrame = {

    val populationDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"./data/PopulationData/world_population.csv")
      .cache()

    val populationDf = populationDfRaw.select(col("name"), col("pop2020"))

    // Convert Name to match Covid Data
    val populationDfFixedUS = populationDf
      .withColumn("Country_Name",
        when(col("name") === "United States", "US")
              .otherwise(when(col("name") === "Swaziland", "Eswatini")
                .otherwise(when(col("name") === "Macedonia", "North Macedonia")
                  .otherwise(when(col("name") === "DR Congo", "Congo (Kinshasa)")
                    .otherwise(when(col("name") === "Republic of the Congo", "Congo (Brazzaville")
                          .otherwise(when(col("name") === "Moldova", "Republic of Moldova")
                              .otherwise(when(col("name") === "North Korea", "Republic of Korea")
                                .otherwise(when(col("name") === "Ivory Coast", "Cote d'Ivoire")
                                  .otherwise(when(col("name") === "Czech Republic", "Czechia")
                                      .otherwise(col("name")))))))))))
          .drop("name")

          // Convert column name
          populationDfFixedUS.withColumnRenamed("pop2020", "Population")
  }

  def aggregateByCountryByDate(df: DataFrame): DataFrame = {

    df.groupBy(col("Country"), col("Date"))
      .agg(sum(col("Deaths")).as("Deaths"),
        sum(col("Confirmed")).as("Confirmed"),
        sum(col("Recovered")).as("Recovered"))
      .orderBy(col("Deaths").desc, col("Date"))
  }

  def calculateDifferenceBetweenDays(df: DataFrame): DataFrame = {

    val windowSpec = Window.partitionBy("Country").orderBy("Date")

    val countByCountryDiffDeathsDf = df
      .withColumn("Difference Deaths By Day", col("Deaths") - when(lag("Deaths", 1).over(windowSpec).isNull, 0).otherwise(lag("Deaths", 1).over(windowSpec)))
      .orderBy(desc("Deaths"), desc("Date"))

    val countByCountryDiffConfirmedDf = countByCountryDiffDeathsDf
      .withColumn("Difference Confirmed By Day", col("Confirmed") - when(lag("Confirmed", 1).over(windowSpec).isNull, 0).otherwise(lag("Confirmed", 1).over(windowSpec)))
      .orderBy(desc("Deaths"), desc("Date"))

    countByCountryDiffConfirmedDf
      .withColumn("Difference Recovered By Day", col("Recovered") - when(lag("Recovered", 1).over(windowSpec).isNull, 0).otherwise(lag("Recovered", 1).over(windowSpec)))
      .orderBy(desc("Deaths"), desc("Date"))
  }

  val df1a = readAndCleanData1(spark, "schema_1/no_iso_date", "Last Update", true)
  val df1b = readAndCleanData1(spark, "schema_1/iso_date", "Last Update", false)
  val df2 = readAndCleanData1(spark, "schema_2", "Last Update", false)
  val df3a = readAndCleanData2(spark, "schema_3/no_iso_date", "Last_Update", true)
  val df3b = readAndCleanData2(spark, "schema_3/iso_date", "Last_Update", false)

  val combinedDf = df1a.unionByName(df1b).unionByName(df2).unionByName(df3a).unionByName(df3b).cache()

  val dfAggregate = aggregateByCountryByDate(combinedDf)

  val dfDiff = calculateDifferenceBetweenDays(dfAggregate)

  val populationDf = readAndCleanPopulationData(spark)

  val dfDiffWithPop = dfDiff.join(populationDf, dfDiff.col("Country") === populationDf.col("Country_Name"), "inner").drop("Country_Name")

  //dfDiffWithPop.show()

  dfDiffWithPop.filter(col("Country") === "France").show()
}

