import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

object CovidApp extends App {

  val spark = SparkSession.builder()
    .appName("CovidApp")
    .master("local")
    .config("spark.es.node", "127.0.0.1")
    .config("spark.es.port", "9200")
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

    // Convert Country Name to match Covid Data
    val populationDfFixedNameCountries = populationDf
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
    populationDfFixedNameCountries.withColumnRenamed("pop2020", "Population")
  }

  def readAndCleanLocalisationData(spark: SparkSession): DataFrame = {

    val localisationDfRaw = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("./data/LocalisationData/geolocation_country.csv")
      .cache()

    val localisationDf = localisationDfRaw.select(col("name"), col("latitude"), col("longitude"))

    // Convert Country Name to match Covid Data
    val localisationDfFixedNameCountries = localisationDf
      .withColumn("Country_Name",
        when(col("name") === "United States", "US")
          .otherwise(when(col("name") === "Swaziland", "Eswatini")
            .otherwise(when(col("name") === "Macedonia [FYROM]", "North Macedonia")
              .otherwise(when(col("name") === "Congo [DRC]", "Congo (Kinshasa)")
                .otherwise(when(col("name") === "Congo [Republic]", "Congo (Brazzaville")
                  .otherwise(when(col("name") === "Moldova", "Republic of Moldova")
                    .otherwise(when(col("name") === "North Korea", "Republic of Korea")
                      .otherwise(when(col("name") === "Côte d'Ivoire", "Cote d'Ivoire")
                        .otherwise(when(col("name") === "Czech Republic", "Czechia")
                          .otherwise(col("name")))))))))))
      .drop("name")

    val concatCoordinates = localisationDfFixedNameCountries
      .withColumn("coordinates",
        struct(
          col("latitude").cast(StringType).as("lat"),
          col("longitude").cast(StringType).as("lon")))
      .drop("latitude", "longitude")

    concatCoordinates
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

    df.withColumn("diffDeathsByDay", col("Deaths") - when(lag("Deaths", 1).over(windowSpec).isNull, 0).otherwise(lag("Deaths", 1).over(windowSpec)))
      .withColumn("diffConfirmedByDay", col("Confirmed") - when(lag("Confirmed", 1).over(windowSpec).isNull, 0).otherwise(lag("Confirmed", 1).over(windowSpec)))
      .withColumn("diffRecoveredByDay", col("Recovered") - when(lag("Recovered", 1).over(windowSpec).isNull, 0).otherwise(lag("Recovered", 1).over(windowSpec)))
      .orderBy(desc("Deaths"), desc("Date"))
  }

  def joinCovidAndOtherCountryData(covidDf: DataFrame, otherDf: DataFrame): DataFrame = {

    covidDf.join(otherDf, covidDf.col("Country") === otherDf.col("Country_Name"), "inner").drop("Country_Name")
  }

  def calculateCasesPerMillion(df: DataFrame): DataFrame = {

    df.withColumn("deathsCasesPer1M", round(col("Deaths") / col("Population") * 1000, 3))
      .withColumn("confirmedCasesPer1M", round(col("Confirmed") / col("Population") * 1000, 3))
  }

  def convertCountryToESMap(df: DataFrame): DataFrame = {

    // Convert Country Name to match Elastic Search Map
    val elasticSearchNameCountries = df
      .withColumn("Country_Name",
        when(col("Country") === "US", "United States")
          .otherwise(when(col("Country") === "Libya", "Libya Arab Jamahiriya")
            .otherwise(when(col("Country") === "Macedonia [FYROM]", "North Macedonia")
              .otherwise(when(col("Country") === "Congo (Kinshasa)", "Congo")
                .otherwise(when(col("Country") === "Congo (Brazzaville)", "Democratic Republic of the Congo")
                  .otherwise(when(col("Country") === "Tanzania", "United Republic of Tanzania")
                    .otherwise(when(col("Country") === "Republic of Korea", "Korea, Democratic People's Republic of")
                      .otherwise(when(col("Country") === "South Korea", "Korea, Republic of")
                        .otherwise(when(col("Country") === "Syria", "Syrian Arab Republic")
                          .otherwise(when(col("Country") === "Czechia", "Czech Republic")
                            .otherwise(when(col("Country") === "Iran", "Iran (Islamic Republic of)")
                              .otherwise(when(col("Country") === "Laos", "Laos People's Democratic Republic")
                                .otherwise(when(col("Country") === "North Macedonia", "The former Yugoslav Republic of Macedonia")
                                  .otherwise(col("Country")))))))))))))))
          .drop("Country")
    elasticSearchNameCountries
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

  val dfDiffWithPop = joinCovidAndOtherCountryData(dfDiff, populationDf)

  val dfDiffWithCasesPerMillions = calculateCasesPerMillion(dfDiffWithPop)

  val localisationDf = readAndCleanLocalisationData(spark)

  val dfDiffWithLocalisation = joinCovidAndOtherCountryData(dfDiffWithCasesPerMillions, localisationDf)

  val dfWithESMapCountryName = convertCountryToESMap(dfDiffWithLocalisation)

  dfWithESMapCountryName.saveToEs("covid")
}

