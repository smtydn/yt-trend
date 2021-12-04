package yttrend

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import yttrend.Country.Country

object CountryCategoryStats {
  /**
    * Run the Spark application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("YTTrend").getOrCreate()

    for (country <- Country.values) {
      // Read dataset and category IDs into DataFrames.
      val datasetDF: DataFrame = readDatasetAsDF(spark, country).cache()
      val categoryIdDF: DataFrame = readCategoryIdAsDF(spark, country).cache()

      val countryCategoryCount: DataFrame = countryCategoryCountDesc(datasetDF, categoryIdDF, country)
      writeDataFrameToJSON(countryCategoryCount, s"country-category-count/${country.toString.toLowerCase}")
    }

    spark.stop()
  }

  /**
    * Create a DataFrame that contains categories' names, IDs and counts of occurrences.
    * @param datasetDF DataFrame for dataset.
    * @param categoryIdDF DataFrame for category IDs.
    * @param country Country's name as a String.
    * @return DataFrame that contains the columns "category_id", "count", "title" and "country".
    */
  def countryCategoryCountDesc(datasetDF: DataFrame, categoryIdDF: DataFrame, country: Country.Value): DataFrame = {
    val df1 = datasetDF
      .withColumn("category_id", when(col("category_id").cast("int").isNotNull, col("category_id")))
      .groupBy("category_id")
      .count()
      .orderBy(desc("count"))

    val df2 = categoryIdDF
      .withColumn("items", explode(col("items")))
      .select(col("items.id"), col("items.snippet.title"))

    df1
      .join(df2, df1("category_id") === df2("id"))
      .drop("id")
      .withColumn("country", lit(country.id))
  }

  /**
    * Read CSV file for given country to a DataFrame.
    *
    * @param spark SparkSession instance.
    * @param country Country abbreviation as a String.
    * @return DataFrame for given country's dataset file.
    */
  private def readDatasetAsDF(spark: SparkSession, country: Country.Value): DataFrame = {
    val datasetPath: String = s"${datasetFolderPath.get}/${country}videos.csv"

    spark
      .read
      .option(key = "header", value = "true")
      .csv(path = datasetPath)
  }

  /**
    * Read JSON file for given country to a DataFrame.
    *
    * @param spark SparkSession instance.
    * @param country Country abbreviation as a String.
    * @return DataFrame for given country's category IDs.
    */
  private def readCategoryIdAsDF(spark: SparkSession, country: Country.Value): DataFrame = {
    val categoryIdFilePath: String = s"${datasetFolderPath.get}/${country}_category_id.json"

    spark
      .read
      .option(key = "multiLine", value = "true")
      .json(path = categoryIdFilePath)
  }

  /**
    * Write given DataFrame to a JSON file.
    * Root folder will be defined by `countryCategoryStatsOutputPath` that is declared in package.
    * `folderName` represents the child folder.
    *
    * @param df The DataFrame that will be saved.
    * @param folderName Child folder's name.
    */
  private def writeDataFrameToJSON(df: DataFrame, folderName: String): Unit = {
    df.coalesce(numPartitions = 1)
      .write.mode(SaveMode.Append)
      .json(s"${countryCategoryStatsOutputPath.get}/${folderName}")
  }
}
