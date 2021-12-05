package yttrend

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import yttrend._

object LikeRatio {
   /**
    * Run the Spark application.
    * 
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName(lrAppName)
      .getOrCreate()

    import spark.implicits._
    var resultDF: DataFrame = Seq.empty[(Integer, String, Integer, Integer, Float)].toDF(
      Seq("category_id", "category_title", "likes", "dislikes", "ratio"):_*
    )

    for (country <- Country.values) {
      // Read dataset and category IDs into DataFrames.
      val datasetDF: DataFrame = readDatasetAsDF(spark, country)
      val categoryIdDF: DataFrame = readCategoryIdAsDF(spark, country)

      val likeRatioDF: DataFrame = likeRatio(datasetDF, categoryIdDF)
      resultDF = resultDF.union(likeRatioDF)
    }

    writeDataFrameToJSON(resultDF)

    spark.stop()
  }

  /**
    * Create a DataFrame that holds like-dislike counts and their ratios for a category.
    * The method joins given DataFrames and calculates the ratio between likes and dislikes.
    * 
    * @param datasetDF The dataset as a DataFrame.
    * @param categoryIdDF Category ID's as a DataFrame.
    * @return The DataFrame that has the columns "category_id", "likes", "dislikes", "ratio" and "category_title".
    */
  def likeRatio(datasetDF: DataFrame, categoryIdDF: DataFrame): DataFrame = {
    datasetDF
      .groupBy(col("category_id"))
      .agg(
        sum("likes").as("likes"),
        sum("dislikes").as("dislikes")
      )
      .withColumn("ratio", round((col("likes") / (col("likes") + col("dislikes"))) * 100, 2))
      .join(categoryIdDF, "category_id")
      .select(
        col("category_id"),
        col("category_title"),
        col("likes"),
        col("dislikes"),
        col("ratio")
      )
  }

  /**
    * Read CSV file for given country to a DataFrame.
    * The DataFrame has "category_id", "likes" and "dislikes" columns.
    *
    * @param spark SparkSession instance.
    * @param country Country abbreviation as a String.
    * @return DataFrame for given country's dataset file.
    */
  private def readDatasetAsDF(spark: SparkSession, country: Country.Value): DataFrame = {
    val datasetPath: String = s"${datasetFolderPath}/${country}videos.csv"

    spark
      .read
      .option(key = "header", value = "true")
      .csv(path = datasetPath)
      .select(
        col("category_id").cast("int"),
        col("likes").cast("int"),
        col("dislikes").cast("int")
      )
  }

  /**
    * Read JSON file for given country to a DataFrame.
    * The DataFrame has "category_id" and "category_title" columns.
    *
    * @param spark SparkSession instance.
    * @param country Country abbreviation as a String.
    * @return DataFrame for given country's category IDs.
    */
  private def readCategoryIdAsDF(spark: SparkSession, country: Country.Value): DataFrame = {
    val categoryIdFilePath: String = s"${datasetFolderPath}/${country}_category_id.json"

    spark
      .read
      .option(key = "multiLine", value = "true")
      .json(path = categoryIdFilePath)
      .withColumn("items", explode(col("items")))
      .select(
        col("items.id").cast("int").as("category_id"),
        col("items.snippet.title").as("category_title")
      )
  }

  /**
    * Write given DataFrame to a JSON file.
    * Root folder will be defined by `countryCategoryStatsOutputPath` that is declared in package.
    * `folderName` represents the child folder.
    *
    * @param df The DataFrame that will be saved.
    * @param folderName Child folder's name.
    */
  private def writeDataFrameToJSON(df: DataFrame): Unit = {
    df.coalesce(numPartitions = 1)
      .write.mode(SaveMode.Append)
      .json(lrOutputPath)
  }
}
