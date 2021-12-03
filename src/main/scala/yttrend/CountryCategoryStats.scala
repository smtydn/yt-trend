package yttrend

import org.apache.spark.sql.{SparkSession, DataFrame}

object CountryCategoryStats {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("YTTrend").getOrCreate()

    for (country <- countries) {
      // Define `datasetPath` and `categoryIdFilePath`.
      val datasetPath: String = s"${datasetFolderPath.get}/${country}videos.csv"
      val categoryIdFilePath: String = s"${datasetFolderPath.get}/${country}_category_id.json"

      // Read dataset and category IDs into DataFrames.
      val datasetDF: DataFrame = spark
        .read
        .option(key = "header", value = "true")
        .csv(path = datasetPath)
      val categoryIdDF: DataFrame = spark
        .read
        .option(key = "multiLine", value = "true")
        .json(path = categoryIdFilePath)
    }

    spark.stop()
  }
}
