package yttrend

package object yttrend {
  val datasetFolderPath: String = sys.env.get("DATASET_FOLDER_PATH").get

  // CountryCategoryStats
  val ccsAppName: String = "country-category-stats"
  val ccsOutputPath: String = sys.env.get("CCS_OUTPUT_PATH").get
}
