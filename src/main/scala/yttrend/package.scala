package yttrend

package object yttrend {
  val datasetFolderPath: String = sys.env.get("DATASET_FOLDER_PATH").get

  // CountryCategoryStats
  val ccsAppName: String = "country-category-stats"
  val ccsOutputPath: String = sys.env.getOrElse("CCS_OUTPUT_PATH", "/tmp/ccs-output")

  // LikeRatio
  val lrAppName: String = "like-ratio"
  val lrOutputPath: String = sys.env.getOrElse("LR_OUTPUT_PATH", "/tmp/lr-output")
}
