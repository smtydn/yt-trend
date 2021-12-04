package object yttrend {
  val datasetFolderPath: Option[String] = sys.env.get("DATASET_FOLDER_PATH")
  val countryCategoryStatsOutputPath: Option[String] = sys.env.get("CCS_OUTPUT_PATH")
}
