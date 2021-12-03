package object yttrend {
  val datasetFolderPath: Option[String] = sys.env.get("DATASET_FOLDER_PATH")
  val countries: List[String] = List("CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US")
}
