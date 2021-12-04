# Trending YouTube Video Statistics
A Spark project for extracting various statistics from YouTube Video Statistics.

## Dataset
The dataset is shared at [Kaggle - Trending YouTube Video Statistics](https://www.kaggle.com/datasnaek/youtube-new).

## Running
For running the Spark application:
1. Build the JAR.
```bash
$ sbt package
```
2. Run `spark-submit` command. The JAR should be created under `<project_root>/target/scala-2.13`.
It should be named as `trending-youtube-video-statistics_2.13-<version>.jar`.
```bash
$ spark-submit \
--class "yttrend.CountryCategoryStats"
<jar_path> 
```

## Environment Variables
- `DATASET_FOLDER_PATH`: Folder path that the dataset exists. E.g. `/tmp/dataset`
- `CCS_OUTPUT_PATH`: Folder path that will contain extraction results for `CountryCategoryStats`