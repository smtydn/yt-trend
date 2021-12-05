# Trending YouTube Video Statistics
A Spark project for extracting various statistics from YouTube Video Statistics.

## Dataset
The dataset is shared at [Kaggle - Trending YouTube Video Statistics](https://www.kaggle.com/datasnaek/youtube-new).

## Applications
- Country Category Stats: Extracts number of videos per categories that was in trends for the countries that in the dataset.
Class name: `yttrend.CountryCategoryStats`
- Like Ratio: Extracts like-dislike ratio for categories.
Class name: `yttrend.LikeRatio`

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
--class "<class_name>"
<jar_path> 
```

## Environment Variables
- `DATASET_FOLDER_PATH`: Folder path that the dataset exists. E.g. `/tmp/dataset`
- `CCS_OUTPUT_PATH`: (default: `/tmp/ccs-output`) Folder path that will contain extraction results for `CountryCategoryStats`
- `LR_OUTPUT_PATH`: (default: `/tmp/lr-output`) Folder path that will contain extraction results for `LikeRatio`