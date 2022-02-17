This module creates use the Great Expectations package to validate mahle data across the pipeline. First it checks 
raw data from S3 coming in as parquet files every 15 minutes. Then it loads files from Omni that correspond to different
stages of the pipeline. Those files are then validated against an expectations suite. For each validation action, we create
an expectations suite based on historical data. The expectations were built with an automatic profiler against historical data. 
These expectations can be tweaked if the profiler results in too strict expectations. 

