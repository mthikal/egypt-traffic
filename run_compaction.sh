#!/bin/bash

cd /home/ubuntu/egypt-traffic

docker run --rm \
  --network egypt-traffic_default \
  -e DEPLOY_ENV=prod \
  -e S3_BUCKET=egypt-traffic-lake-prod \
  -e AWS_REGION=eu-central-1 \
  -v $(pwd)/spark/jobs:/opt/spark/jobs \
  apache/spark:3.5.0 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark/jobs/compact_job.py \
  >> /home/ubuntu/egypt-traffic/logs/compaction.log 2>&1
