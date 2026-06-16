from datetime import datetime, timedelta
import os

from pyspark.sql import SparkSession

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
S3_BUCKET = os.environ["S3_BUCKET"]

DEPLOY_ENV = os.getenv("DEPLOY_ENV", "local")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def build_spark():
    builder = (
        SparkSession.builder
        .appName("EgyptTrafficCompaction")
        .master("spark://spark-master:7077")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
    )

    if DEPLOY_ENV == "prod":
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
    else:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    return builder.getOrCreate()


def compact_partition(spark, path, partitions=4):
    print(f"Compacting: {path}")

    try:
        df = spark.read.parquet(path)

        count = df.count()

        if count == 0:
            print(f"Skipping empty partition: {path}")
            return

        temp_path = path + "_tmp"

        (
            df.repartition(partitions)
            .write
            .mode("overwrite")
            .parquet(temp_path)
        )

        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path),
            hadoop_conf,
        )

        original = spark._jvm.org.apache.hadoop.fs.Path(path)
        temp = spark._jvm.org.apache.hadoop.fs.Path(temp_path)
        backup = spark._jvm.org.apache.hadoop.fs.Path(path + "_old")

        fs.delete(backup, True)

        fs.rename(original, backup)
        fs.rename(temp, original)
        fs.delete(backup, True)

        print(f"Compacted {count} rows: {path}")

    except Exception as e:
        print(f"Skipping {path}: {e}")


def main():
    spark = build_spark()

    target = datetime.utcnow().date() - timedelta(days=1)

    year = target.year
    month = target.month
    day = target.day

    datasets = [
        "bronze/flow",
        "bronze/incidents",
        "silver/flow",
        "silver/incidents",
    ]

    for dataset in datasets:
        path = (
            f"s3a://{S3_BUCKET}/"
            f"{dataset}/"
            f"year={year}/month={month}/day={day}"
        )

        compact_partition(spark, path)

    spark.stop()


if __name__ == "__main__":
    main()
