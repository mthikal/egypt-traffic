"""
Egypt Traffic — Spark Structured Streaming Job
================================================
Pipeline:
  Kafka → Bronze (Parquet, raw, partitioned by date)
        → Silver flow   (15-min tumbling window aggregates + all derived fields)
        → Silver incidents (per-incident rows + all derived fields)
        → Gold fact_traffic_flow  (star schema → PostgreSQL via foreachBatch)
        → Gold fact_incidents     (star schema → PostgreSQL via foreachBatch)

Architecture reference: 06_medallion_architecture.md
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

# ──────────────────────────────────────────────────────────────────────────────
# Config — all sensitive values come from environment variables
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP     = os.environ["KAFKA_BOOTSTRAP"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]   # kept for reference / future SR use
S3_BUCKET           = os.environ["S3_BUCKET"]
# AWS_ACCESS_KEY      = os.environ["AWS_ACCESS_KEY_ID"]
# AWS_SECRET_KEY      = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION          = os.getenv("AWS_REGION", "eu-central-1")
JDBC_URL            = os.getenv("JDBC_URL", "")
POSTGRES_PASSWORD   = os.getenv("POSTGRES_PASSWORD", "traffic")
CHECKPOINT_BASE     = f"s3a://{S3_BUCKET}/checkpoints"
DEPLOY_ENV          = os.getenv("DEPLOY_ENV", "local")

TOPIC_FLOW      = "traffic-flow"
TOPIC_INCIDENTS = "traffic-incidents"

# ──────────────────────────────────────────────────────────────────────────────
# Avro schemas — read from mounted .avsc files
# ──────────────────────────────────────────────────────────────────────────────

SCHEMA_DIR = "/opt/spark/schemas"

if not os.path.exists(SCHEMA_DIR):
    raise RuntimeError(
        f"Schema directory not found: {SCHEMA_DIR}"
    )

with open(f"{SCHEMA_DIR}/traffic_flow.avsc") as f:
    FLOW_AVRO_SCHEMA = f.read()

with open(f"{SCHEMA_DIR}/traffic_incident.avsc") as f:
    INCIDENT_AVRO_SCHEMA = f.read()

# ──────────────────────────────────────────────────────────────────────────────
# City mapping — full location list (flow location_name + incident bbox_name)
# ──────────────────────────────────────────────────────────────────────────────

CITY_MAP = {
    # Cairo Ring Road
    "ring_road_north":       "cairo",
    "ring_road_east":        "cairo",
    "ring_road_south":       "cairo",
    "ring_road_west":        "cairo",
    # Downtown Cairo
    "tahrir_square":         "cairo",
    "ramses_square":         "cairo",
    "salah_salem_north":     "cairo",
    "salah_salem_south":     "cairo",
    "corniche_downtown":     "cairo",
    # Airport corridor
    "cairo_airport":         "cairo",
    # October 6th / Mehwar
    "6th_october_bridge":    "cairo",
    "mehwar_north":          "cairo",
    # Giza
    "giza_square":           "cairo",
    # East Cairo
    "new_admin_capital":     "cairo",
    "new_cairo_90th":        "cairo",
    # Alexandria — flow locations
    "alex_corniche_east":    "alexandria",
    "alex_corniche_west":    "alexandria",
    "alex_victoria_square":  "alexandria",
    "alex_port_area":        "alexandria",
    "alex_montaza":          "alexandria",
    "alex_abu_qir_road":     "alexandria",
    # Incident bbox names
    "cairo_downtown":        "cairo",
    "cairo_east":            "cairo",
    "cairo_ring_road":       "cairo",
    "alexandria":            "alexandria",
}


def city_expr(col_name: str) -> F.Column:
    """Build a chained WHEN expression that maps location/bbox name → city."""
    expr = F.lit("unknown")
    for loc, city in CITY_MAP.items():
        expr = F.when(F.col(col_name) == loc, city).otherwise(expr)
    return expr


# ──────────────────────────────────────────────────────────────────────────────
# FRC label mapping (Functional Road Class)
# ──────────────────────────────────────────────────────────────────────────────

def frc_label_expr() -> F.Column:
    return (
        F.when(F.col("frc") == "FRC0", "Motorway / Freeway")
         .when(F.col("frc") == "FRC1", "Major Road")
         .when(F.col("frc") == "FRC2", "Secondary Road")
         .when(F.col("frc") == "FRC3", "Connecting Road")
         .when(F.col("frc") == "FRC4", "Minor Road")
         .when(F.col("frc") == "FRC5", "Local Road")
         .when(F.col("frc") == "FRC6", "Private / Unpaved Road")
         .otherwise("Unknown")
    )


# ──────────────────────────────────────────────────────────────────────────────
# Magnitude label mapping
# ──────────────────────────────────────────────────────────────────────────────

def magnitude_label_expr() -> F.Column:
    return (
        F.when(F.col("magnitude") == 0, "Unknown")
         .when(F.col("magnitude") == 1, "Minor")
         .when(F.col("magnitude") == 2, "Moderate")
         .when(F.col("magnitude") == 3, "Major")
         .when(F.col("magnitude") == 4, "Undefined")
         .otherwise("Unknown")
    )


# ──────────────────────────────────────────────────────────────────────────────
# JDBC properties for Gold writes
# ──────────────────────────────────────────────────────────────────────────────

JDBC_PROPS = {
    "driver":   "org.postgresql.Driver",
    "user":     "traffic",
    "password": POSTGRES_PASSWORD,
}

# ──────────────────────────────────────────────────────────────────────────────
# Spark session
# ──────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("EgyptTrafficStreaming")
        .master("spark://spark-master:7077")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.sql.streaming.checkpointLocation",
            CHECKPOINT_BASE
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            "true"
        )
    )

    if DEPLOY_ENV == "prod":
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
    else:
        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.access.key",
                AWS_ACCESS_KEY
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                AWS_SECRET_KEY
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
# def build_spark() -> SparkSession:
#     spark = (
#         SparkSession.builder
#         .appName("EgyptTrafficStreaming")
#         .master("spark://spark-master:7077")
#         .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
#         .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
#         .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config(
#             "spark.hadoop.fs.s3a.aws.credentials.provider",
#             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
#         )
#         .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
#         .config("spark.sql.shuffle.partitions", "4")
#         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel("WARN")
#     return spark


# ──────────────────────────────────────────────────────────────────────────────
# Kafka source
# ──────────────────────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .option("failOnDataLoss", "false")
        .load()
    )


# ──────────────────────────────────────────────────────────────────────────────
# Avro deserialization — strips the 5-byte Confluent Schema Registry header
# before passing payload to from_avro()
# ──────────────────────────────────────────────────────────────────────────────

def deserialize_avro(df: DataFrame, avro_schema: str) -> DataFrame:
    """
    Confluent wire format: [magic byte 0x00] [4-byte schema-id] [avro payload]
    substring(value, 6, ...) skips those 5 bytes (Spark SQL is 1-indexed).
    """
    return (
        df.withColumn(
            "data",
            from_avro(
                F.expr("substring(value, 6, length(value) - 5)"),
                avro_schema,
            ),
        )
        .select("data.*", "timestamp")
    )


# ──────────────────────────────────────────────────────────────────────────────
# BRONZE — raw Parquet, append-only, partitioned by ingestion date
# Architecture: Bronze stores exactly what arrived; no transforms, no filtering.
# Format: Parquet (not JSON) — consistent with architecture doc.
# ──────────────────────────────────────────────────────────────────────────────

def write_bronze(df: DataFrame, name: str):
    """
    Partition by year/month/day derived from ingested_at.
    df must still contain ingested_at (call before any Silver transforms).
    """
    bronze_path = f"s3a://{S3_BUCKET}/bronze/{name}"
    ingestion_ts = F.from_unixtime(F.col("ingested_at") / 1000)
    return (
        df
        .withColumn("year",  F.year(ingestion_ts))
        .withColumn("month", F.month(ingestion_ts))
        .withColumn("day",   F.dayofmonth(ingestion_ts))
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_{name}")
        .partitionBy("year", "month", "day")
        .trigger(processingTime="1 minute")
        .start()
    )


# ──────────────────────────────────────────────────────────────────────────────
# SILVER — FLOW
# Architecture grain: one row per location per 15-minute tumbling window.
# All derived fields computed here (not in Gold, not in BI tools).
# ──────────────────────────────────────────────────────────────────────────────

def transform_flow_silver(raw_df: DataFrame) -> DataFrame:
    """
    Step 1: enrich each raw reading with event_time, city, frc_label,
            congestion_ratio (fill nulls), congestion_level, confidence_band,
            speed_drop_pct, travel_time_delay fields.
    Step 2 (below in flow_silver_df): tumbling 15-min window aggregation.

    NOTE: ingested_at is kept here because Bronze is written from raw_df
    directly. This function is called on raw_df AFTER Bronze is started.
    """
    return (
        raw_df
        # ── Geo coords — prefer segment midpoint, fall back to query point ───
        # Source schema has query_lat/lon (always present) and
        # segment_lat/lon (nullable — present when TomTom returns geometry).
        .withColumn("lat", F.coalesce(F.col("segment_lat"), F.col("query_lat")))
        .withColumn("lon", F.coalesce(F.col("segment_lon"), F.col("query_lon")))
        # ── Timestamps ──────────────────────────────────────────────────────
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("ingested_at") / 1000)),
        )
        # ── Geography ───────────────────────────────────────────────────────
        .withColumn("city", city_expr("location_name"))
        # ── FRC label ───────────────────────────────────────────────────────
        .withColumn("frc_label", frc_label_expr())
        # ── Congestion ratio — fill null when free_flow_speed was 0 ─────────
        .withColumn(
            "congestion_ratio",
            F.coalesce(F.col("congestion_ratio"), F.lit(1.0)),
        )
        # ── Derived: speed drop % ────────────────────────────────────────────
        # ((free_flow - current) / free_flow) * 100; guard div-by-zero
        .withColumn(
            "speed_drop_pct",
            F.when(
                F.col("free_flow_speed_kmh") > 0,
                F.round(
                    (F.col("free_flow_speed_kmh") - F.col("current_speed_kmh"))
                    / F.col("free_flow_speed_kmh") * 100,
                    2,
                ),
            ).otherwise(F.lit(None).cast("float")),
        )
        # ── Derived: travel time delay ───────────────────────────────────────
        .withColumn(
            "travel_time_delay_seconds",
            F.col("current_travel_time") - F.col("free_flow_travel_time"),
        )
        .withColumn(
            "travel_time_delay_minutes",
            F.round(
                (F.col("current_travel_time") - F.col("free_flow_travel_time")) / 60.0,
                2,
            ),
        )
        # ── Derived: congestion severity score (0=free, 100=standstill) ──────
        .withColumn(
            "congestion_severity_score",
            F.round((F.lit(1.0) - F.col("congestion_ratio")) * 100, 2),
        )
        # ── Derived: congestion level label ──────────────────────────────────
        .withColumn(
            "congestion_level",
            F.when(F.col("road_closed"),              "road_closed")
             .when(F.col("congestion_ratio") >= 0.95, "free_flow")
             .when(F.col("congestion_ratio") >= 0.75, "light")
             .when(F.col("congestion_ratio") >= 0.50, "moderate")
             .otherwise(                               "heavy"),
        )
        # ── Derived: confidence band ─────────────────────────────────────────
        .withColumn(
            "confidence_band",
            F.when(F.col("confidence") >= 0.8, "high")
             .when(F.col("confidence") >= 0.5, "medium")
             .otherwise(                        "low"),
        )
    )


def flow_silver_df(enriched_df: DataFrame) -> DataFrame:
    """
    15-minute tumbling window aggregation over the enriched per-reading df.
    Watermark of 2 minutes tolerates minor late arrival.
    Output grain: location_name × window_start (matches silver_flow schema in md).
    """
    return (
        enriched_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "15 minutes"),
            F.col("location_name"),
            F.col("city"),
            # frc is constant per location — carry through with first()
        )
        .agg(
            # Speed
            F.avg("current_speed_kmh")   .alias("avg_speed_kmh"),
            F.min("current_speed_kmh")   .alias("min_speed_kmh"),
            F.max("current_speed_kmh")   .alias("max_speed_kmh"),
            F.avg("free_flow_speed_kmh") .alias("avg_free_flow_kmh"),
            # Congestion ratio
            F.avg("congestion_ratio")    .alias("avg_congestion_ratio"),
            F.min("congestion_ratio")    .alias("min_congestion_ratio"),
            F.max("congestion_ratio")    .alias("max_congestion_ratio"),
            # Travel time
            F.avg("current_travel_time") .alias("avg_travel_time_seconds"),
            F.avg("free_flow_travel_time").alias("avg_free_flow_travel_time_seconds"),
            F.avg("travel_time_delay_seconds").alias("avg_travel_time_delay_seconds"),
            F.avg("travel_time_delay_minutes").alias("avg_travel_time_delay_minutes"),
            # Derived
            F.avg("speed_drop_pct")             .alias("avg_speed_drop_pct"),
            F.avg("congestion_severity_score")  .alias("avg_congestion_severity_score"),
            F.avg("confidence")                 .alias("avg_confidence"),
            # Carry scalar fields — same per location within a window
            F.first("frc",             ignorenulls=True).alias("frc_code"),
            F.first("frc_label",       ignorenulls=True).alias("frc_label"),
            F.first("congestion_level",ignorenulls=True).alias("congestion_level"),
            F.first("confidence_band", ignorenulls=True).alias("confidence_band"),
            F.first("road_closed",     ignorenulls=True).alias("road_closed_flag"),
            F.first("lat",             ignorenulls=True).alias("lat"),
            F.first("lon",             ignorenulls=True).alias("lon"),
            F.count("*")                                .alias("sample_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


def write_silver_flow(df: DataFrame):
    silver_path = f"s3a://{S3_BUCKET}/silver/flow"
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_flow")
        .partitionBy("city")
        .trigger(processingTime="1 minute")
        .start()
    )


# ──────────────────────────────────────────────────────────────────────────────
# SILVER — INCIDENTS
# Architecture grain: one row per incident per 15-minute ingestion window.
# NOT aggregated — each incident keeps its own row (details matter for Gold).
# ──────────────────────────────────────────────────────────────────────────────

def transform_incident_silver(raw_df: DataFrame) -> DataFrame:
    """
    Enrich each incident row with:
    - event_time, city
    - parsed timestamps (start_time_parsed, end_time_parsed)
    - incident_duration_minutes
    - delay_minutes, length_km
    - magnitude_label, is_road_closure, has_known_location, is_active
    """
    return (
        raw_df
        # ── Timestamps ──────────────────────────────────────────────────────
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("ingested_at") / 1000)),
        )
        .withColumn("city", city_expr("bbox_name"))
        # ── Parse ISO-string timestamps from TomTom ──────────────────────────
        .withColumn(
            "start_time_parsed",
            F.to_timestamp(F.col("start_time")),
        )
        .withColumn(
            "end_time_parsed",
            F.to_timestamp(F.col("end_time")),
        )
        # ── Derived: incident duration ────────────────────────────────────────
        .withColumn(
            "incident_duration_minutes",
            F.when(
                F.col("start_time_parsed").isNotNull()
                & F.col("end_time_parsed").isNotNull(),
                F.round(
                    (
                        F.unix_timestamp(F.col("end_time_parsed"))
                        - F.unix_timestamp(F.col("start_time_parsed"))
                    ) / 60.0,
                    2,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        # ── Derived: delay in minutes ─────────────────────────────────────────
        .withColumn(
            "delay_minutes",
            F.round(F.col("delay_seconds") / 60.0, 2),
        )
        # ── Derived: affected length in km ────────────────────────────────────
        .withColumn(
            "length_km",
            F.round(F.col("length_meters") / 1000.0, 3),
        )
        # ── Derived: magnitude label ──────────────────────────────────────────
        .withColumn("magnitude_label", magnitude_label_expr())
        # ── Derived: boolean flags ────────────────────────────────────────────
        .withColumn(
            "is_road_closure",
            F.lower(F.col("category")) == "road closed",
        )
        .withColumn(
            "has_known_location",
            F.col("lat").isNotNull() & F.col("lon").isNotNull(),
        )
        # is_active: ingested_at falls within [start_time, end_time]
        .withColumn(
            "is_active",
            F.when(
                F.col("start_time_parsed").isNotNull()
                & F.col("end_time_parsed").isNotNull(),
                (F.col("event_time") >= F.col("start_time_parsed"))
                & (F.col("event_time") <= F.col("end_time_parsed")),
            ).otherwise(F.lit(True)),  # if no times, assume still active
        )
        # ── Drop raw string timestamps (replaced by parsed versions) ─────────
        .drop("ingested_at", "start_time", "end_time")
    )


def incident_silver_df(enriched_df: DataFrame) -> DataFrame:
    """
    FIX (Priority 1 — incidents lag root cause):
    Tumbling 15-min window aggregation, deduplicating repeated polls of the
    same active incident. Without this, every raw Kafka poll (every ~10-30s)
    becomes its own row, writing hundreds of near-duplicate rows per minute
    to fact_incidents and starving the flow pipeline of processing time.

    Grain after this step: one row per incident identity per 15-min window
    (matches the documented fact_incidents grain in 06_medallion_architecture.md).
    """
    return (
        enriched_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "15 minutes"),
            F.col("bbox_name"),
            F.col("category"),
            F.col("from_location"),
            F.col("to_location"),
            F.col("start_time_parsed"),
        )
        .agg(
            F.first("city",                      ignorenulls=True).alias("city"),
            F.first("icon_category_id",          ignorenulls=True).alias("icon_category_id"),
            F.first("magnitude",                 ignorenulls=True).alias("magnitude"),
            F.first("magnitude_label",           ignorenulls=True).alias("magnitude_label"),
            F.first("delay_seconds",             ignorenulls=True).alias("delay_seconds"),
            F.first("delay_minutes",             ignorenulls=True).alias("delay_minutes"),
            F.first("length_meters",             ignorenulls=True).alias("length_meters"),
            F.first("length_km",                 ignorenulls=True).alias("length_km"),
            F.first("road_numbers",              ignorenulls=True).alias("road_numbers"),
            F.first("end_time_parsed",           ignorenulls=True).alias("end_time_parsed"),
            F.first("incident_duration_minutes", ignorenulls=True).alias("incident_duration_minutes"),
            F.first("is_road_closure",           ignorenulls=True).alias("is_road_closure"),
            F.first("has_known_location",        ignorenulls=True).alias("has_known_location"),
            F.first("is_active",                 ignorenulls=True).alias("is_active"),
            F.first("lat",                       ignorenulls=True).alias("lat"),
            F.first("lon",                       ignorenulls=True).alias("lon"),
        )
        .withColumn("event_time", F.col("window.start"))
        .drop("window")
    )


def write_silver_incidents(df: DataFrame):
    silver_path = f"s3a://{S3_BUCKET}/silver/incidents"
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_incidents")
        .partitionBy("city")
        .trigger(processingTime="1 minute")
        .start()
    )


# ──────────────────────────────────────────────────────────────────────────────
# GOLD — fact_traffic_flow
# Architecture: star schema in PostgreSQL.
# Key lookups (date_key, time_key, location_sk) done inside foreachBatch
# by querying dim tables via JDBC (batch reads are fine inside foreachBatch).
# ──────────────────────────────────────────────────────────────────────────────

def _lookup_dim_keys_flow(batch_df, spark: SparkSession) -> DataFrame:
    """
    Join silver_flow batch with dim_date and dim_time to resolve surrogate keys.
    dim_location lookup: match on location_name where is_current = TRUE.
    All dim tables are small — broadcast joins are safe.
    """
    dim_date = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_date")
        .options(**JDBC_PROPS)
        .load()
        .select("date_key", "full_date")
    )

    dim_time = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_time")
        .options(**JDBC_PROPS)
        .load()
        .select("time_key", "hour", "minute")
    )

    dim_location = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_location")
        .options(**JDBC_PROPS)
        .load()
        .filter(F.col("is_current") == True)
        .select("location_sk", "location_key")   # location_key = location_name natural key
    )

    return (
        batch_df
        # derive join keys from window_start
        .withColumn("_date",   F.to_date(F.col("window_start")))
        .withColumn("_hour",   F.hour(F.col("window_start")))
        .withColumn("_minute", F.minute(F.col("window_start")))
        # join dims
        .join(F.broadcast(dim_date),
              F.col("_date") == F.col("full_date"), "left")
        .join(F.broadcast(dim_time),
              (F.col("_hour") == dim_time["hour"])
              & (F.col("_minute") == dim_time["minute"]), "left")
        .join(F.broadcast(dim_location),
              F.col("location_name") == F.col("location_key"), "left")
        .drop("_date", "_hour", "_minute", "full_date",
              dim_time["hour"], dim_time["minute"], "location_key")
    )


def upsert_flow_gold(batch_df, batch_id: int, spark: SparkSession):
    print(f"[GOLD flow] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        return

    enriched = _lookup_dim_keys_flow(batch_df, spark)

    gold_cols_renamed = (
        enriched
        .withColumnRenamed("window_start", "window_start_utc")
        .withColumnRenamed("window_end",   "window_end_utc")
    )

    gold_cols = [
        # Surrogate / foreign keys
        "date_key", "time_key", "location_sk",
        # Window
        "window_start_utc", "window_end_utc",
        # Road class
        "frc_code", "frc_label",
        # Speed measures
        "avg_speed_kmh", "min_speed_kmh", "max_speed_kmh", "avg_free_flow_kmh",
        # Congestion measures
        "avg_congestion_ratio", "min_congestion_ratio", "max_congestion_ratio",
        "avg_congestion_severity_score",
        # Travel time measures
        "avg_travel_time_seconds", "avg_free_flow_travel_time_seconds",
        "avg_travel_time_delay_seconds", "avg_travel_time_delay_minutes",
        # Other derived
        "avg_speed_drop_pct",
        # Labels / flags
        "congestion_level", "road_closed_flag",
        "avg_confidence", "confidence_band",
        # Quality
        "sample_count",
    ]

    (
        gold_cols_renamed
        .select(*gold_cols)
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "fact_traffic_flow")
        .mode("append")
        .options(**JDBC_PROPS)
        .save()
    )


def write_flow_gold(df: DataFrame, spark: SparkSession):
    return (
        df.writeStream
        .foreachBatch(lambda batch_df, batch_id: upsert_flow_gold(batch_df, batch_id, spark))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_flow")
        .trigger(processingTime="1 minute")
        .start()
    )


# ──────────────────────────────────────────────────────────────────────────────
# GOLD — fact_incidents
# ──────────────────────────────────────────────────────────────────────────────

def _lookup_dim_keys_incidents(batch_df, spark: SparkSession) -> DataFrame:
    """
    Join silver_incidents batch with dim_date, dim_time, dim_location,
    dim_incident_category to resolve all surrogate keys.
    """
    dim_date = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_date")
        .options(**JDBC_PROPS)
        .load()
        .select("date_key", "full_date")
    )

    dim_time = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_time")
        .options(**JDBC_PROPS)
        .load()
        .select("time_key", "hour", "minute")
    )

    dim_location = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_location")
        .options(**JDBC_PROPS)
        .load()
        .filter(F.col("is_current") == True)
        .select("location_sk", "location_key")
    )

    dim_category = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "dim_incident_category")
        .options(**JDBC_PROPS)
        .load()
        .select("category_sk", "category_raw")
    )

    return (
        batch_df
        .withColumn("_date",   F.to_date(F.col("event_time")))
        .withColumn("_hour",   F.hour(F.col("event_time")))
        # dim_time only has slots at :00/:15/:30/:45 — floor to nearest slot
        .withColumn("_minute", (F.floor(F.minute(F.col("event_time")) / 15) * 15).cast("int"))
        .join(F.broadcast(dim_date),
              F.col("_date") == F.col("full_date"), "left")
        .join(F.broadcast(dim_time),
              (F.col("_hour")   == dim_time["hour"])
              & (F.col("_minute") == dim_time["minute"]), "left")
        # incidents join location on bbox_name (approximation — best effort)
        .join(F.broadcast(dim_location),
              F.col("bbox_name") == F.col("location_key"), "left")
        .join(F.broadcast(dim_category),
              F.col("category") == F.col("category_raw"), "left")
        .drop("_date", "_hour", "_minute", "full_date",
              dim_time["hour"], dim_time["minute"],
              "location_key", "category_raw")
    )


def upsert_incident_gold(batch_df, batch_id: int, spark: SparkSession):
    print(f"[GOLD incidents] batch {batch_id}: {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        return

    enriched = _lookup_dim_keys_incidents(batch_df, spark)

    renamed = (
        enriched
        .withColumnRenamed("event_time",        "window_start_utc")
        .withColumnRenamed("start_time_parsed", "incident_start_utc")
        .withColumnRenamed("end_time_parsed",   "incident_end_utc")
        .withColumnRenamed("is_active",         "is_active_at_ingestion")
    )

    gold_cols = [
        # Foreign keys
        "date_key", "time_key", "location_sk", "category_sk",
        # Window / timing
        "window_start_utc",
        "incident_start_utc", "incident_end_utc", "incident_duration_minutes",
        # Location detail
        "from_location", "to_location", "road_numbers",
        "lat", "lon", "has_known_location",
        # Incident attributes
        "magnitude", "magnitude_label",
        "delay_seconds", "delay_minutes",
        "length_meters", "length_km",
        # Flags
        "is_road_closure", "is_active_at_ingestion",
    ]

    (
        renamed
        .select(*gold_cols)
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "fact_incidents")
        .mode("append")
        .options(**JDBC_PROPS)
        .save()
    )


def write_incident_gold(df: DataFrame, spark: SparkSession):
    return (
        df.writeStream
        .foreachBatch(lambda batch_df, batch_id: upsert_incident_gold(batch_df, batch_id, spark))
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_incidents")
        .trigger(processingTime="1 minute")
        .start()
    )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()

    # ── FLOW PIPELINE ─────────────────────────────────────────────────────────

    # 1. Read raw Kafka messages
    flow_raw_kafka = read_kafka(spark, TOPIC_FLOW)
    flow_raw_df    = deserialize_avro(flow_raw_kafka, FLOW_AVRO_SCHEMA)

    # 2. Bronze — raw Parquet, append-only
    #    Must start BEFORE any transform that drops ingested_at
    q_bronze_flow = write_bronze(flow_raw_df, "flow")

    # 3. Silver — enrich per-reading, then tumbling-window aggregate
    flow_enriched  = transform_flow_silver(flow_raw_df)
    flow_silver    = flow_silver_df(flow_enriched)
    q_silver_flow  = write_silver_flow(flow_silver)

    # 4. Gold — star schema upsert into PostgreSQL (only if JDBC_URL is set)
    if JDBC_URL:
        q_gold_flow = write_flow_gold(flow_silver, spark)
    else:
        print("[GOLD] JDBC_URL not set — skipping fact_traffic_flow write")

    # ── INCIDENTS PIPELINE ────────────────────────────────────────────────────

    # 1. Read raw Kafka messages
    inc_raw_kafka = read_kafka(spark, TOPIC_INCIDENTS)
    inc_raw_df    = deserialize_avro(inc_raw_kafka, INCIDENT_AVRO_SCHEMA)

    # 2. Bronze — raw Parquet, append-only
    q_bronze_inc  = write_bronze(inc_raw_df, "incidents")

    # 3. Silver — per-incident rows with all derived fields
    inc_enriched  = transform_incident_silver(inc_raw_df)

    # FIX (Priority 1): collapse repeated polls of the same active incident
    # into one row per incident per 15-min window before writing downstream.
    inc_silver    = incident_silver_df(inc_enriched)

    q_silver_inc  = write_silver_incidents(inc_silver)

    # 4. Gold — star schema upsert into PostgreSQL
    if JDBC_URL:
        q_gold_inc = write_incident_gold(inc_silver, spark)
    else:
        print("[GOLD] JDBC_URL not set — skipping fact_incidents write")

    # ── Block until any stream terminates (or error) ──────────────────────────
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
