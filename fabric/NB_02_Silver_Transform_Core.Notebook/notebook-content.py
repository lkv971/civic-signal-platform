# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9acb4c25-6c85-4c86-8fe0-2f648d8f670d",
# META       "default_lakehouse_name": "LH_CivicSignal_Silver",
# META       "default_lakehouse_workspace_id": "33c8bc67-ef39-4ee1-9049-d38e9d32e75e",
# META       "known_lakehouses": [
# META         {
# META           "id": "9985f952-d718-475c-a462-a199fea986e3"
# META         },
# META         {
# META           "id": "9acb4c25-6c85-4c86-8fe0-2f648d8f670d"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# ============================================================
# PIPELINE PARAMETERS
# ============================================================

p_silver_run_id = ""
p_fail_on_dq_error = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CIVIC SIGNAL
# METADATA-DRIVEN SILVER TRANSFORMATION
#
# Silver principles:
#   - parse governed payloads with explicit schemas
#   - resolve current state across all append-only Bronze history
#   - retain first/last-seen and ingestion lineage
#   - publish only records that pass ERROR-level data quality
#   - preserve cross-source opportunity duplicates as candidates
# ============================================================

from datetime import datetime, timezone
from functools import reduce
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# 1. RUN CONTEXT AND TABLE CONFIGURATION
# ============================================================

SUPPORTED_SCHEMA_VERSION = "1.0"
BRONZE_RAW_TABLE = "LH_CivicSignal_Bronze.dbo.bronze_raw_records"
BRONZE_MANIFEST_TABLE = "LH_CivicSignal_Bronze.dbo.bronze_ingestion_manifest"

SILVER_SOURCES_TABLE = "dbo.silver_sources"
SILVER_BUYERS_TABLE = "dbo.silver_buyers"
SILVER_OPPORTUNITIES_TABLE = "dbo.silver_opportunities"
SILVER_DUPLICATES_TABLE = "dbo.silver_opportunity_duplicate_candidates"
SILVER_DQ_TABLE = "dbo.silver_data_quality_results"
SILVER_RUNS_TABLE = "dbo.silver_processing_runs"

run_started_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)

if p_silver_run_id is None or str(p_silver_run_id).strip() == "":
    silver_run_id = str(uuid.uuid4())
else:
    silver_run_id = str(p_silver_run_id).strip()

if isinstance(p_fail_on_dq_error, bool):
    fail_on_dq_error = p_fail_on_dq_error
else:
    fail_on_dq_error = str(p_fail_on_dq_error).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

print(f"Silver run ID: {silver_run_id}")
print(f"Fail on DQ error: {fail_on_dq_error}")


# ============================================================
# 2. GOVERNED BRONZE PAYLOAD CONTRACTS
#
# These schemas exactly match the payloads emitted by NB_01.
# Dates remain strings here so malformed values can be identified before
# conversion to governed Silver date columns.
# ============================================================

buyer_payload_schema = StructType(
    [
        StructField("source_record_id", StringType(), True),
        StructField("buyer_id", StringType(), True),
        StructField("buyer_name", StringType(), True),
        StructField("buyer_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("website", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("is_synthetic", BooleanType(), True),
    ]
)

opportunity_payload_schema = StructType(
    [
        StructField("source_record_id", StringType(), True),
        StructField("opportunity_id", StringType(), True),
        StructField("external_reference", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("buyer_id", StringType(), True),
        StructField("buyer_name", StringType(), True),
        StructField("category_raw", StringType(), True),
        StructField("status_raw", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("closing_date", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("estimated_value", DecimalType(18, 2), True),
        StructField("official_source_url", StringType(), True),
        StructField("duplicate_group_hint", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("is_synthetic", BooleanType(), True),
    ]
)

ENTITY_CONFIG = {
    "buyers": {
        "payload_schema": buyer_payload_schema,
        "business_id": "buyer_id",
    },
    "opportunities": {
        "payload_schema": opportunity_payload_schema,
        "business_id": "opportunity_id",
    },
}


# ============================================================
# 3. REUSABLE DELTA AND DATA-QUALITY HELPERS
# ============================================================

def merge_delta(source_df: DataFrame, table_name: str, merge_condition: str) -> None:
    """Create a Delta table or upsert its current-state rows."""

    if spark.catalog.tableExists(table_name):
        (
            DeltaTable.forName(spark, table_name)
            .alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def replace_run_rows(source_df: DataFrame, table_name: str) -> None:
    """Replace one run's append-style audit rows to make reruns idempotent."""

    if spark.catalog.tableExists(table_name):
        DeltaTable.forName(spark, table_name).delete(
            F.col("silver_run_id") == F.lit(silver_run_id)
        )
        source_df.write.format("delta").mode("append").saveAsTable(table_name)
    else:
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def is_blank(column: Column) -> Column:
    return column.isNull() | (F.trim(column.cast("string")) == F.lit(""))


def dq_failure_rows(
    source_df: DataFrame,
    condition: Column,
    business_key: Column,
    rule_id: str,
    severity: str,
    rule_message: str,
) -> DataFrame:
    """Project failed records into the governed DQ result contract."""

    return (
        source_df.filter(condition)
        .select(
            F.lit(silver_run_id).alias("silver_run_id"),
            F.lit(run_started_at_utc).cast("timestamp").alias("checked_at_utc"),
            F.col("entity_type"),
            F.col("source_system"),
            F.col("source_record_id"),
            business_key.cast("string").alias("business_key"),
            F.lit(rule_id).alias("rule_id"),
            F.lit(severity).alias("severity"),
            F.lit(rule_message).alias("rule_message"),
        )
    )


def canonical_record_hash(column_names: list[str]) -> Column:
    canonical_struct = F.struct(
        *[F.col(column_name).alias(column_name) for column_name in column_names]
    )
    return F.sha2(F.to_json(canonical_struct), 256)


def union_all(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), dataframes)


# ============================================================
# 4. READ BRONZE EXPLICITLY AND RESOLVE CURRENT STATE
#
# Bronze is append-only. first_seen/last_seen are calculated across all
# history before the latest envelope is selected for each source record.
# ============================================================

bronze_raw_df = spark.read.table(BRONZE_RAW_TABLE)
bronze_manifest_df = spark.read.table(BRONZE_MANIFEST_TABLE)

history_window = Window.partitionBy(
    "entity_type",
    "source_system",
    "source_record_id",
)

latest_window = history_window.orderBy(
    F.col("_ingested_at_utc").desc(),
    F.col("_ingestion_batch_id").desc(),
)

latest_raw_df = (
    bronze_raw_df.withColumn(
        "first_seen_at_utc",
        F.min("_ingested_at_utc").over(history_window),
    )
    .withColumn(
        "last_seen_at_utc",
        F.max("_ingested_at_utc").over(history_window),
    )
    .withColumn("_latest_row_number", F.row_number().over(latest_window))
    .filter(F.col("_latest_row_number") == 1)
    .drop("_latest_row_number")
)

parsed_entities = {
    entity_type: (
        latest_raw_df.filter(F.col("entity_type") == entity_type).withColumn(
            "payload",
            F.from_json(
                F.col("raw_payload_json"),
                config["payload_schema"],
                {"mode": "PERMISSIVE"},
            ),
        )
    )
    for entity_type, config in ENTITY_CONFIG.items()
}

buyer_raw_df = parsed_entities["buyers"]
opportunity_raw_df = parsed_entities["opportunities"]


# ============================================================
# 5. BUILD SILVER SOURCES FROM THE BRONZE MANIFEST
# ============================================================

raw_source_flags_df = bronze_raw_df.groupBy("source_system").agg(
    F.min(
        F.coalesce(F.col("_is_synthetic"), F.lit(False)).cast("integer")
    )
    .cast("boolean")
    .alias("is_synthetic")
)

silver_sources_df = (
    bronze_manifest_df.groupBy("source_system")
    .agg(
        F.max("source_display_name").alias("source_display_name"),
        F.max("source_type").alias("source_type"),
        F.min("enabled_flag").cast("integer").alias("enabled_flag"),
        F.array_join(
            F.sort_array(F.collect_set("entity_type")),
            ",",
        ).alias("supported_entity_types"),
        F.array_join(
            F.sort_array(F.collect_set("schema_version")),
            ",",
        ).alias("schema_versions"),
    )
    .join(raw_source_flags_df, on="source_system", how="left")
    .withColumn("is_synthetic", F.coalesce("is_synthetic", F.lit(False)))
    .withColumn("silver_run_id", F.lit(silver_run_id))
    .withColumn("processed_at_utc", F.lit(run_started_at_utc).cast("timestamp"))
    .select(
        "source_system",
        "source_display_name",
        "source_type",
        "enabled_flag",
        "supported_entity_types",
        "schema_versions",
        "is_synthetic",
        "silver_run_id",
        "processed_at_utc",
    )
)


# ============================================================
# 6. BUYER DATA QUALITY AND GOVERNED BUYER ROWS
# ============================================================

buyer_id_col = F.col("payload.buyer_id")

buyer_dq_rules = [
    (
        is_blank(buyer_id_col),
        "DQ001",
        "ERROR",
        "Required buyer business ID is missing.",
    ),
    (
        ~F.col("payload.source_system").eqNullSafe(F.col("source_system")),
        "DQ002",
        "ERROR",
        "Envelope source_system does not match payload source_system.",
    ),
    (
        ~F.col("payload.source_record_id").eqNullSafe(F.col("source_record_id")),
        "DQ003",
        "ERROR",
        "Envelope source_record_id does not match payload source_record_id.",
    ),
    (
        ~F.col("schema_version").eqNullSafe(F.lit(SUPPORTED_SCHEMA_VERSION)),
        "DQ004",
        "ERROR",
        f"Unsupported schema_version; expected {SUPPORTED_SCHEMA_VERSION}.",
    ),
    (
        ~F.coalesce(F.col("_is_synthetic"), F.lit(False)),
        "DQ008",
        "ERROR",
        "Envelope _is_synthetic is not true.",
    ),
    (
        ~F.coalesce(F.col("payload.is_synthetic"), F.lit(False)),
        "DQ009",
        "ERROR",
        "Payload is_synthetic is not true.",
    ),
    (
        ~F.coalesce(F.col("_source_environment"), F.lit("")).eqNullSafe(
            F.lit("synthetic")
        ),
        "DQ010",
        "ERROR",
        "Envelope _source_environment is not synthetic.",
    ),
]

buyer_dq_df = union_all(
    [
        dq_failure_rows(
            buyer_raw_df,
            condition,
            buyer_id_col,
            rule_id,
            severity,
            message,
        )
        for condition, rule_id, severity, message in buyer_dq_rules
    ]
)

invalid_buyer_records_df = (
    buyer_dq_df.filter(F.col("severity") == "ERROR")
    .select("source_system", "source_record_id")
    .distinct()
)

valid_buyer_raw_df = buyer_raw_df.join(
    invalid_buyer_records_df,
    on=["source_system", "source_record_id"],
    how="left_anti",
)

silver_buyers_df = (
    valid_buyer_raw_df.select(
        F.sha2(
            F.concat_ws("||", F.col("source_system"), F.col("payload.buyer_id")),
            256,
        ).alias("buyer_key"),
        F.col("payload.buyer_id").alias("buyer_id"),
        F.col("payload.buyer_name").alias("buyer_name"),
        F.col("payload.buyer_type").alias("buyer_type"),
        F.col("payload.country").alias("country"),
        F.col("payload.country_code").alias("country_code"),
        F.col("payload.region").alias("region"),
        F.col("payload.website").alias("website"),
        F.col("source_system"),
        F.col("source_record_id"),
        F.col("schema_version"),
        F.col("_source_environment").alias("source_environment"),
        F.col("_is_synthetic").alias("is_synthetic"),
        F.col("_ingestion_batch_id").alias("ingestion_batch_id"),
        F.col("first_seen_at_utc"),
        F.col("last_seen_at_utc"),
    )
    .withColumn(
        "record_hash",
        canonical_record_hash(
            [
                "buyer_id",
                "buyer_name",
                "buyer_type",
                "country",
                "country_code",
                "region",
                "website",
                "source_system",
            ]
        ),
    )
    .withColumn("silver_run_id", F.lit(silver_run_id))
    .withColumn("processed_at_utc", F.lit(run_started_at_utc).cast("timestamp"))
)


# ============================================================
# 7. OPPORTUNITY DATA QUALITY
# ============================================================

published_date_text = F.trim(F.col("payload.published_date"))
closing_date_text = F.trim(F.col("payload.closing_date"))
parsed_published_date = F.expr(
    "try_to_timestamp(trim(payload.published_date), 'yyyy-MM-dd')"
).cast("date")
parsed_closing_date = F.expr(
    "try_to_timestamp(trim(payload.closing_date), 'yyyy-MM-dd')"
).cast("date")

invalid_published_date = (
    is_blank(F.col("payload.published_date"))
    | parsed_published_date.isNull()
    | (F.date_format(parsed_published_date, "yyyy-MM-dd") != published_date_text)
)

invalid_closing_date = (
    is_blank(F.col("payload.closing_date"))
    | parsed_closing_date.isNull()
    | (F.date_format(parsed_closing_date, "yyyy-MM-dd") != closing_date_text)
)

valid_buyer_references_df = valid_buyer_raw_df.select(
    F.col("source_system").alias("buyer_source_system"),
    F.col("payload.buyer_id").alias("referenced_buyer_id"),
).distinct()

opportunity_checked_raw_df = (
    opportunity_raw_df.alias("opportunity")
    .join(
        valid_buyer_references_df.alias("buyer"),
        (
            F.col("opportunity.source_system")
            == F.col("buyer.buyer_source_system")
        )
        & (
            F.col("opportunity.payload.buyer_id")
            == F.col("buyer.referenced_buyer_id")
        ),
        "left",
    )
    .select(
        "opportunity.*",
        F.col("buyer.referenced_buyer_id")
        .isNotNull()
        .alias("_buyer_reference_exists"),
    )
)

opportunity_id_col = F.col("payload.opportunity_id")

opportunity_dq_rules = [
    (
        is_blank(opportunity_id_col),
        "DQ001",
        "ERROR",
        "Required opportunity business ID is missing.",
    ),
    (
        ~F.col("payload.source_system").eqNullSafe(F.col("source_system")),
        "DQ002",
        "ERROR",
        "Envelope source_system does not match payload source_system.",
    ),
    (
        ~F.col("payload.source_record_id").eqNullSafe(F.col("source_record_id")),
        "DQ003",
        "ERROR",
        "Envelope source_record_id does not match payload source_record_id.",
    ),
    (
        ~F.col("schema_version").eqNullSafe(F.lit(SUPPORTED_SCHEMA_VERSION)),
        "DQ004",
        "ERROR",
        f"Unsupported schema_version; expected {SUPPORTED_SCHEMA_VERSION}.",
    ),
    (
        parsed_closing_date < parsed_published_date,
        "DQ005",
        "ERROR",
        "Opportunity closing_date is earlier than published_date.",
    ),
    (
        F.col("payload.estimated_value").isNotNull()
        & (F.col("payload.estimated_value") < F.lit(0)),
        "DQ006",
        "ERROR",
        "Opportunity estimated_value is negative.",
    ),
    (
        ~F.col("_buyer_reference_exists"),
        "DQ007",
        "ERROR",
        "Opportunity buyer reference does not exist in the valid current buyer set.",
    ),
    (
        ~F.coalesce(F.col("_is_synthetic"), F.lit(False)),
        "DQ008",
        "ERROR",
        "Envelope _is_synthetic is not true.",
    ),
    (
        ~F.coalesce(F.col("payload.is_synthetic"), F.lit(False)),
        "DQ009",
        "ERROR",
        "Payload is_synthetic is not true.",
    ),
    (
        ~F.coalesce(F.col("_source_environment"), F.lit("")).eqNullSafe(
            F.lit("synthetic")
        ),
        "DQ010",
        "ERROR",
        "Envelope _source_environment is not synthetic.",
    ),
    (
        invalid_published_date,
        "DQ011",
        "ERROR",
        "Required opportunity published_date is missing or is not a valid yyyy-MM-dd date.",
    ),
    (
        invalid_closing_date,
        "DQ012",
        "ERROR",
        "Required opportunity closing_date is missing or is not a valid yyyy-MM-dd date.",
    ),
    (
        F.col("payload.category_raw").isNull(),
        "DQ101",
        "WARNING",
        "Opportunity category_raw is missing.",
    ),
]

opportunity_dq_df = union_all(
    [
        dq_failure_rows(
            opportunity_checked_raw_df,
            condition,
            opportunity_id_col,
            rule_id,
            severity,
            message,
        )
        for condition, rule_id, severity, message in opportunity_dq_rules
    ]
)

all_dq_df = buyer_dq_df.unionByName(opportunity_dq_df)

invalid_opportunity_records_df = (
    opportunity_dq_df.filter(F.col("severity") == "ERROR")
    .select("source_system", "source_record_id")
    .distinct()
)

valid_opportunity_raw_df = opportunity_checked_raw_df.join(
    invalid_opportunity_records_df,
    on=["source_system", "source_record_id"],
    how="left_anti",
)


# ============================================================
# 8. GOVERNED OPPORTUNITY ROWS AND DUPLICATE CANDIDATES
# ============================================================

category_map = F.create_map(
    F.lit("Digital services"),
    F.lit("Digital Services"),
    F.lit("Construction and works"),
    F.lit("Construction & Works"),
    F.lit("Professional services"),
    F.lit("Professional Services"),
    F.lit("Facilities and maintenance"),
    F.lit("Facilities & Maintenance"),
    F.lit("Goods and supplies"),
    F.lit("Goods & Supplies"),
    F.lit("Healthcare supplies"),
    F.lit("Healthcare Supplies"),
    F.lit("Transport and logistics"),
    F.lit("Transport & Logistics"),
    F.lit("Energy and utilities"),
    F.lit("Energy & Utilities"),
)

status_map = F.create_map(
    F.lit("Open"),
    F.lit("Open"),
    F.lit("Closed"),
    F.lit("Closed"),
    F.lit("Awarded"),
    F.lit("Awarded"),
    F.lit("Cancelled"),
    F.lit("Cancelled"),
    F.lit("Under evaluation"),
    F.lit("Under Evaluation"),
)

opportunity_business_base_df = (
    valid_opportunity_raw_df.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col("source_system"),
                F.col("payload.opportunity_id"),
            ),
            256,
        ).alias("opportunity_key"),
        F.col("payload.opportunity_id").alias("opportunity_id"),
        F.col("payload.external_reference").alias("external_reference"),
        F.col("payload.title").alias("title"),
        F.col("payload.description").alias("description"),
        F.sha2(
            F.concat_ws("||", F.col("source_system"), F.col("payload.buyer_id")),
            256,
        ).alias("buyer_key"),
        F.col("payload.buyer_id").alias("buyer_id"),
        F.col("payload.buyer_name").alias("buyer_name"),
        F.col("payload.category_raw").alias("category_raw"),
        F.element_at(category_map, F.col("payload.category_raw")).alias(
            "category_normalized"
        ),
        F.when(F.col("payload.category_raw").isNull(), F.lit("MISSING"))
        .when(
            F.element_at(category_map, F.col("payload.category_raw")).isNotNull(),
            F.lit("CLASSIFIED"),
        )
        .otherwise(F.lit("UNCLASSIFIED"))
        .alias("category_quality_status"),
        F.col("payload.status_raw").alias("status_raw"),
        F.element_at(status_map, F.col("payload.status_raw")).alias("status"),
        F.col("payload.country").alias("country"),
        F.col("payload.country_code").alias("country_code"),
        F.col("payload.region").alias("region"),
        parsed_published_date.alias("published_date"),
        parsed_closing_date.alias("closing_date"),
        F.datediff(parsed_closing_date, parsed_published_date)
        .cast("integer")
        .alias("days_to_close"),
        F.col("payload.currency_code").alias("currency_code"),
        F.col("payload.estimated_value")
        .cast(DecimalType(18, 2))
        .alias("estimated_value"),
        F.col("payload.official_source_url").alias("official_source_url"),
        F.col("payload.duplicate_group_hint").alias("duplicate_group_hint"),
        F.col("source_system"),
        F.col("source_record_id"),
        F.col("schema_version"),
        F.col("_source_environment").alias("source_environment"),
        F.col("_is_synthetic").alias("is_synthetic"),
        F.col("_ingestion_batch_id").alias("ingestion_batch_id"),
        F.col("first_seen_at_utc"),
        F.col("last_seen_at_utc"),
    )
    .withColumn(
        "record_hash",
        canonical_record_hash(
            [
                "opportunity_id",
                "external_reference",
                "title",
                "description",
                "buyer_id",
                "buyer_name",
                "category_raw",
                "category_normalized",
                "category_quality_status",
                "status_raw",
                "status",
                "country",
                "country_code",
                "region",
                "published_date",
                "closing_date",
                "currency_code",
                "estimated_value",
                "official_source_url",
                "duplicate_group_hint",
                "source_system",
            ]
        ),
    )
)

duplicate_groups_df = (
    opportunity_business_base_df.filter(F.col("duplicate_group_hint").isNotNull())
    .groupBy("duplicate_group_hint")
    .agg(F.countDistinct("source_system").alias("distinct_source_count"))
    .filter(F.col("distinct_source_count") > 1)
    .select("duplicate_group_hint")
)

silver_opportunities_df = (
    opportunity_business_base_df.join(
        duplicate_groups_df.withColumn("potential_duplicate_flag", F.lit(True)),
        on="duplicate_group_hint",
        how="left",
    )
    .withColumn(
        "potential_duplicate_flag",
        F.coalesce(F.col("potential_duplicate_flag"), F.lit(False)),
    )
    .withColumn("silver_run_id", F.lit(silver_run_id))
    .withColumn("processed_at_utc", F.lit(run_started_at_utc).cast("timestamp"))
    .select(
        "opportunity_key",
        "opportunity_id",
        "external_reference",
        "title",
        "description",
        "buyer_key",
        "buyer_id",
        "buyer_name",
        "category_raw",
        "category_normalized",
        "category_quality_status",
        "status_raw",
        "status",
        "country",
        "country_code",
        "region",
        "published_date",
        "closing_date",
        "days_to_close",
        "currency_code",
        "estimated_value",
        "official_source_url",
        "duplicate_group_hint",
        "potential_duplicate_flag",
        "source_system",
        "source_record_id",
        "schema_version",
        "source_environment",
        "is_synthetic",
        "ingestion_batch_id",
        "first_seen_at_utc",
        "last_seen_at_utc",
        "record_hash",
        "silver_run_id",
        "processed_at_utc",
    )
)

duplicate_candidates_df = silver_opportunities_df.filter(
    F.col("potential_duplicate_flag")
).select(
    "duplicate_group_hint",
    "opportunity_key",
    "source_system",
    "opportunity_id",
    "title",
    "buyer_name",
    "published_date",
    "potential_duplicate_flag",
    "silver_run_id",
    "processed_at_utc",
)


# ============================================================
# 9. RUN METRICS, DQ PERSISTENCE, AND PUBLICATION GATE
# ============================================================

bronze_input_rows = bronze_raw_df.count()
latest_buyer_rows = buyer_raw_df.count()
latest_opportunity_rows = opportunity_raw_df.count()
output_source_rows = silver_sources_df.count()
output_buyer_rows = silver_buyers_df.count()
output_opportunity_rows = silver_opportunities_df.count()
duplicate_candidate_rows = duplicate_candidates_df.count()
duplicate_group_count = duplicate_groups_df.count()

dq_counts = (
    all_dq_df.agg(
        F.sum(F.when(F.col("severity") == "ERROR", 1).otherwise(0)).alias(
            "dq_error_count"
        ),
        F.sum(F.when(F.col("severity") == "WARNING", 1).otherwise(0)).alias(
            "dq_warning_count"
        ),
    )
    .first()
)

dq_error_count = int(dq_counts["dq_error_count"] or 0)
dq_warning_count = int(dq_counts["dq_warning_count"] or 0)

processing_run_schema = StructType(
    [
        StructField("silver_run_id", StringType(), False),
        StructField("started_at_utc", TimestampType(), False),
        StructField("completed_at_utc", TimestampType(), False),
        StructField("bronze_input_rows", LongType(), False),
        StructField("latest_buyer_rows", LongType(), False),
        StructField("latest_opportunity_rows", LongType(), False),
        StructField("output_source_rows", LongType(), False),
        StructField("output_buyer_rows", LongType(), False),
        StructField("output_opportunity_rows", LongType(), False),
        StructField("duplicate_candidate_rows", LongType(), False),
        StructField("duplicate_group_count", LongType(), False),
        StructField("dq_error_count", LongType(), False),
        StructField("dq_warning_count", LongType(), False),
        StructField("status", StringType(), False),
    ]
)


def processing_run_df(status: str, published: bool) -> DataFrame:
    completed_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    return spark.createDataFrame(
        [
            {
                "silver_run_id": silver_run_id,
                "started_at_utc": run_started_at_utc,
                "completed_at_utc": completed_at_utc,
                "bronze_input_rows": bronze_input_rows,
                "latest_buyer_rows": latest_buyer_rows,
                "latest_opportunity_rows": latest_opportunity_rows,
                "output_source_rows": output_source_rows if published else 0,
                "output_buyer_rows": output_buyer_rows if published else 0,
                "output_opportunity_rows": output_opportunity_rows if published else 0,
                "duplicate_candidate_rows": (
                    duplicate_candidate_rows if published else 0
                ),
                "duplicate_group_count": duplicate_group_count if published else 0,
                "dq_error_count": dq_error_count,
                "dq_warning_count": dq_warning_count,
                "status": status,
            }
        ],
        schema=processing_run_schema,
    )


# DQ is persisted before any business table is published.
replace_run_rows(all_dq_df, SILVER_DQ_TABLE)

if fail_on_dq_error and dq_error_count > 0:
    merge_delta(
        processing_run_df("FailedDQ", published=False),
        SILVER_RUNS_TABLE,
        "target.silver_run_id = source.silver_run_id",
    )
    raise RuntimeError(
        f"Silver run {silver_run_id} failed with {dq_error_count} ERROR DQ result(s)."
    )


# ============================================================
# 10. DELTA PUBLICATION
#
# ERROR-level records are quarantined from business outputs. Existing
# Silver rows are never deleted merely because they are absent from a batch.
# ============================================================

merge_delta(
    silver_sources_df,
    SILVER_SOURCES_TABLE,
    "target.source_system = source.source_system",
)

merge_delta(
    silver_buyers_df,
    SILVER_BUYERS_TABLE,
    "target.source_system = source.source_system AND target.buyer_id = source.buyer_id",
)

merge_delta(
    silver_opportunities_df,
    SILVER_OPPORTUNITIES_TABLE,
    (
        "target.source_system = source.source_system "
        "AND target.opportunity_id = source.opportunity_id"
    ),
)

duplicate_candidates_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_DUPLICATES_TABLE)

merge_delta(
    processing_run_df("Succeeded", published=True),
    SILVER_RUNS_TABLE,
    "target.silver_run_id = source.silver_run_id",
)

print("==========================================")
print("CIVIC SIGNAL SILVER TRANSFORMATION COMPLETE")
print("==========================================")
print(f"Silver run ID: {silver_run_id}")
print("Status: Succeeded")
print(f"DQ errors: {dq_error_count}")
print(f"DQ warnings: {dq_warning_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 11. FINAL SILVER VALIDATION REPORT
#
# Synthetic baseline expectations (validation only):
#   sources = 3
#   buyers = 18
#   opportunities = 180
#   duplicate candidate rows = 6
#   duplicate groups = 2
# ============================================================

validation_sources_df = spark.table(SILVER_SOURCES_TABLE).agg(
    F.count(F.lit(1)).alias("source_row_count")
)

validation_buyers = spark.table(SILVER_BUYERS_TABLE)
validation_buyers_df = validation_buyers.agg(
    F.count(F.lit(1)).alias("buyer_row_count")
)

validation_opportunities = spark.table(SILVER_OPPORTUNITIES_TABLE)
validation_opportunities_df = validation_opportunities.agg(
    F.count(F.lit(1)).alias("opportunity_row_count")
)

validation_candidates = spark.table(SILVER_DUPLICATES_TABLE).filter(
    F.col("silver_run_id") == silver_run_id
)

validation_candidates_df = validation_candidates.agg(
    F.count(F.lit(1)).alias("duplicate_candidate_row_count"),
    F.countDistinct("duplicate_group_hint").alias("duplicate_group_count"),
)

duplicate_buyer_keys_df = (
    validation_buyers.groupBy("source_system", "buyer_id")
    .count()
    .filter(F.col("count") > 1)
    .agg(F.count(F.lit(1)).alias("duplicate_buyer_business_keys"))
)

duplicate_opportunity_keys_df = (
    validation_opportunities.groupBy("source_system", "opportunity_id")
    .count()
    .filter(F.col("count") > 1)
    .agg(F.count(F.lit(1)).alias("duplicate_opportunity_business_keys"))
)

orphan_opportunity_buyers_df = (
    validation_opportunities.select("buyer_key")
    .distinct()
    .join(
        validation_buyers.select("buyer_key").distinct(),
        on="buyer_key",
        how="left_anti",
    )
    .agg(F.count(F.lit(1)).alias("orphan_opportunity_buyer_references"))
)

validation_dq_df = (
    spark.table(SILVER_DQ_TABLE)
    .filter(F.col("silver_run_id") == silver_run_id)
    .agg(
        F.sum(F.when(F.col("severity") == "ERROR", 1).otherwise(0)).alias(
            "dq_error_count"
        ),
        F.sum(F.when(F.col("severity") == "WARNING", 1).otherwise(0)).alias(
            "dq_warning_count"
        ),
    )
    .select(
        F.coalesce(F.col("dq_error_count"), F.lit(0)).alias("dq_error_count"),
        F.coalesce(F.col("dq_warning_count"), F.lit(0)).alias(
            "dq_warning_count"
        ),
    )
)

validation_run_df = (
    spark.table(SILVER_RUNS_TABLE)
    .filter(F.col("silver_run_id") == silver_run_id)
    .select(F.col("status").alias("silver_run_status"))
)

validation_report_df = (
    validation_sources_df.crossJoin(validation_buyers_df)
    .crossJoin(validation_opportunities_df)
    .crossJoin(validation_candidates_df)
    .crossJoin(duplicate_buyer_keys_df)
    .crossJoin(duplicate_opportunity_keys_df)
    .crossJoin(orphan_opportunity_buyers_df)
    .crossJoin(validation_dq_df)
    .crossJoin(validation_run_df)
)

display(validation_report_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
