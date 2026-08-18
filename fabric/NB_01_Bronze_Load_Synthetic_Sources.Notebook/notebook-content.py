# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9985f952-d718-475c-a462-a199fea986e3",
# META       "default_lakehouse_name": "LH_CivicSignal_Bronze",
# META       "default_lakehouse_workspace_id": "33c8bc67-ef39-4ee1-9049-d38e9d32e75e",
# META       "known_lakehouses": [
# META         {
# META           "id": "9985f952-d718-475c-a462-a199fea986e3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# PIPELINE PARAMETERS
#
# Mark this cell as the Fabric notebook parameter cell.
# A future Data Factory pipeline will override
# p_ingestion_batch_id with the pipeline RunId.
# ============================================================

p_ingestion_batch_id = ""
p_source_environment = "synthetic"
p_seed = 42

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CIVIC SIGNAL
# METADATA-DRIVEN SYNTHETIC BRONZE INGESTION
#
# Public portfolio implementation only.
# All organizations, opportunities and URLs generated here
# are synthetic.
#
# Bronze principles:
#   - preserve raw source-shaped payloads
#   - retain ingestion metadata
#   - append by ingestion batch
#   - support multiple sources through one manifest
#   - do not apply Silver business rules here
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType
)

from datetime import datetime, timedelta, timezone

import random
import uuid
import json


# ============================================================
# 1. RUN CONTEXT
# ============================================================

run_started_at_utc = (
    datetime.now(timezone.utc)
    .replace(tzinfo=None)
)


if (
    p_ingestion_batch_id is None
    or str(p_ingestion_batch_id).strip() == ""
):
    ingestion_batch_id = str(uuid.uuid4())
else:
    ingestion_batch_id = str(
        p_ingestion_batch_id
    ).strip()


source_environment = (
    str(p_source_environment).strip()
    if p_source_environment is not None
    else "synthetic"
)


seed_value = int(p_seed)

random.seed(seed_value)


print(
    f"Ingestion batch ID: {ingestion_batch_id}"
)

print(
    f"Source environment: {source_environment}"
)

print(
    f"Synthetic seed: {seed_value}"
)


# ============================================================
# 2. SOURCE MANIFEST
#
# These are fictional source systems.
# example.org is intentionally used for synthetic URLs.
# ============================================================

sources = [
    {
        "source_system":
            "national_civic_portal",

        "source_display_name":
            "National Civic Procurement Portal",

        "source_base_url":
            "https://national-civic.example.org",

        "source_type":
            "national_portal"
    },
    {
        "source_system":
            "metro_procurement_exchange",

        "source_display_name":
            "Metro Procurement Exchange",

        "source_base_url":
            "https://metro-procurement.example.org",

        "source_type":
            "municipal_portal"
    },
    {
        "source_system":
            "regional_infrastructure_hub",

        "source_display_name":
            "Regional Infrastructure Tender Hub",

        "source_base_url":
            "https://regional-infrastructure.example.org",

        "source_type":
            "development_portal"
    }
]


manifest_rows = []

for source in sources:

    for entity_type, records_per_run in [
        ("buyers", 6),
        ("opportunities", 60)
    ]:

        manifest_rows.append(
            {
                "source_system":
                    source["source_system"],

                "source_display_name":
                    source["source_display_name"],

                "source_type":
                    source["source_type"],

                "entity_type":
                    entity_type,

                "enabled_flag":
                    1,

                "schema_version":
                    "1.0",

                "records_per_run":
                    records_per_run,

                "raw_path_pattern":
                    (
                        "Files/raw/"
                        f"{source['source_system']}/"
                        f"{entity_type}/"
                        "batch_id={batch_id}"
                    ),

                "updated_at_utc":
                    run_started_at_utc
            }
        )


manifest_schema = StructType(
    [
        StructField(
            "source_system",
            StringType(),
            False
        ),

        StructField(
            "source_display_name",
            StringType(),
            False
        ),

        StructField(
            "source_type",
            StringType(),
            False
        ),

        StructField(
            "entity_type",
            StringType(),
            False
        ),

        StructField(
            "enabled_flag",
            IntegerType(),
            False
        ),

        StructField(
            "schema_version",
            StringType(),
            False
        ),

        StructField(
            "records_per_run",
            IntegerType(),
            False
        ),

        StructField(
            "raw_path_pattern",
            StringType(),
            False
        ),

        StructField(
            "updated_at_utc",
            TimestampType(),
            False
        )
    ]
)


manifest_df = spark.createDataFrame(
    manifest_rows,
    schema=manifest_schema
)


# ============================================================
# 3. SYNTHETIC DOMAIN CONFIGURATION
# ============================================================

buyer_names = [
    "Synthetic Digital Services Office",
    "Synthetic Metro Infrastructure Office",
    "Synthetic Public Health Agency",
    "Synthetic Education Services Authority",
    "Synthetic Facilities Management Office",
    "Synthetic Regional Transport Agency"
]


buyer_types = [
    "Government department",
    "Municipal entity",
    "Public agency",
    "Development authority"
]


regions = [
    "Central",
    "Coastal",
    "Northern",
    "Southern"
]


categories = [
    "Digital services",
    "Construction and works",
    "Professional services",
    "Facilities and maintenance",
    "Goods and supplies",
    "Healthcare supplies",
    "Transport and logistics",
    "Energy and utilities"
]


statuses = [
    "Open",
    "Closed",
    "Awarded",
    "Cancelled",
    "Under evaluation"
]


status_weights = [
    0.50,
    0.18,
    0.12,
    0.05,
    0.15
]


anchor_date = datetime(
    2026,
    8,
    1
)


# ============================================================
# 4. BUILD SOURCE-SHAPED PAYLOADS
# ============================================================

payloads_by_source_entity = {}

raw_record_rows = []


def register_raw_record(
    entity_type,
    source_system,
    source_record_id,
    payload
):

    raw_record_rows.append(
        {
            "entity_type":
                entity_type,

            "source_system":
                source_system,

            "source_record_id":
                source_record_id,

            "schema_version":
                "1.0",

            "raw_payload_json":
                json.dumps(
                    payload,
                    sort_keys=True,
                    default=str
                ),

            "_ingestion_batch_id":
                ingestion_batch_id,

            "_ingested_at_utc":
                run_started_at_utc,

            "_source_environment":
                source_environment,

            "_is_synthetic":
                True
        }
    )


for source_index, source in enumerate(
    sources,
    start=1
):

    source_system = (
        source["source_system"]
    )

    source_base_url = (
        source["source_base_url"]
    )


    # --------------------------------------------------------
    # BUYERS
    # --------------------------------------------------------

    source_buyers = []

    buyer_payloads = []


    for buyer_number in range(
        1,
        7
    ):

        buyer_id = (
            f"{source_system}"
            f"-buyer-"
            f"{buyer_number:03d}"
        )


        buyer_payload = {
            "source_record_id":
                buyer_id,

            "buyer_id":
                buyer_id,

            "buyer_name":
                (
                    buyer_names[
                        buyer_number - 1
                    ]
                    + f" {source_index}"
                ),

            "buyer_type":
                random.choice(
                    buyer_types
                ),

            "country":
                "Example Republic",

            "country_code":
                "EX",

            "region":
                random.choice(
                    regions
                ),

            "website":
                (
                    f"{source_base_url}/"
                    f"buyers/{buyer_id}"
                ),

            "source_system":
                source_system,

            "is_synthetic":
                True
        }


        source_buyers.append(
            buyer_payload
        )

        buyer_payloads.append(
            buyer_payload
        )


        register_raw_record(
            entity_type="buyers",
            source_system=source_system,
            source_record_id=buyer_id,
            payload=buyer_payload
        )


    payloads_by_source_entity[
        (
            source_system,
            "buyers"
        )
    ] = buyer_payloads


    # --------------------------------------------------------
    # OPPORTUNITIES
    # --------------------------------------------------------

    opportunity_payloads = []


    for opportunity_number in range(
        1,
        61
    ):

        opportunity_id = (
            f"{source_system}"
            f"-opp-"
            f"{opportunity_number:04d}"
        )


        buyer = random.choice(
            source_buyers
        )


        published_date = (
            anchor_date
            + timedelta(
                days=random.randint(
                    0,
                    120
                )
            )
        )


        closing_date = (
            published_date
            + timedelta(
                days=random.randint(
                    7,
                    45
                )
            )
        )


        category_raw = (
            None
            if random.random() < 0.07
            else random.choice(
                categories
            )
        )


        status_raw = random.choices(
            statuses,
            weights=status_weights,
            k=1
        )[0]


        duplicate_group_hint = (
            f"cross-source-{opportunity_number:04d}"
            if opportunity_number % 25 == 0
            else None
        )


        estimated_value = (
            None
            if random.random() < 0.18
            else round(
                random.uniform(
                    25000,
                    2500000
                ),
                2
            )
        )


        opportunity_payload = {
            "source_record_id":
                opportunity_id,

            "opportunity_id":
                opportunity_id,

            "external_reference":
                (
                    f"CSP-"
                    f"{source_index}-"
                    f"{opportunity_number:04d}"
                ),

            "title":
                (
                    f"{random.choice(categories)} "
                    f"procurement requirement "
                    f"{opportunity_number}"
                ),

            "description":
                (
                    "Synthetic procurement opportunity "
                    "created for the Civic Signal "
                    "public reference implementation."
                ),

            "buyer_id":
                buyer["buyer_id"],

            "buyer_name":
                buyer["buyer_name"],

            "category_raw":
                category_raw,

            "status_raw":
                status_raw,

            "country":
                "Example Republic",

            "country_code":
                "EX",

            "region":
                random.choice(
                    regions
                ),

            "published_date":
                published_date
                .strftime(
                    "%Y-%m-%d"
                ),

            "closing_date":
                closing_date
                .strftime(
                    "%Y-%m-%d"
                ),

            "currency_code":
                "XCU",

            "estimated_value":
                estimated_value,

            "official_source_url":
                (
                    f"{source_base_url}/"
                    f"opportunities/"
                    f"{opportunity_id}"
                ),

            "duplicate_group_hint":
                duplicate_group_hint,

            "source_system":
                source_system,

            "is_synthetic":
                True
        }


        opportunity_payloads.append(
            opportunity_payload
        )


        register_raw_record(
            entity_type="opportunities",
            source_system=source_system,
            source_record_id=opportunity_id,
            payload=opportunity_payload
        )


    payloads_by_source_entity[
        (
            source_system,
            "opportunities"
        )
    ] = opportunity_payloads


# ============================================================
# 5. BRONZE RAW-RECORD ENVELOPE
# ============================================================

raw_record_schema = StructType(
    [
        StructField(
            "entity_type",
            StringType(),
            False
        ),

        StructField(
            "source_system",
            StringType(),
            False
        ),

        StructField(
            "source_record_id",
            StringType(),
            False
        ),

        StructField(
            "schema_version",
            StringType(),
            False
        ),

        StructField(
            "raw_payload_json",
            StringType(),
            False
        ),

        StructField(
            "_ingestion_batch_id",
            StringType(),
            False
        ),

        StructField(
            "_ingested_at_utc",
            TimestampType(),
            False
        ),

        StructField(
            "_source_environment",
            StringType(),
            False
        ),

        StructField(
            "_is_synthetic",
            BooleanType(),
            False
        )
    ]
)


raw_records_df = (
    spark.createDataFrame(
        raw_record_rows,
        schema=raw_record_schema
    )
)


# ============================================================
# 6. IDEMPOTENCY CHECK
# ============================================================

batch_already_loaded = False


if spark.catalog.tableExists(
    "bronze_raw_records"
):

    batch_already_loaded = (
        spark.table(
            "bronze_raw_records"
        )
        .filter(
            F.col(
                "_ingestion_batch_id"
            )
            == ingestion_batch_id
        )
        .limit(1)
        .count()
        > 0
    )


print(
    "Batch already loaded:",
    batch_already_loaded
)


# ============================================================
# 7. WRITE MANIFEST
#
# Manifest is configuration state, so overwrite is intentional.
# ============================================================

manifest_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "bronze_ingestion_manifest"
    )


# ============================================================
# 8. WRITE RAW SOURCE FILES + BRONZE ENVELOPE
#
# The raw Files area preserves source-shaped payloads.
# The Delta envelope makes ingestion auditable/queryable.
# ============================================================

if not batch_already_loaded:

    for (
        source_system,
        entity_type
    ), payload_rows in (
        payloads_by_source_entity.items()
    ):

        raw_path = (
            "Files/raw/"
            f"{source_system}/"
            f"{entity_type}/"
            f"batch_id={ingestion_batch_id}"
        )


        source_payload_df = (
            spark.createDataFrame(
                payload_rows
            )
        )


        source_payload_df.write \
            .mode("overwrite") \
            .json(
                raw_path
            )


    raw_records_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            "bronze_raw_records"
        )


# ============================================================
# 9. INGESTION AUDIT
# ============================================================

run_completed_at_utc = (
    datetime.now(timezone.utc)
    .replace(tzinfo=None)
)


run_schema = StructType(
    [
        StructField(
            "ingestion_batch_id",
            StringType(),
            False
        ),

        StructField(
            "source_environment",
            StringType(),
            False
        ),

        StructField(
            "started_at_utc",
            TimestampType(),
            False
        ),

        StructField(
            "completed_at_utc",
            TimestampType(),
            False
        ),

        StructField(
            "manifest_row_count",
            IntegerType(),
            False
        ),

        StructField(
            "raw_record_count",
            IntegerType(),
            False
        ),

        StructField(
            "status",
            StringType(),
            False
        ),

        StructField(
            "is_synthetic",
            BooleanType(),
            False
        )
    ]
)


run_status = (
    "SkippedAlreadyLoaded"
    if batch_already_loaded
    else "Succeeded"
)


run_rows = [
    {
        "ingestion_batch_id":
            ingestion_batch_id,

        "source_environment":
            source_environment,

        "started_at_utc":
            run_started_at_utc,

        "completed_at_utc":
            run_completed_at_utc,

        "manifest_row_count":
            len(
                manifest_rows
            ),

        "raw_record_count":
            len(
                raw_record_rows
            ),

        "status":
            run_status,

        "is_synthetic":
            True
    }
]


run_df = spark.createDataFrame(
    run_rows,
    schema=run_schema
)


# One audit row per batch ID.

if spark.catalog.tableExists(
    "bronze_ingestion_runs"
):

    existing_run = (
        spark.table(
            "bronze_ingestion_runs"
        )
        .filter(
            F.col(
                "ingestion_batch_id"
            )
            == ingestion_batch_id
        )
        .limit(1)
        .count()
        > 0
    )

else:

    existing_run = False


if not existing_run:

    run_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            "bronze_ingestion_runs"
        )


# ============================================================
# 10. RUN SUMMARY
# ============================================================

print(
    "=========================================="
)

print(
    "CIVIC SIGNAL BRONZE INGESTION COMPLETE"
)

print(
    "=========================================="
)

print(
    f"Batch ID: {ingestion_batch_id}"
)

print(
    f"Status: {run_status}"
)

print(
    f"Manifest rows: {len(manifest_rows)}"
)

print(
    f"Raw records generated: {len(raw_record_rows)}"
)

print(
    "Expected buyers: 18"
)

print(
    "Expected opportunities: 180"
)

print(
    "Expected total raw records: 198"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# BRONZE VALIDATION
# ============================================================

display(
    spark.sql(
        """
        SELECT
            entity_type,
            COUNT(*) AS row_count
        FROM bronze_raw_records
        GROUP BY entity_type
        ORDER BY entity_type
        """
    )
)


display(
    spark.sql(
        """
        SELECT
            source_system,
            COUNT(*) AS row_count
        FROM bronze_raw_records
        GROUP BY source_system
        ORDER BY source_system
        """
    )
)


display(
    spark.sql(
        """
        SELECT
            COUNT(*) AS manifest_rows
        FROM bronze_ingestion_manifest
        """
    )
)


display(
    spark.sql(
        """
        SELECT
            ingestion_batch_id,
            source_environment,
            raw_record_count,
            status,
            started_at_utc,
            completed_at_utc
        FROM bronze_ingestion_runs
        ORDER BY completed_at_utc DESC
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql(
        """
        SELECT
            _ingestion_batch_id,
            COUNT(*) AS raw_record_count
        FROM bronze_raw_records
        GROUP BY _ingestion_batch_id
        ORDER BY _ingestion_batch_id
        """
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
